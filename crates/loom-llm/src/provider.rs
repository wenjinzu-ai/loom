use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;

use crate::types::{ChatChunk, ChatMessage, ChatResponse, ToolDefinition};
use loom_core::Result;

/// LLM Provider trait — 统一多厂商接口
#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn chat(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<ChatResponse>;

    async fn chat_stream(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<BoxStream<'static, Result<ChatChunk>>>;

    /// 返回当前模型的上下文窗口大小（tokens）。
    ///
    /// 用于动态计算压缩阈值。若 provider 无法确定，返回 `None`，
    /// 由上层使用 fallback 阈值。
    fn context_length(&self) -> Option<usize> {
        None
    }
}

/// OpenAI 兼容 Provider 配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpenAiConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

impl Default for OpenAiConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: String::new(),
            model: "gpt-4o-mini".to_string(),
            temperature: Some(0.7),
            max_tokens: None,
        }
    }
}

/// OpenAI 兼容 Provider（支持 OpenRouter、DeepSeek、Qwen 等）
pub struct OpenAiProvider {
    client: reqwest::Client,
    config: OpenAiConfig,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }

    pub fn from_env() -> Self {
        let config = OpenAiConfig {
            api_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
            base_url: std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            model: std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string()),
            ..Default::default()
        };
        Self::new(config)
    }

    fn build_request_body(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolDefinition],
        stream: bool,
    ) -> Value {
        let messages_json: Vec<Value> = messages
            .iter()
            .map(|m| {
                let mut obj = serde_json::json!({
                    "role": m.role.to_string(),
                });
                if let Some(content) = &m.content {
                    obj["content"] = Value::String(content.clone());
                }
                if let Some(tool_calls) = &m.tool_calls {
                    obj["tool_calls"] = serde_json::json!(tool_calls
                        .iter()
                        .map(|tc| serde_json::json!({
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.name,
                                "arguments": tc.arguments.to_string(),
                            }
                        }))
                        .collect::<Vec<_>>());
                }
                if let Some(tcid) = &m.tool_call_id {
                    obj["tool_call_id"] = Value::String(tcid.clone());
                }
                obj
            })
            .collect();

        let tools_json: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": self.config.model,
            "messages": messages_json,
            "stream": stream,
        });
        if !tools_json.is_empty() {
            body["tools"] = Value::Array(tools_json);
        }
        if let Some(t) = self.config.temperature {
            body["temperature"] = Value::from(t);
        }
        if let Some(m) = self.config.max_tokens {
            body["max_tokens"] = Value::from(m);
        }
        body
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn chat(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<ChatResponse> {
        let body = self.build_request_body(&messages, &tools, false);
        let url = format!("{}/chat/completions", self.config.base_url);

        tracing::debug!(
            "[llm] chat request: model={}, messages={}, tools={}, messages_body={:?}, tools_def={:?}",
            self.config.model,
            messages.len(),
            tools.len(),
            messages,
            tools
        );
        let start = std::time::Instant::now();

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                let kind = if e.is_timeout() {
                    loom_core::LlmErrorKind::Timeout
                } else if e.is_connect() {
                    loom_core::LlmErrorKind::Connection
                } else {
                    loom_core::LlmErrorKind::Unknown
                };
                loom_core::LoomError::LlmApi(loom_core::LlmApiError {
                    kind,
                    status_code: None,
                    message: format!("LLM request failed: {e}"),
                    retry_after_secs: None,
                })
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            let text = resp.text().await.unwrap_or_default();
            tracing::debug!("[llm] chat error: status={}, body={}", status, text);

            let kind = classify_llm_error(status.as_u16(), &text);
            return Err(loom_core::LoomError::LlmApi(loom_core::LlmApiError {
                kind,
                status_code: Some(status.as_u16()),
                message: text,
                retry_after_secs: retry_after,
            }));
        }

        let json: Value = resp
            .json()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("LLM parse failed: {e}")))?;

        let choice = json["choices"]
            .get(0)
            .ok_or_else(|| loom_core::LoomError::Other("LLM returned no choices".into()))?;

        let message = &choice["message"];
        let content = message["content"].as_str().map(|s| s.to_string());
        let finish_reason = choice["finish_reason"].as_str().map(|s| s.to_string());

        let usage = json["usage"].as_object().map(|u| crate::types::TokenUsage {
            prompt_tokens: u
                .get("prompt_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            completion_tokens: u
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
            total_tokens: u
                .get("total_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize,
        });

        let tool_calls: Vec<crate::types::ToolCall> = message["tool_calls"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|tc| {
                        let id = tc["id"].as_str()?.to_string();
                        let name = tc["function"]["name"].as_str()?.to_string();
                        let args_str = tc["function"]["arguments"].as_str().unwrap_or("{}");
                        let arguments: Value =
                            serde_json::from_str(args_str).unwrap_or(Value::Null);
                        Some(crate::types::ToolCall {
                            id,
                            name,
                            arguments,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        tracing::debug!(
            "[llm] chat response: finish_reason={:?}, content_len={}, tool_calls={}, elapsed_ms={}, content={:?}, tool_calls_body={:?}",
            finish_reason,
            content.as_ref().map(|s| s.len()).unwrap_or(0),
            tool_calls.len(),
            start.elapsed().as_millis(),
            content,
            tool_calls
        );

        Ok(ChatResponse {
            content,
            tool_calls,
            finish_reason,
            usage,
        })
    }

    async fn chat_stream(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<BoxStream<'static, Result<ChatChunk>>> {
        let body = self.build_request_body(&messages, &tools, true);
        let url = format!("{}/chat/completions", self.config.base_url);
        let api_key = self.config.api_key.clone();
        let client = self.client.clone();

        let stream = async_stream::try_stream! {
            let resp = client
                .post(&url)
                .header("Authorization", format!("Bearer {api_key}"))
                .json(&body)
                .send()
                .await
                .map_err(|e| loom_core::LoomError::Other(format!("LLM stream failed: {e}")))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());
                let text = resp.text().await.unwrap_or_default();
                let kind = classify_llm_error(status.as_u16(), &text);
                return Err(loom_core::LoomError::LlmApi(loom_core::LlmApiError {
                    kind,
                    status_code: Some(status.as_u16()),
                    message: text,
                    retry_after_secs: retry_after,
                }))?;
            }

            use futures::StreamExt;
            let mut lines = resp.bytes_stream();
            let mut buf = String::new();
            let mut current_tool_id: Option<String> = None;
            while let Some(chunk) = lines.next().await {
                let chunk = chunk.map_err(|e| loom_core::LoomError::Other(format!("stream read: {e}")))?;
                buf.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].trim().to_string();
                    buf.drain(..=pos);
                    if line.is_empty() || !line.starts_with("data: ") {
                        continue;
                    }
                    let data = &line[6..];
                    if data == "[DONE]" {
                        yield ChatChunk::Done { finish_reason: None };
                        return;
                    }
                    if let Ok(json) = serde_json::from_str::<Value>(data) {
                        if let Some(choice) = json["choices"].get(0) {
                            let delta = &choice["delta"];
                            if let Some(content) = delta["content"].as_str() {
                                if !content.is_empty() {
                                    yield ChatChunk::Delta(content.to_string());
                                }
                            }
                            if let Some(tool_calls) = delta["tool_calls"].as_array() {
                                for tc in tool_calls {
                                    if let Some(id) = tc["id"].as_str() {
                                        current_tool_id = Some(id.to_string());
                                    }
                                    if let Some(name) = tc["function"]["name"].as_str() {
                                        let id = current_tool_id.clone().unwrap_or_default();
                                        yield ChatChunk::ToolCallStart { id, name: name.to_string() };
                                    }
                                    if let Some(args) = tc["function"]["arguments"].as_str() {
                                        let id = current_tool_id.clone().unwrap_or_default();
                                        yield ChatChunk::ToolCallArgsDelta { id, args: args.to_string() };
                                    }
                                }
                            }
                            if let Some(reason) = choice["finish_reason"].as_str() {
                                yield ChatChunk::Done { finish_reason: Some(reason.to_string()) };
                            }
                        }
                    }
                }
            }
            yield ChatChunk::Done { finish_reason: None };
        };

        Ok(Box::pin(stream))
    }

    fn context_length(&self) -> Option<usize> {
        Some(estimate_context_length(&self.config.model))
    }
}

/// 根据 HTTP 状态码和响应体分类 LLM 错误
fn classify_llm_error(status: u16, body: &str) -> loom_core::LlmErrorKind {
    let lower = body.to_lowercase();
    let is_context_overflow = || -> bool {
        lower.contains("context_length_exceeded")
            || lower.contains("maximum context length")
            || lower.contains("context length")
            || lower.contains("too many tokens")
    };
    match status {
        429 => loom_core::LlmErrorKind::RateLimit,
        401 | 403 => loom_core::LlmErrorKind::Authentication,
        402 => loom_core::LlmErrorKind::ContentPolicy,
        413 => loom_core::LlmErrorKind::ContextOverflow,
        400 => {
            if is_context_overflow() {
                loom_core::LlmErrorKind::ContextOverflow
            } else if lower.contains("content_policy")
                || lower.contains("content filter")
                || lower.contains("safety")
            {
                loom_core::LlmErrorKind::ContentPolicy
            } else {
                loom_core::LlmErrorKind::InvalidRequest
            }
        }
        s if s >= 500 => loom_core::LlmErrorKind::ServerError,
        _ => {
            if is_context_overflow() {
                loom_core::LlmErrorKind::ContextOverflow
            } else if lower.contains("rate limit") || lower.contains("too many requests") {
                loom_core::LlmErrorKind::RateLimit
            } else if lower.contains("content_policy") || lower.contains("content filter") {
                loom_core::LlmErrorKind::ContentPolicy
            } else if lower.contains("timeout") {
                loom_core::LlmErrorKind::Timeout
            } else {
                loom_core::LlmErrorKind::Unknown
            }
        }
    }
}

/// 根据模型名推断上下文窗口大小（tokens）。
///
/// 覆盖主流模型；未知模型回退到 128k（现代主流大模型的常见上限）。
pub fn estimate_context_length(model: &str) -> usize {
    let m = model.to_lowercase();
    // GPT 系列
    if m.contains("gpt-4o") && !m.contains("mini") {
        return 128_000;
    }
    if m.contains("gpt-4o-mini") {
        return 128_000;
    }
    if m.contains("gpt-4-turbo") {
        return 128_000;
    }
    if m.contains("gpt-4") {
        return 8_192;
    }
    if m.contains("gpt-3.5") || m.contains("gpt-35") {
        return 16_384;
    }
    // Claude 系列
    if m.contains("claude-opus") {
        return 200_000;
    }
    if m.contains("claude-sonnet") {
        return 200_000;
    }
    if m.contains("claude-haiku") {
        return 200_000;
    }
    // DeepSeek
    if m.contains("deepseek-chat") || m.contains("deepseek-v3") {
        return 64_000;
    }
    if m.contains("deepseek-coder") {
        return 128_000;
    }
    // Qwen
    if m.contains("qwen2.5") || m.contains("qwen3") {
        return 131_072;
    }
    if m.contains("qwen") {
        return 32_768;
    }
    // Gemini
    if m.contains("gemini-2.5") || m.contains("gemini-2") {
        return 1_000_000;
    }
    if m.contains("gemini-1.5") {
        return 1_048_576;
    }
    if m.contains("gemini") {
        return 32_768;
    }
    // 默认：现代主流模型通常 ≥ 128k
    128_000
}

/// Mock Provider — 用于无 API Key 时的演示和测试
///
/// 根据简单规则生成响应，支持 echo 类工具的模拟调用。
pub struct MockProvider;

impl MockProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LlmProvider for MockProvider {
    async fn chat(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<ChatResponse> {
        let last_user = messages
            .iter()
            .rev()
            .find(|m| m.role == crate::types::Role::User)
            .and_then(|m| m.content.clone())
            .unwrap_or_default();

        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();

        if tool_names.contains(&"echo") && last_user.to_lowercase().contains("echo") {
            return Ok(ChatResponse {
                content: None,
                tool_calls: vec![crate::types::ToolCall {
                    id: "call_mock_1".to_string(),
                    name: "echo".to_string(),
                    arguments: serde_json::json!({ "message": last_user }),
                }],
                finish_reason: Some("tool_calls".to_string()),
                usage: None,
            });
        }

        Ok(ChatResponse {
            content: Some(format!(
                "[mock-llm] 收到你的消息：{last_user}\n可用工具：{tool_names:?}"
            )),
            tool_calls: vec![],
            finish_reason: Some("stop".to_string()),
            usage: None,
        })
    }

    async fn chat_stream(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<ToolDefinition>,
    ) -> Result<BoxStream<'static, Result<ChatChunk>>> {
        let resp = self.chat(messages, tools).await?;
        let chunks: Vec<Result<ChatChunk>> = if let Some(content) = resp.content {
            let mut v = vec![Ok(ChatChunk::Delta(content))];
            v.push(Ok(ChatChunk::Done {
                finish_reason: resp.finish_reason,
            }));
            v
        } else {
            let mut v = Vec::new();
            for tc in resp.tool_calls {
                v.push(Ok(ChatChunk::ToolCallStart {
                    id: tc.id.clone(),
                    name: tc.name.clone(),
                }));
                v.push(Ok(ChatChunk::ToolCallArgsDelta {
                    id: tc.id.clone(),
                    args: tc.arguments.to_string(),
                }));
                v.push(Ok(ChatChunk::ToolCallEnd));
            }
            v.push(Ok(ChatChunk::Done {
                finish_reason: resp.finish_reason,
            }));
            v
        };
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}