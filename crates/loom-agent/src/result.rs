//! 子 Agent 结构化结果回传
//!
//! - summary: 文本摘要
//! - structured_payload: 结构化负载（JSON，经 output_schema 验证）
//! - error_classification: 错误分类
//! - usage_metadata: 使用量元数据（迭代数、工具调用数等）
//! - tool_execution_summary: 工具执行摘要
//! - tool_trace: 最近 N 条工具调用结果（{tool, preview, is_error}）
//! - duration_ms: 执行时长（毫秒）
//! - result_hash: 结果哈希（用于去重/验证）

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// 错误分类
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClassification {
    /// 无错误
    #[default]
    None,
    /// 工具执行错误
    ToolError,
    /// 模型 API 错误
    ModelError,
    /// 超时
    Timeout,
    /// 委派错误
    DelegationError,
    /// 检查点/恢复错误
    CheckpointError,
    /// 资源不足（token、并发等）
    ResourceExhausted,
    /// 父中断传播取消
    Cancelled,
    /// 未知错误
    Unknown,
}

impl ErrorClassification {
    pub fn from_error(err: &str) -> Self {
        let lower = err.to_lowercase();
        if lower.contains("timeout") || lower.contains("timed out") {
            Self::Timeout
        } else if lower.contains("tool") {
            Self::ToolError
        } else if lower.contains("api") || lower.contains("llm") || lower.contains("model") {
            Self::ModelError
        } else if lower.contains("delegat") || lower.contains("spawn") {
            Self::DelegationError
        } else if lower.contains("checkpoint") || lower.contains("resume") {
            Self::CheckpointError
        } else if lower.contains("resource") || lower.contains("quota") || lower.contains("token") {
            Self::ResourceExhausted
        } else if lower.contains("cancel") {
            Self::Cancelled
        } else if err.is_empty() {
            Self::None
        } else {
            Self::Unknown
        }
    }
}

/// 使用量元数据
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageMetadata {
    /// 迭代轮数
    pub iterations: usize,
    /// 工具调用总数
    pub tool_calls: usize,
    /// 委派的子 Agent 数
    pub subagents_spawned: usize,
    /// 压缩次数
    pub compressions: usize,
}

/// 单个工具执行摘要
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolExecutionEntry {
    pub name: String,
    pub count: usize,
}

/// 工具执行摘要（按名称聚合）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolExecutionSummary {
    pub entries: Vec<ToolExecutionEntry>,
}

impl ToolExecutionSummary {
    pub fn from_tool_names(names: &[String]) -> Self {
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for name in names {
            *counts.entry(name.clone()).or_insert(0) += 1;
        }
        let entries = counts
            .into_iter()
            .map(|(name, count)| ToolExecutionEntry { name, count })
            .collect();
        Self { entries }
    }

    pub fn total_calls(&self) -> usize {
        self.entries.iter().map(|e| e.count).sum()
    }
}

/// 单条工具调用轨迹（用于父 Agent 了解子 Agent 做了什么）
///
/// `_extract_output_tail`：提取最近 N 条工具调用结果，
/// 包含工具名、内容预览和是否为错误。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolTraceEntry {
    /// 工具名称
    pub tool: String,
    /// 调用参数（JSON）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
    /// 内容预览（截断）
    pub preview: String,
    /// 是否为错误输出
    pub is_error: bool,
}

/// 工具轨迹（最近 N 条工具调用结果）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolTrace {
    pub entries: Vec<ToolTraceEntry>,
}

impl ToolTrace {
    /// 从消息历史中提取最近 N 条工具调用结果
    pub fn from_messages(messages: &[loom_llm::ChatMessage], max_entries: usize) -> Self {
        use loom_llm::Role;

        // 建立 tool_call_id -> (tool_name, arguments) 映射
        let mut info_by_call_id: std::collections::HashMap<String, (String, Option<Value>)> =
            std::collections::HashMap::new();
        for msg in messages {
            if msg.role == Role::Assistant {
                if let Some(tcs) = &msg.tool_calls {
                    for tc in tcs {
                        let args = if tc.arguments.is_null() {
                            None
                        } else {
                            Some(tc.arguments.clone())
                        };
                        info_by_call_id.insert(tc.id.clone(), (tc.name.clone(), args));
                    }
                }
            }
        }

        // 从后往前收集 tool 消息
        let mut tail: Vec<ToolTraceEntry> = Vec::new();
        for msg in messages.iter().rev() {
            if tail.len() >= max_entries {
                break;
            }
            if msg.role != Role::Tool {
                continue;
            }
            let content = msg.content.as_deref().unwrap_or("");
            let (tool_name, arguments) = msg
                .tool_call_id
                .as_ref()
                .and_then(|id| info_by_call_id.get(id).cloned())
                .unwrap_or_else(|| ("tool".to_string(), None));
            let preview = if content.chars().count() > 2000 {
                content.chars().take(2000).collect::<String>()
            } else {
                content.to_string()
            };
            let is_error = looks_like_error_output(content);
            tail.push(ToolTraceEntry {
                tool: tool_name,
                arguments,
                preview,
                is_error,
            });
        }
        tail.reverse();
        Self { entries: tail }
    }
}

/// 保守的错误检测器：结构化 JSON 含 error 字段或 error/failed status，
/// 或首行以经典错误标记开头。
fn looks_like_error_output(content: &str) -> bool {
    if content.is_empty() {
        return false;
    }
    let trimmed = content.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
            if let Some(obj) = parsed.as_object() {
                if obj.get("error").is_some() {
                    return true;
                }
                if let Some(status) = obj.get("status").and_then(|v| v.as_str()) {
                    let s = status.trim().to_lowercase();
                    if matches!(s.as_str(), "error" | "failed" | "failure" | "timeout") {
                        return true;
                    }
                }
            }
        }
    }
    let first = content.lines().next().unwrap_or("").trim().to_lowercase();
    first.starts_with("error:")
        || first.starts_with("failed:")
        || first.starts_with("traceback ")
        || first.starts_with("exception:")
}

/// 子 Agent 结构化结果
///
/// 由 `AgentEvent::Finished` 转换而来，包含丰富的结构化信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubagentResult {
    /// 文本摘要（Agent 最终响应）
    pub summary: String,
    /// 结构化负载（可选，Agent 可返回 JSON）
    pub structured_payload: Option<Value>,
    /// 是否成功完成
    pub success: bool,
    /// 错误分类
    pub error_classification: ErrorClassification,
    /// 错误消息（失败时）
    pub error_message: Option<String>,
    /// 使用量元数据
    pub usage: UsageMetadata,
    /// 工具执行摘要
    pub tool_summary: ToolExecutionSummary,
    /// 最近 N 条工具调用轨迹
    pub tool_trace: ToolTrace,
    /// 执行时长（毫秒）
    pub duration_ms: u64,
    /// 结果哈希（SHA256 of summary，用于去重/验证）
    pub result_hash: String,
}

impl SubagentResult {
    /// 从成功的 AgentEvent 构建
    pub fn from_success(
        response: String,
        iterations: usize,
        tool_calls_made: usize,
        tool_names: &[String],
    ) -> Self {
        Self::from_success_with_meta(response, iterations, tool_calls_made, tool_names, 0, None)
    }

    /// 从成功的 AgentEvent 构建，附带执行时长和工具轨迹
    pub fn from_success_with_meta(
        response: String,
        iterations: usize,
        tool_calls_made: usize,
        tool_names: &[String],
        duration_ms: u64,
        tool_trace_json: Option<Value>,
    ) -> Self {
        let result_hash = hash_hex(&response);
        let structured_payload = extract_json_payload(&response);
        let tool_trace = tool_trace_json
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        Self {
            summary: response,
            structured_payload,
            success: true,
            error_classification: ErrorClassification::None,
            error_message: None,
            usage: UsageMetadata {
                iterations,
                tool_calls: tool_calls_made,
                ..Default::default()
            },
            tool_summary: ToolExecutionSummary::from_tool_names(tool_names),
            tool_trace,
            duration_ms,
            result_hash,
        }
    }

    /// 从错误构建
    pub fn from_error(err: &str, iterations: usize, tool_calls_made: usize) -> Self {
        let result_hash = hash_hex(err);
        Self {
            summary: String::new(),
            structured_payload: None,
            success: false,
            error_classification: ErrorClassification::from_error(err),
            error_message: Some(err.to_string()),
            usage: UsageMetadata {
                iterations,
                tool_calls: tool_calls_made,
                ..Default::default()
            },
            tool_summary: ToolExecutionSummary::default(),
            tool_trace: ToolTrace::default(),
            duration_ms: 0,
            result_hash,
        }
    }

    /// 转为 JSON 值（用于工具返回）
    pub fn to_json(&self) -> Value {
        json!({
            "summary": self.summary,
            "structured_payload": self.structured_payload,
            "success": self.success,
            "error_classification": self.error_classification,
            "error_message": self.error_message,
            "usage": self.usage,
            "tool_summary": self.tool_summary,
            "tool_trace": self.tool_trace,
            "duration_ms": self.duration_ms,
            "result_hash": self.result_hash,
        })
    }

    /// 应用委派摘要预算：截断 summary 到 max_chars 字符
    ///
    /// 子 Agent 结果返回父 Agent 前截断过长的摘要，防止大结果撑爆父上下文。
    /// 截断时保留尾部，因为结论通常在末尾。
    pub fn with_summary_budget(mut self, max_chars: usize) -> Self {
        if max_chars == 0 || self.summary.chars().count() <= max_chars {
            return self;
        }
        let truncated: String = self.summary.chars().rev().take(max_chars).collect::<String>().chars().rev().collect();
        self.summary = format!("...[truncated, {} chars total]\n{}", self.summary.chars().count(), truncated);
        self.result_hash = hash_hex(&self.summary);
        self
    }
}

/// 计算输入的哈希值并返回十六进制字符串
///
/// 注意：使用 `DefaultHasher`（非密码学哈希），仅用于快速去重/验证。
fn hash_hex(input: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// 从文本响应中提取 JSON 负载
///
/// 尝试解析响应为 JSON；若响应包含 ```json ... ``` 代码块则提取其中内容。
fn extract_json_payload(response: &str) -> Option<Value> {
    let trimmed = response.trim();

    // 直接尝试解析
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Some(v);
    }

    // 尝试提取 ```json 代码块
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            let json_str = &after[..end].trim();
            if let Ok(v) = serde_json::from_str::<Value>(json_str) {
                return Some(v);
            }
        }
    }

    // 尝试提取第一个 {...} 或 [...] 块
    let first_brace = trimmed.find('{').or_else(|| trimmed.find('['))?;
    let close = if trimmed.as_bytes()[first_brace] == b'{' {
        trimmed.rfind('}')?
    } else {
        trimmed.rfind(']')?
    };
    let candidate = &trimmed[first_brace..=close];
    serde_json::from_str::<Value>(candidate).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        assert_eq!(
            ErrorClassification::from_error(""),
            ErrorClassification::None
        );
        assert_eq!(
            ErrorClassification::from_error("tool call failed"),
            ErrorClassification::ToolError
        );
        assert_eq!(
            ErrorClassification::from_error("request timed out"),
            ErrorClassification::Timeout
        );
        assert_eq!(
            ErrorClassification::from_error("model api error"),
            ErrorClassification::ModelError
        );
    }

    #[test]
    fn test_extract_json_direct() {
        let payload = extract_json_payload(r#"{"key": "value"}"#);
        assert!(payload.is_some());
        assert_eq!(payload.unwrap()["key"], "value");
    }

    #[test]
    fn test_extract_json_codeblock() {
        let response = "Here is the result:\n```json\n{\"status\": \"ok\"}\n```";
        let payload = extract_json_payload(response);
        assert!(payload.is_some());
        assert_eq!(payload.unwrap()["status"], "ok");
    }

    #[test]
    fn test_subagent_result_success() {
        let result =
            SubagentResult::from_success("done".to_string(), 5, 3, &["read_file".to_string()]);
        assert!(result.success);
        assert_eq!(result.usage.iterations, 5);
        assert_eq!(result.tool_summary.total_calls(), 1);
        assert!(!result.result_hash.is_empty());
    }

    #[test]
    fn test_tool_execution_summary() {
        let names = vec![
            "read_file".to_string(),
            "read_file".to_string(),
            "write_file".to_string(),
        ];
        let summary = ToolExecutionSummary::from_tool_names(&names);
        assert_eq!(summary.total_calls(), 3);
    }
}

/// Agent 执行进度事件（实时推送给前端）
///
/// 在 `run_loop` 的关键节点发送，用于前端实时展示思考/工具调用进度。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgressEvent {
    /// 新一轮迭代开始
    Iteration { iteration: usize },
    /// 工具调用开始
    ToolStart {
        tool: String,
        arguments: Option<Value>,
    },
    /// 工具调用结束
    ToolEnd {
        tool: String,
        result: String,
        is_error: bool,
    },
}