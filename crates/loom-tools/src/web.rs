//! 网络工具集
//!
//! - web_search: 网页搜索（支持 DuckDuckGo 等后端）
//! - web_extract: 提取网页纯文本内容
//! - http_get: 原始 HTTP GET（保留）

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};

/// web_extract 默认字符上限
const EXTRACT_DEFAULT_CHAR_LIMIT: usize = 100_000;
/// web_extract 最大字符上限
const EXTRACT_MAX_CHAR_LIMIT: usize = 500_000;

pub struct WebToolSet;

#[async_trait]
impl ToolSet for WebToolSet {
    fn name(&self) -> &str {
        "web"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec {
                name: "web_search".into(),
                description: "Search the web and return a list of results with title, URL, and description. Returns metadata only; use web_extract to fetch full page content.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query string"},
                        "limit": {"type": "integer", "description": "Maximum number of results (1-100, default: 5)", "default": 5, "minimum": 1, "maximum": 100}
                    },
                    "required": ["query"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["web".into()],
            },
            ToolSpec {
                name: "web_extract".into(),
                description: "Fetch one or more URLs and extract the main readable text content (titles, paragraphs, lists) with HTML stripped. Returns title, URL, and clean text.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "urls": {
                            "type": ["string", "array"],
                            "items": {"type": "string"},
                            "description": "A single URL or an array of URLs to fetch and extract text from"
                        },
                        "char_limit": {"type": "integer", "description": "Maximum characters of extracted text per URL (default: 100000, max: 500000)", "default": 100000}
                    },
                    "required": ["urls"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["web".into()],
            },
            ToolSpec {
                name: "http_get".into(),
                description: "发起 HTTP GET 请求，返回响应体文本".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"url": {"type": "string"}},
                    "required": ["url"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["web".into()],
            },
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "web_search" => self.web_search(&args).await,
            "web_extract" => self.web_extract(&args).await,
            "http_get" => self.http_get(&args).await,
            _ => Err(loom_core::LoomError::CapabilityNotFound(tool_name.into())),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(stream::once(async move { Ok(v) })))
    }
}

impl WebToolSet {
    async fn web_search(&self, args: &Value) -> Result<Value> {
        let query = args["query"].as_str().unwrap_or("");
        if query.is_empty() {
            return Err(loom_core::LoomError::Other(
                "web_search: 'query' is required".into(),
            ));
        }
        let limit = args["limit"].as_u64().unwrap_or(5).clamp(1, 100) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("Mozilla/5.0 (compatible; LoomBot/1.0)")
            .build()
            .map_err(|e| loom_core::LoomError::Other(format!("web_search client: {e}")))?;

        let url = format!(
            "https://lite.duckduckgo.com/lite/?q={}",
            urlencoding::encode(query)
        );

        let resp =
            client.get(&url).send().await.map_err(|e| {
                loom_core::LoomError::Other(format!("web_search request failed: {e}"))
            })?;

        let status = resp.status().as_u16();
        if status != 200 {
            return Ok(json!({
                "success": false,
                "error": format!("Search request failed with status {status}"),
            }));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("web_search body: {e}")))?;

        let results = parse_duckduckgo_lite(&body, limit);

        Ok(json!({
            "success": true,
            "query": query,
            "results": results,
            "count": results.len(),
        }))
    }

    async fn web_extract(&self, args: &Value) -> Result<Value> {
        let urls: Vec<String> = match &args["urls"] {
            Value::String(s) => vec![s.clone()],
            Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => {
                return Err(loom_core::LoomError::Other(
                    "web_extract: 'urls' must be a string or array of strings".into(),
                ))
            }
        };

        if urls.is_empty() {
            return Err(loom_core::LoomError::Other(
                "web_extract: 'urls' is required".into(),
            ));
        }

        let char_limit = args["char_limit"]
            .as_u64()
            .unwrap_or(EXTRACT_DEFAULT_CHAR_LIMIT as u64)
            .min(EXTRACT_MAX_CHAR_LIMIT as u64) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .user_agent("Mozilla/5.0 (compatible; LoomBot/1.0)")
            .build()
            .map_err(|e| loom_core::LoomError::Other(format!("web_extract client: {e}")))?;

        let mut extracted = Vec::new();
        for url in urls {
            match self.extract_single(&client, &url, char_limit).await {
                Ok(item) => extracted.push(item),
                Err(e) => extracted.push(json!({
                    "url": url,
                    "success": false,
                    "error": e.to_string(),
                })),
            }
        }

        Ok(json!({
            "success": true,
            "count": extracted.len(),
            "results": extracted,
        }))
    }

    async fn extract_single(
        &self,
        client: &reqwest::Client,
        url: &str,
        char_limit: usize,
    ) -> Result<Value> {
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("fetch failed: {e}")))?;

        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            return Ok(json!({
                "url": url,
                "status": status,
                "success": false,
                "error": format!("HTTP {status}"),
            }));
        }

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let body = resp
            .text()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("read body: {e}")))?;

        let (title, text) = if content_type.contains("text/html") {
            extract_html(&body)
        } else {
            (String::new(), body)
        };

        let truncated = text.chars().count() > char_limit;
        let display_text: String = text.chars().take(char_limit).collect();

        Ok(json!({
            "url": url,
            "status": status,
            "success": true,
            "title": title,
            "content_type": content_type,
            "text": display_text,
            "truncated": truncated,
            "char_count": text.chars().count(),
        }))
    }

    async fn http_get(&self, args: &Value) -> Result<Value> {
        let url = args["url"].as_str().unwrap_or("");
        if url.is_empty() {
            return Err(loom_core::LoomError::Other(
                "http_get: url is required".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| loom_core::LoomError::Other(format!("http_get client: {e}")))?;
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("http_get: {e}")))?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("http_get body: {e}")))?;
        Ok(json!({
            "status": status,
            "body": body
        }))
    }
}

/// 解析 DuckDuckGo Lite HTML 搜索结果
fn parse_duckduckgo_lite(html: &str, limit: usize) -> Vec<Value> {
    let mut results = Vec::new();
    // DuckDuckGo Lite 结果在 <a class="result-link"> 中
    let link_re =
        regex::Regex::new(r#"<a[^>]*class="result-link"[^>]*href="([^"]*)"[^>]*>(.*?)</a>"#)
            .unwrap();
    let snippet_re =
        regex::Regex::new(r#"<td[^>]*class="result-snippet"[^>]*>(.*?)</td>"#).unwrap();

    let links: Vec<(String, String)> = link_re
        .captures_iter(html)
        .map(|c| {
            let url = c.get(1).map(|m| m.as_str().to_string()).unwrap_or_default();
            let title = strip_tags(&c.get(2).map(|m| m.as_str().to_string()).unwrap_or_default());
            (url, title)
        })
        .collect();

    let snippets: Vec<String> = snippet_re
        .captures_iter(html)
        .map(|c| strip_tags(&c.get(1).map(|m| m.as_str().to_string()).unwrap_or_default()))
        .collect();

    for (i, (url, title)) in links.into_iter().enumerate() {
        if i >= limit {
            break;
        }
        let snippet = snippets.get(i).cloned().unwrap_or_default();
        results.push(json!({
            "title": title,
            "url": url,
            "description": snippet,
            "position": i + 1,
        }));
    }

    results
}

/// 从 HTML 中提取标题和纯文本
fn extract_html(html: &str) -> (String, String) {
    let title = extract_title(html);
    let text = html_to_text(html);
    (title, text)
}

fn extract_title(html: &str) -> String {
    let re = regex::Regex::new(r"(?is)<title[^>]*>(.*?)</title>").unwrap();
    re.captures(html)
        .and_then(|c| c.get(1))
        .map(|m| strip_tags(m.as_str()).trim().to_string())
        .unwrap_or_default()
}

fn html_to_text(html: &str) -> String {
    let mut text = html.to_string();

    // 移除 script 和 style 块（分别处理，避免 regex 反向引用）
    let script_re = regex::Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap();
    text = script_re.replace_all(&text, " ").to_string();
    let style_re = regex::Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap();
    text = style_re.replace_all(&text, " ").to_string();

    // 移除 HTML 注释
    let comment_re = regex::Regex::new(r"(?s)<!--.*?-->").unwrap();
    text = comment_re.replace_all(&text, " ").to_string();

    // 块级元素转为换行
    let block_re = regex::Regex::new(
        r"(?is)<(p|div|br|li|h[1-6]|tr|section|article|header|footer|nav|blockquote|pre)[^>]*>",
    )
    .unwrap();
    text = block_re.replace_all(&text, "\n").to_string();

    // 移除所有标签
    let tag_re = regex::Regex::new(r"(?s)<[^>]+>").unwrap();
    text = tag_re.replace_all(&text, "").to_string();

    // 解码 HTML 实体
    text = decode_entities(&text);

    // 规范化空白
    text = text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    text
}

fn strip_tags(s: &str) -> String {
    let tag_re = regex::Regex::new(r"(?s)<[^>]+>").unwrap();
    let stripped = tag_re.replace_all(s, "").to_string();
    decode_entities(&stripped)
}

fn decode_entities(s: &str) -> String {
    let mut out = s.to_string();
    out = out.replace("&amp;", "&");
    out = out.replace("&lt;", "<");
    out = out.replace("&gt;", ">");
    out = out.replace("&quot;", "\"");
    out = out.replace("&#39;", "'");
    out = out.replace("&nbsp;", " ");
    // 数字实体 &#NNN;
    let num_re = regex::Regex::new(r"&#(\d+);").unwrap();
    out = num_re
        .replace_all(&out, |caps: &regex::Captures| {
            caps.get(1)
                .and_then(|m| m.as_str().parse::<u32>().ok())
                .and_then(char::from_u32)
                .map(|c| c.to_string())
                .unwrap_or_default()
        })
        .to_string();
    out
}