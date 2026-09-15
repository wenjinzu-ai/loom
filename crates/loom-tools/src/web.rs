//! 网络工具集
//!
//! - web_search: 网页搜索（多后端自动 fallback：DuckDuckGo → Bing → Brave）
//! - web_extract: 提取网页纯文本内容
//! - http_get: 原始 HTTP GET（保留）

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};

/// web_extract 默认字符上限
const EXTRACT_DEFAULT_CHAR_LIMIT: usize = 100_000;
/// web_extract 最大字符上限
const EXTRACT_MAX_CHAR_LIMIT: usize = 500_000;

/// 浏览器 User-Agent（模拟真实浏览器，降低被反爬拦截概率）
const BROWSER_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

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
            .user_agent(BROWSER_UA)
            .build()
            .map_err(|e| loom_core::LoomError::Other(format!("web_search client: {e}")))?;

        // 按优先级尝试多个后端，任一成功即返回
        let mut errors: Vec<String> = Vec::new();

        match search_duckduckgo(&client, query, limit).await {
            Ok(v) => {
                tracing::debug!(backend = "duckduckgo", "web_search succeeded");
                return Ok(v);
            }
            Err(e) => {
                tracing::warn!(backend = "duckduckgo", error = %e, "backend failed, trying next");
                errors.push(format!("duckduckgo: {e}"));
            }
        }

        match search_bing(&client, query, limit).await {
            Ok(v) => {
                tracing::debug!(backend = "bing", "web_search succeeded");
                return Ok(v);
            }
            Err(e) => {
                tracing::warn!(backend = "bing", error = %e, "backend failed, trying next");
                errors.push(format!("bing: {e}"));
            }
        }

        match search_brave(&client, query, limit).await {
            Ok(v) => {
                tracing::debug!(backend = "brave", "web_search succeeded");
                return Ok(v);
            }
            Err(e) => {
                tracing::warn!(backend = "brave", error = %e, "backend failed");
                errors.push(format!("brave: {e}"));
            }
        }

        Ok(json!({
            "success": false,
            "query": query,
            "error": format!("All search backends failed: {}", errors.join(" | ")),
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
            .user_agent(BROWSER_UA)
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
            .user_agent(BROWSER_UA)
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

/// DuckDuckGo Lite 搜索后端
async fn search_duckduckgo(
    client: &reqwest::Client,
    query: &str,
    limit: usize,
) -> Result<Value> {
    let url = format!(
        "https://lite.duckduckgo.com/lite/?q={}",
        urlencoding::encode(query)
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("duckduckgo request failed: {e}")))?;

    let status = resp.status().as_u16();
    if status != 200 {
        return Err(loom_core::LoomError::Other(format!(
            "duckduckgo returned status {status}"
        )));
    }

    let body = resp
        .text()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("duckduckgo body: {e}")))?;

    let results = parse_duckduckgo_lite(&body, limit);
    if results.is_empty() {
        return Err(loom_core::LoomError::Other(
            "duckduckgo returned no results (page structure may have changed)".into(),
        ));
    }

    Ok(json!({
        "success": true,
        "query": query,
        "backend": "duckduckgo",
        "results": results,
        "count": results.len(),
    }))
}

/// Bing 搜索后端
async fn search_bing(client: &reqwest::Client, query: &str, limit: usize) -> Result<Value> {
    let url = format!(
        "https://www.bing.com/search?q={}",
        urlencoding::encode(query)
    );

    let resp = client
        .get(&url)
        .header("Accept-Language", "en-US,en;q=0.9")
        .send()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("bing request failed: {e}")))?;

    let status = resp.status().as_u16();
    if !resp.status().is_success() {
        return Err(loom_core::LoomError::Other(format!(
            "bing returned status {status}"
        )));
    }

    let body = resp
        .text()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("bing body: {e}")))?;

    let results = parse_bing(&body, limit);
    if results.is_empty() {
        return Err(loom_core::LoomError::Other(
            "bing returned no results (page structure may have changed)".into(),
        ));
    }

    Ok(json!({
        "success": true,
        "query": query,
        "backend": "bing",
        "results": results,
        "count": results.len(),
    }))
}

/// Brave Search 后端
async fn search_brave(client: &reqwest::Client, query: &str, limit: usize) -> Result<Value> {
    let url = format!(
        "https://search.brave.com/search?q={}",
        urlencoding::encode(query)
    );

    let resp = client
        .get(&url)
        .header("Accept-Language", "en-US,en;q=0.9")
        .send()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("brave request failed: {e}")))?;

    let status = resp.status().as_u16();
    if !resp.status().is_success() {
        return Err(loom_core::LoomError::Other(format!(
            "brave returned status {status}"
        )));
    }

    let body = resp
        .text()
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("brave body: {e}")))?;

    let results = parse_brave(&body, limit);
    if results.is_empty() {
        return Err(loom_core::LoomError::Other(
            "brave returned no results (page structure may have changed)".into(),
        ));
    }

    Ok(json!({
        "success": true,
        "query": query,
        "backend": "brave",
        "results": results,
        "count": results.len(),
    }))
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

/// 解析 Bing 搜索结果 HTML
///
/// Bing 结果块结构：
/// ```html
/// <li class="b_algo">
///   <h2><a href="https://www.bing.com/ck/a?!&...&u=a1<base64>&...">Title</a></h2>
///   <p>snippet text</p>
/// </li>
/// ```
/// URL 中的 `u=a1<base64>` 是真实 URL 的 base64 编码（a1 为前缀）。
fn parse_bing(html: &str, limit: usize) -> Vec<Value> {
    let mut results = Vec::new();

    // 匹配结果块：<li class="b_algo" ...> ... <h2>...<a href="...">title</a>...</h2> ... <p>snippet</p>
    let result_re = regex::Regex::new(
        r#"(?is)<li class="b_algo"[^>]*>[\s\S]*?<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>([\s\S]*?)</a>\s*</h2>[\s\S]*?<p[^>]*>([\s\S]*?)</p>"#,
    )
    .unwrap();

    for (i, caps) in result_re.captures_iter(html).enumerate() {
        if i >= limit {
            break;
        }
        let raw_url = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
        let url = decode_bing_url(raw_url);
        let title = strip_tags(&caps.get(2).map(|m| m.as_str()).unwrap_or_default());
        let snippet = strip_tags(&caps.get(3).map(|m| m.as_str()).unwrap_or_default());

        results.push(json!({
            "title": title,
            "url": url,
            "description": snippet,
            "position": i + 1,
        }));
    }

    results
}

/// 从 Bing 重定向 URL 中解码真实 URL
///
/// Bing 的结果链接形如：
/// `https://www.bing.com/ck/a?!&...&u=a1aHR0cHM6Ly9leGFtcGxlLmNvbS8&...`
/// 其中 `u=` 参数的值以 `a1` 为前缀，后面是 base64 编码的真实 URL。
fn decode_bing_url(raw_url: &str) -> String {
    // 如果是外部链接直接返回
    if !raw_url.contains("bing.com/ck/a") {
        return raw_url.to_string();
    }

    // 提取 u= 参数值
    if let Some(u_start) = raw_url.find("&u=") {
        let after_u = &raw_url[u_start + 3..];
        let u_value: String = after_u
            .chars()
            .take_while(|c| *c != '&')
            .collect();

        // 去掉 a1 前缀后 base64 解码
        if let Some(b64_part) = u_value.strip_prefix("a1") {
            // 补齐 base64 padding
            let padding = (4 - b64_part.len() % 4) % 4;
            let padded = format!("{}{}", b64_part, "=".repeat(padding));
            if let Ok(decoded) = B64.decode(padded.as_bytes()) {
                if let Ok(real_url) = String::from_utf8(decoded) {
                    return real_url;
                }
            }
        }
    }

    raw_url.to_string()
}

/// 解析 Brave Search 结果 HTML
///
/// Brave 结果块结构（Svelte 渲染，class 带 hash）：
/// ```html
/// <div class="result-wrapper ...">
///   <div class="result-content ...">
///     <a href="https://example.com/" ...>
///       <div class="title search-snippet-title ..." title="Title">Title</div>
///     </a>
///     <div class="generic-snippet ...">
///       <div class="content ...">snippet text</div>
///     </div>
///   </div>
/// </div>
/// ```
fn parse_brave(html: &str, limit: usize) -> Vec<Value> {
    let mut results = Vec::new();

    // 匹配结果链接块：<a href="URL" ...> ... <div class="title ..." title="TITLE"> 或文本
    // 用更宽松的匹配，抓取 result-content 内的第一个外部链接及其标题
    let link_re = regex::Regex::new(
        r#"(?is)<a[^>]*href="(https?://[^"]+)"[^>]*>[\s\S]*?<div[^>]*class="[^"]*title[^"]*"[^>]*title="([^"]*)"[^>]*>"#,
    )
    .unwrap();

    // 摘要匹配：Brave 摘要 div 的 class 以 "content " 开头
    // （注意：要与 "result-content" 区分，后者是结果容器）
    let snippet_re = regex::Regex::new(
        r#"(?is)<div[^>]*class="content [^"]*"[^>]*>([\s\S]*?)</div>"#,
    )
    .unwrap();

    let links: Vec<(String, String)> = link_re
        .captures_iter(html)
        .filter_map(|c| {
            let url = c.get(1).map(|m| m.as_str().to_string()).unwrap_or_default();
            let title = c.get(2).map(|m| m.as_str().to_string()).unwrap_or_default();
            if url.is_empty() || title.is_empty() {
                return None;
            }
            // 过滤掉 Brave 自身的链接
            if url.contains("search.brave.com") || url.contains("brave.com") {
                return None;
            }
            Some((url, title))
        })
        .collect();

    let snippets: Vec<String> = snippet_re
        .captures_iter(html)
        .map(|c| strip_tags(&c.get(1).map(|m| m.as_str().to_string()).unwrap_or_default()))
        .filter(|s| !s.trim().is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_bing_url_redirect() {
        // u=a1 前缀 + base64("https://rust-lang.org/")
        let raw = "https://www.bing.com/ck/a?!&&p=abc&u=a1aHR0cHM6Ly9ydXN0LWxhbmcub3JnLw&ntb=1";
        assert_eq!(decode_bing_url(raw), "https://rust-lang.org/");
    }

    #[test]
    fn test_decode_bing_url_direct() {
        // 非 bing 重定向链接直接返回
        let raw = "https://example.com/page";
        assert_eq!(decode_bing_url(raw), "https://example.com/page");
    }

    #[test]
    fn test_decode_bing_url_missing_u_param() {
        // 没有 u= 参数时返回原始 URL
        let raw = "https://www.bing.com/ck/a?!&&p=abc&ntb=1";
        assert_eq!(decode_bing_url(raw), raw);
    }

    #[test]
    fn test_parse_bing() {
        let html = r#"
        <ol id="b_results">
          <li class="b_algo" data-id>
            <h2><a href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9ydXN0LWxhbmcub3JnLw&ntb=1">Rust Programming Language</a></h2>
            <p>Rust is blazingly fast and memory-efficient.</p>
          </li>
          <li class="b_algo">
            <h2><a href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9leGFtcGxlLmNvbS8&ntb=1">Example Site</a></h2>
            <p>This is an example website.</p>
          </li>
        </ol>
        "#;

        let results = parse_bing(html, 5);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["title"], "Rust Programming Language");
        assert_eq!(results[0]["url"], "https://rust-lang.org/");
        assert_eq!(results[0]["description"], "Rust is blazingly fast and memory-efficient.");
        assert_eq!(results[0]["position"], 1);
        assert_eq!(results[1]["title"], "Example Site");
        assert_eq!(results[1]["url"], "https://example.com/");
        assert_eq!(results[1]["position"], 2);
    }

    #[test]
    fn test_parse_bing_limit() {
        let html = r#"
        <li class="b_algo"><h2><a href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9hLmNvbS8">A</a></h2><p>s1</p></li>
        <li class="b_algo"><h2><a href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9iLmNvbS8">B</a></h2><p>s2</p></li>
        <li class="b_algo"><h2><a href="https://www.bing.com/ck/a?!&u=a1aHR0cHM6Ly9jLmNvbS8">C</a></h2><p>s3</p></li>
        "#;

        let results = parse_bing(html, 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parse_brave() {
        let html = r#"
        <div class="result-wrapper svelte-xxx">
          <div class="result-content svelte-xxx">
            <a href="https://rust-lang.org/en-US/" target="_self" class="svelte-yyy">
              <div class="title search-snippet-title svelte-zzz" title="Rust Programming Language">Rust Programming Language</div>
            </a>
            <div class="generic-snippet svelte-aaa">
              <div class="content svelte-bbb">Rust is blazingly fast and memory-efficient.</div>
            </div>
          </div>
        </div>
        "#;

        let results = parse_brave(html, 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["title"], "Rust Programming Language");
        assert_eq!(results[0]["url"], "https://rust-lang.org/en-US/");
        assert_eq!(results[0]["description"], "Rust is blazingly fast and memory-efficient.");
        assert_eq!(results[0]["position"], 1);
    }

    #[test]
    fn test_parse_brave_filters_brave_links() {
        let html = r#"
        <div class="result-wrapper">
          <a href="https://search.brave.com/images?q=rust">
            <div class="title" title="Brave Images">Brave Images</div>
          </a>
        </div>
        <div class="result-wrapper">
          <a href="https://rust-lang.org/">
            <div class="title" title="Rust">Rust</div>
          </a>
          <div class="generic-snippet"><div class="content">snippet</div></div>
        </div>
        "#;

        let results = parse_brave(html, 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["url"], "https://rust-lang.org/");
    }

    #[test]
    fn test_parse_duckduckgo_lite() {
        let html = r#"
        <a class="result-link" href="https://rust-lang.org/">Rust Programming Language</a>
        <td class="result-snippet">Rust is blazingly fast.</td>
        <a class="result-link" href="https://example.com/">Example</a>
        <td class="result-snippet">Example snippet.</td>
        "#;

        let results = parse_duckduckgo_lite(html, 5);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["title"], "Rust Programming Language");
        assert_eq!(results[0]["url"], "https://rust-lang.org/");
        assert_eq!(results[0]["description"], "Rust is blazingly fast.");
    }

    #[test]
    fn test_strip_tags_and_entities() {
        assert_eq!(strip_tags("<b>Hello &amp; World</b>"), "Hello & World");
        assert_eq!(strip_tags("<a href='#'>Test &quot;quote&quot;</a>"), "Test \"quote\"");
    }
}