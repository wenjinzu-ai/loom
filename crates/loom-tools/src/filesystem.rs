//! 文件系统工具集
//!
//! - read_file: 带行号、分页(offset/limit)、字符预算截断
//! - write_file: 覆盖写入、自动创建父目录
//! - patch: 模糊查找替换，返回 unified diff
//! - search_files: 内容搜索(grep)和文件名搜索(find)

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use regex::Regex;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// read_file 字符预算上限（~100K 字符）
const READ_CHAR_BUDGET: usize = 100_000;
/// read_file 默认行数
const READ_DEFAULT_LIMIT: usize = 2000;
/// read_file 最大行数
const READ_MAX_LIMIT: usize = 2000;

pub struct FilesystemToolSet;

#[async_trait]
impl ToolSet for FilesystemToolSet {
    fn name(&self) -> &str {
        "filesystem"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec {
                name: "read_file".into(),
                description: "Read a text file with line numbers and pagination. Output format: 'LINE_NUM|CONTENT'. Use offset and limit for large files. Reads exceeding ~100K characters are truncated on a line boundary and return a next_offset; continue with offset to read the rest.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to the file to read"},
                        "offset": {"type": "integer", "description": "Line number to start reading from (1-indexed, default: 1)", "default": 1, "minimum": 1},
                        "limit": {"type": "integer", "description": "Maximum number of lines to read (default: 2000, max: 2000)", "default": 2000, "maximum": 2000}
                    },
                    "required": ["path"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["file".into()],
            },
            ToolSpec {
                name: "write_file".into(),
                description: "Write content to a file, completely replacing existing content. Creates parent directories automatically. OVERWRITES the entire file — use 'patch' for targeted edits.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to the file to write (created if doesn't exist, overwritten if does)"},
                        "content": {"type": "string", "description": "Complete content to write to the file"}
                    },
                    "required": ["path", "content"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["file".into()],
            },
            ToolSpec {
                name: "patch".into(),
                description: "Targeted find-and-replace edits in files. Uses fuzzy matching so minor whitespace differences won't break it. Returns a unified diff. Finds a unique string and replaces it.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path to edit"},
                        "old_string": {"type": "string", "description": "Exact text to find and replace. Must be unique unless replace_all=true."},
                        "new_string": {"type": "string", "description": "Replacement text; must differ from old_string. Pass '' to delete."},
                        "replace_all": {"type": "boolean", "description": "Replace all occurrences instead of requiring a unique match (default: false)", "default": false}
                    },
                    "required": ["path", "old_string", "new_string"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["file".into()],
            },
            ToolSpec {
                name: "search_files".into(),
                description: "Search file contents or find files by name. Content search (target='content'): Regex search inside files. File search (target='files'): Find files by glob pattern.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string", "description": "Regex pattern for content search, or glob pattern for file search"},
                        "target": {"type": "string", "enum": ["content", "files"], "description": "'content' searches inside files, 'files' searches by name", "default": "content"},
                        "path": {"type": "string", "description": "Directory or file to search in (default: current working directory)", "default": "."},
                        "file_glob": {"type": "string", "description": "Filter files by pattern in content mode (e.g. '*.py')"},
                        "limit": {"type": "integer", "description": "Maximum number of results (default: 50)", "default": 50},
                        "offset": {"type": "integer", "description": "Skip first N results for pagination (default: 0)", "default": 0},
                        "output_mode": {"type": "string", "enum": ["content", "files_only", "count"], "description": "Content mode output format", "default": "content"},
                        "context": {"type": "integer", "description": "Context lines before/after each match (content mode only)", "default": 0}
                    },
                    "required": ["pattern"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["file".into()],
            },
            ToolSpec {
                name: "list_dir".into(),
                description: "List directory entries".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
                output_schema: json!({"type": "array", "items": {"type": "string"}}),
                streaming: false,
                tags: vec!["file".into()],
            },
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "read_file" => self.read_file(&args).await,
            "write_file" => self.write_file(&args).await,
            "patch" => self.patch(&args).await,
            "search_files" => self.search_files(&args).await,
            "list_dir" => self.list_dir(&args).await,
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

impl FilesystemToolSet {
    async fn read_file(&self, args: &Value) -> Result<Value> {
        let path = args["path"].as_str().unwrap_or("");
        if path.is_empty() {
            return Err(loom_core::LoomError::Other(
                "read_file: 'path' is required".into(),
            ));
        }
        let offset = args["offset"].as_u64().unwrap_or(1).max(1) as usize;
        let limit = args["limit"]
            .as_u64()
            .unwrap_or(READ_DEFAULT_LIMIT as u64)
            .min(READ_MAX_LIMIT as u64) as usize;

        let content = tokio::fs::read_to_string(path).await.map_err(|e| {
            loom_core::LoomError::Other(format!("read_file: cannot read '{path}': {e}"))
        })?;

        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();
        let start_idx = offset.saturating_sub(1);
        let end_idx = (start_idx + limit).min(total_lines);

        if start_idx >= total_lines {
            return Ok(json!({
                "content": "",
                "total_lines": total_lines,
                "offset": offset,
                "truncated": false,
            }));
        }

        let mut out = String::new();
        let mut char_count = 0usize;
        let mut lines_emitted = 0usize;
        let mut truncated = false;

        for (i, line) in lines[start_idx..end_idx].iter().enumerate() {
            let line_num = start_idx + i + 1;
            let formatted = format!("{line_num}|{line}\n");
            if char_count + formatted.len() > READ_CHAR_BUDGET {
                truncated = true;
                break;
            }
            out.push_str(&formatted);
            char_count += formatted.len();
            lines_emitted += 1;
        }

        if truncated {
            let next_offset = start_idx + lines_emitted + 1;
            Ok(json!({
                "content": out,
                "total_lines": total_lines,
                "offset": offset,
                "truncated": true,
                "truncated_by": "chars",
                "next_offset": next_offset,
                "hint": format!(
                    "Output truncated at the {READ_CHAR_BUDGET}-char budget after {lines_emitted} line(s) (lines {offset}-{} of {total_lines}). Use offset={next_offset} to continue.",
                    start_idx + lines_emitted
                ),
            }))
        } else {
            Ok(json!({
                "content": out,
                "total_lines": total_lines,
                "offset": offset,
                "truncated": false,
            }))
        }
    }

    async fn write_file(&self, args: &Value) -> Result<Value> {
        let path = args["path"].as_str().unwrap_or("");
        if path.is_empty() {
            return Err(loom_core::LoomError::Other(
                "write_file: 'path' is required".into(),
            ));
        }
        let content = match args["content"].as_str() {
            Some(c) => c,
            None => {
                return Err(loom_core::LoomError::Other(
                    "write_file: 'content' is required and must be a string".into(),
                ))
            }
        };

        let p = Path::new(path);
        if let Some(parent) = p.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    loom_core::LoomError::Other(format!("write_file: create dirs failed: {e}"))
                })?;
            }
        }

        tokio::fs::write(p, content).await.map_err(|e| {
            loom_core::LoomError::Other(format!("write_file: cannot write '{path}': {e}"))
        })?;

        Ok(json!({
            "path": path,
            "bytes_written": content.len(),
            "success": true,
        }))
    }

    async fn patch(&self, args: &Value) -> Result<Value> {
        let path = args["path"].as_str().unwrap_or("");
        if path.is_empty() {
            return Err(loom_core::LoomError::Other(
                "patch: 'path' is required".into(),
            ));
        }
        let old_string = match args["old_string"].as_str() {
            Some(s) => s,
            None => {
                return Err(loom_core::LoomError::Other(
                    "patch: 'old_string' is required".into(),
                ))
            }
        };
        let new_string = match args["new_string"].as_str() {
            Some(s) => s,
            None => {
                return Err(loom_core::LoomError::Other(
                    "patch: 'new_string' is required".into(),
                ))
            }
        };
        let replace_all = args["replace_all"].as_bool().unwrap_or(false);

        let content = tokio::fs::read_to_string(path).await.map_err(|e| {
            loom_core::LoomError::Other(format!("patch: cannot read '{path}': {e}"))
        })?;

        let (new_content, replacements) =
            fuzzy_replace(&content, old_string, new_string, replace_all)?;

        if replacements == 0 {
            return Err(loom_core::LoomError::Other(format!(
                "patch: no match found for old_string in '{path}'"
            )));
        }

        tokio::fs::write(path, &new_content).await.map_err(|e| {
            loom_core::LoomError::Other(format!("patch: cannot write '{path}': {e}"))
        })?;

        let diff = build_unified_diff(&content, &new_content, path);

        Ok(json!({
            "path": path,
            "replacements": replacements,
            "diff": diff,
        }))
    }

    async fn search_files(&self, args: &Value) -> Result<Value> {
        let pattern = args["pattern"].as_str().unwrap_or("");
        if pattern.is_empty() {
            return Err(loom_core::LoomError::Other(
                "search_files: 'pattern' is required".into(),
            ));
        }
        let target = args["target"].as_str().unwrap_or("content");
        let search_path = args["path"].as_str().unwrap_or(".");
        let file_glob = args["file_glob"].as_str();
        let limit = args["limit"].as_u64().unwrap_or(50) as usize;
        let offset = args["offset"].as_u64().unwrap_or(0) as usize;
        let output_mode = args["output_mode"].as_str().unwrap_or("content");
        let context = args["context"].as_u64().unwrap_or(0) as usize;

        let base = PathBuf::from(search_path);
        if !base.exists() {
            return Err(loom_core::LoomError::Other(format!(
                "search_files: path not found: {search_path}"
            )));
        }

        match target {
            "files" => search_by_name(&base, pattern, limit, offset),
            "content" => search_by_content(
                &base,
                pattern,
                file_glob,
                limit,
                offset,
                output_mode,
                context,
            ),
            _ => Err(loom_core::LoomError::Other(format!(
                "search_files: invalid target '{target}', expected 'content' or 'files'"
            ))),
        }
    }

    async fn list_dir(&self, args: &Value) -> Result<Value> {
        let path = args["path"].as_str().unwrap_or("");
        let mut entries = tokio::fs::read_dir(path)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("list_dir: {e}")))?;
        let mut names = vec![];
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("list_dir entry: {e}")))?
        {
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        names.sort();
        Ok(json!(names))
    }
}

/// 模糊查找替换：尝试多种匹配策略
///
/// 策略优先级：
/// 1. 精确匹配（整段字符串）
/// 2. 行级匹配（忽略行尾空白）
fn fuzzy_replace(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<(String, usize)> {
    if old == new {
        return Err(loom_core::LoomError::Other(
            "patch: new_string must differ from old_string".into(),
        ));
    }

    // 策略1：精确匹配整段字符串
    if let Some(result) = exact_replace(content, old, new, replace_all) {
        return Ok(result);
    }

    // 策略2：行级匹配（忽略每行末尾空白）
    if let Some(result) = line_replace(content, old, new, replace_all) {
        return Ok(result);
    }

    Ok((content.to_string(), 0))
}

/// 精确整段替换
fn exact_replace(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<(String, usize)> {
    if old.is_empty() {
        return None;
    }
    let count = content.matches(old).count();
    if count == 0 {
        return None;
    }
    if !replace_all && count > 1 {
        return Some((content.to_string(), 0));
    }
    let new_content = if replace_all {
        content.replace(old, new)
    } else {
        let pos = content.find(old)?;
        let mut out = String::with_capacity(content.len() - old.len() + new.len());
        out.push_str(&content[..pos]);
        out.push_str(new);
        out.push_str(&content[pos + old.len()..]);
        out
    };
    Some((new_content, count))
}

/// 行级替换：忽略每行末尾空白后匹配
fn line_replace(content: &str, old: &str, new: &str, replace_all: bool) -> Option<(String, usize)> {
    let content_lines: Vec<&str> = content.lines().collect();
    let old_lines: Vec<&str> = old.lines().collect();
    if old_lines.is_empty() {
        return None;
    }

    let matches = find_line_matches(&content_lines, &old_lines);
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some((content.to_string(), 0));
    }

    let count = if replace_all { matches.len() } else { 1 };
    let mut new_content = String::new();
    let mut last_end = 0;
    let new_lines: Vec<&str> = new.lines().collect();

    for (applied, (start, end)) in matches.into_iter().enumerate() {
        if !replace_all && applied > 0 {
            break;
        }
        new_content.push_str(&content_lines[last_end..start].join("\n"));
        if start > last_end || last_end > 0 {
            new_content.push('\n');
        }
        new_content.push_str(&new_lines.join("\n"));
        last_end = end;
    }
    if last_end < content_lines.len() {
        if last_end > 0 || !new_content.is_empty() {
            new_content.push('\n');
        }
        new_content.push_str(&content_lines[last_end..].join("\n"));
    }
    if content.ends_with('\n') && !new_content.ends_with('\n') {
        new_content.push('\n');
    }

    Some((new_content, count))
}

/// 查找所有行级匹配的 (start, end) 区间
fn find_line_matches(content_lines: &[&str], old_lines: &[&str]) -> Vec<(usize, usize)> {
    let mut results = Vec::new();
    let old_count = old_lines.len();
    let mut i = 0;
    while i + old_count <= content_lines.len() {
        let mut matched = true;
        for j in 0..old_count {
            if content_lines[i + j].trim_end() != old_lines[j].trim_end() {
                matched = false;
                break;
            }
        }
        if matched {
            results.push((i, i + old_count));
            i += old_count;
        } else {
            i += 1;
        }
    }
    results
}

/// 构建简化的 unified diff
fn build_unified_diff(old: &str, new: &str, path: &str) -> String {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    let mut diff = format!("--- a/{path}\n+++ b/{path}\n");

    // 简化的 LCS diff
    let (old_start, old_end, new_start, new_end) = find_diff_range(&old_lines, &new_lines);

    diff.push_str(&format!(
        "@@ -{},{} +{},{} @@\n",
        old_start + 1,
        old_end - old_start,
        new_start + 1,
        new_end - new_start
    ));

    for i in old_start..old_end {
        if i < old_lines.len() {
            diff.push_str(&format!("-{}\n", old_lines[i]));
        }
    }
    for i in new_start..new_end {
        if i < new_lines.len() {
            diff.push_str(&format!("+{}\n", new_lines[i]));
        }
    }

    diff
}

fn find_diff_range(old: &[&str], new: &[&str]) -> (usize, usize, usize, usize) {
    let mut start = 0;
    while start < old.len() && start < new.len() && old[start] == new[start] {
        start += 1;
    }
    let mut old_end = old.len();
    let mut new_end = new.len();
    while old_end > start && new_end > start && old[old_end - 1] == new[new_end - 1] {
        old_end -= 1;
        new_end -= 1;
    }
    (start, old_end, start, new_end)
}

/// 按文件名 glob 搜索
fn search_by_name(base: &Path, pattern: &str, limit: usize, offset: usize) -> Result<Value> {
    let matcher = glob::Pattern::new(pattern).map_err(|e| {
        loom_core::LoomError::Other(format!(
            "search_files: invalid glob pattern '{pattern}': {e}"
        ))
    })?;

    let mut results: Vec<String> = Vec::new();
    for entry in walkdir::WalkDir::new(base)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let name = entry.file_name().to_string_lossy();
            if matcher.matches(&name) {
                let rel = entry
                    .path()
                    .strip_prefix(base)
                    .unwrap_or(entry.path())
                    .to_string_lossy()
                    .to_string();
                results.push(rel);
            }
        }
    }
    results.sort();

    let total = results.len();
    let paginated: Vec<String> = results.into_iter().skip(offset).take(limit).collect();

    Ok(json!({
        "target": "files",
        "pattern": pattern,
        "path": base.to_string_lossy(),
        "total": total,
        "count": paginated.len(),
        "files": paginated,
    }))
}

/// 按内容 regex 搜索
fn search_by_content(
    base: &Path,
    pattern: &str,
    file_glob: Option<&str>,
    limit: usize,
    offset: usize,
    output_mode: &str,
    context: usize,
) -> Result<Value> {
    let re = Regex::new(pattern).map_err(|e| {
        loom_core::LoomError::Other(format!("search_files: invalid regex '{pattern}': {e}"))
    })?;

    let file_matcher = file_glob.and_then(|g| glob::Pattern::new(g).ok());

    let mut matches: Vec<Value> = Vec::new();
    let mut file_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut matched_files: Vec<String> = Vec::new();

    for entry in walkdir::WalkDir::new(base)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(ref m) = file_matcher {
            if !m.matches(&name) {
                continue;
            }
        }

        let path = entry.path();
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let rel = path
            .strip_prefix(base)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        let lines: Vec<&str> = content.lines().collect();
        let mut file_match_count = 0;

        for (i, line) in lines.iter().enumerate() {
            if re.is_match(line) {
                file_match_count += 1;
                if output_mode == "content" {
                    let line_num = i + 1;
                    let mut entry = json!({
                        "file": rel,
                        "line": line_num,
                        "content": line,
                    });
                    if context > 0 {
                        let start = i.saturating_sub(context);
                        let end = (i + context + 1).min(lines.len());
                        let ctx_lines: Vec<&str> = lines[start..end].to_vec();
                        entry["context"] = json!(ctx_lines);
                    }
                    matches.push(entry);
                }
            }
        }

        if file_match_count > 0 {
            *file_counts.entry(rel.clone()).or_insert(0) += file_match_count;
            matched_files.push(rel);
        }

        if matches.len() >= limit + offset && output_mode == "content" {
            break;
        }
    }

    match output_mode {
        "files_only" => {
            matched_files.sort();
            let total = matched_files.len();
            let paginated: Vec<String> =
                matched_files.into_iter().skip(offset).take(limit).collect();
            Ok(json!({
                "target": "content",
                "pattern": pattern,
                "path": base.to_string_lossy(),
                "output_mode": "files_only",
                "total_files": total,
                "files": paginated,
            }))
        }
        "count" => {
            let mut counts: Vec<Value> = file_counts
                .into_iter()
                .map(|(f, c)| json!({"file": f, "count": c}))
                .collect();
            counts.sort_by(|a, b| a["file"].as_str().cmp(&b["file"].as_str()));
            Ok(json!({
                "target": "content",
                "pattern": pattern,
                "path": base.to_string_lossy(),
                "output_mode": "count",
                "total_files": counts.len(),
                "counts": counts,
            }))
        }
        _ => {
            let total = matches.len();
            let paginated: Vec<Value> = matches.into_iter().skip(offset).take(limit).collect();
            Ok(json!({
                "target": "content",
                "pattern": pattern,
                "path": base.to_string_lossy(),
                "output_mode": "content",
                "total": total,
                "count": paginated.len(),
                "matches": paginated,
            }))
        }
    }
}