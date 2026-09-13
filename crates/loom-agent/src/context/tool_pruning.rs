//! 工具输出修剪
//!
//! 多层工具输出管理：
//! 1. `prune_tool_outputs` — 截断超长工具输出（基础修剪）
//! 2. `dedupe_tool_results` — 重复工具输出检测（相同内容替换为占位符）
//! 3. `pressure_demote_tail` — 压力降级（上下文紧张时降级保护区域内的大工具输出）
//! 4. skill 修剪标记 — `skill_view` 等大结果用标记替代

use loom_llm::{ChatMessage, Role};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// 旧工具输出占位符
pub(crate) const PRUNED_TOOL_PLACEHOLDER: &str = "[Old tool output cleared to save context space]";

/// 重复工具输出占位符
pub(crate) const DUPLICATE_TOOL_PLACEHOLDER: &str =
    "[Duplicate tool output — same content as a more recent call]";

/// 技能修剪标记前缀
pub(crate) const SKILL_PRUNED_MARKER_PREFIX: &str = "[SKILL_PRUNED:";

/// 技能修剪最小字符数（低于此值保留原文）
const SKILL_VIEW_PRUNE_MIN_CHARS: usize = 5000;

/// 判断工具输出是否已是压缩占位符
///
/// 仅匹配本模块生成的三种占位符前缀，避免误判以 `[` 开头的合法工具输出。
fn is_tool_placeholder(content: &str) -> bool {
    content.starts_with("[Old tool output")
        || content.starts_with("[Duplicate tool output")
        || content.starts_with("[SKILL_PRUNED:")
}

/// 最大重注入的修剪技能标记数
const MAX_PRUNED_SKILL_MARKERS: usize = 20;

/// 文件变异工具集合
const FILE_MUTATING_TOOLS: &[&str] = &["write_file", "patch"];

/// 检查文件变异结果是否落地
pub(crate) fn file_mutation_landed(tool_name: &str, result: &serde_json::Value) -> bool {
    if !FILE_MUTATING_TOOLS.contains(&tool_name) {
        return false;
    }
    if result.get("error").is_some() {
        return false;
    }
    match tool_name {
        "write_file" => result.get("bytes_written").is_some(),
        "patch" => result
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        _ => false,
    }
}

/// 修剪过长的工具输出
///
/// 对每条 tool 消息，若内容超过 `max_tool_output_chars`，截断并附加标记。
/// 返回被修剪的工具输出数量。
pub(crate) fn prune_tool_outputs(messages: &mut [ChatMessage], max_tool_output_chars: usize) -> usize {
    let mut pruned = 0;
    for msg in messages.iter_mut() {
        if msg.role != Role::Tool {
            continue;
        }
        let Some(content) = msg.content.as_mut() else {
            continue;
        };
        if content.chars().count() <= max_tool_output_chars {
            continue;
        }
        let original_len = content.chars().count();
        let truncated: String = content.chars().take(max_tool_output_chars).collect();
        *content = format!("{truncated}\n... [truncated, original {original_len} chars]");
        pruned += 1;
    }
    pruned
}

/// 重复工具输出检测
///
/// 若旧工具结果与更新的工具结果内容相同，将旧的替换为占位符。
/// 从后向前扫描，记录已见内容，遇到重复的则替换。
pub(crate) fn dedupe_tool_results(messages: &mut [ChatMessage]) -> usize {
    let mut seen: HashSet<u64> = HashSet::new();
    let mut deduped = 0;
    // 从后向前扫描：新的结果保留，旧的重复结果替换
    for msg in messages.iter_mut().rev() {
        if msg.role != Role::Tool {
            continue;
        }
        let Some(content) = msg.content.as_ref() else {
            continue;
        };
        // 已是占位符的跳过
        if is_tool_placeholder(content) {
            continue;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        content.hash(&mut hasher);
        let hash = hasher.finish();
        if !seen.insert(hash) {
            // 重复：替换为占位符
            if let Some(c) = msg.content.as_mut() {
                *c = DUPLICATE_TOOL_PLACEHOLDER.to_string();
                deduped += 1;
            }
        }
    }
    deduped
}

/// 构建技能修剪标记
pub(crate) fn skill_pruned_marker(skill_name: &str) -> String {
    format!(
        "{SKILL_PRUNED_MARKER_PREFIX} content lost in compression; reload with skill_view(name='{skill_name}')]"
    )
}

/// 从文本中提取修剪过的技能名
pub(crate) fn extract_pruned_skill_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut pos = 0;
    while let Some(start) = text[pos..].find(SKILL_PRUNED_MARKER_PREFIX) {
        let abs_start = pos + start;
        let after = &text[abs_start + SKILL_PRUNED_MARKER_PREFIX.len()..];
        if let Some(name_start) = after.find("name='") {
            let name_area = &after[name_start + 6..];
            if let Some(name_end) = name_area.find('\'') {
                names.push(name_area[..name_end].to_string());
            }
        }
        pos = abs_start + SKILL_PRUNED_MARKER_PREFIX.len();
    }
    // 去重保序
    let mut seen = HashSet::new();
    names.into_iter().filter(|n| seen.insert(n.clone())).collect()
}

/// 重注入被摘要器漏掉的技能修剪标记
///
/// LLM 可能把 `[SKILL_PRUNED]` 标记改写掉，需在摘要后重注入。
pub(crate) fn reinject_pruned_skill_markers(summary: &str, skill_names: &[String]) -> String {
    if skill_names.is_empty() {
        return summary.to_string();
    }
    let existing: HashSet<String> = extract_pruned_skill_names(summary).into_iter().collect();
    let missing: Vec<&String> = skill_names.iter().filter(|n| !existing.contains(*n)).collect();
    if missing.is_empty() {
        return summary.to_string();
    }
    let mut result = summary.to_string();
    result.push_str("\n\n## Pruned Skills\n");
    for name in missing.iter().take(MAX_PRUNED_SKILL_MARKERS) {
        result.push_str(&skill_pruned_marker(name));
        result.push('\n');
    }
    result
}

/// 修剪 skill_view 等大结果为标记
///
/// 返回 (修剪后的消息, 被修剪的技能名列表)
pub(crate) fn prune_large_skill_results(
    messages: &mut [ChatMessage],
) -> (usize, Vec<String>) {
    let mut pruned = 0;
    let mut skill_names = Vec::new();
    // 先建立 tool_call_id -> skill_name 的映射
    let mut call_id_to_skill: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for msg in messages.iter() {
        if msg.role != Role::Assistant {
            continue;
        }
        if let Some(tcs) = &msg.tool_calls {
            for tc in tcs {
                if tc.name == "skill_view" {
                    if let Some(name) = tc.arguments.get("name").and_then(|v| v.as_str()) {
                        call_id_to_skill.insert(tc.id.clone(), name.to_string());
                    }
                }
            }
        }
    }

    for msg in messages.iter_mut() {
        if msg.role != Role::Tool {
            continue;
        }
        let Some(content) = msg.content.as_ref() else {
            continue;
        };
        if content.chars().count() < SKILL_VIEW_PRUNE_MIN_CHARS {
            continue;
        }
        let Some(tcid) = msg.tool_call_id.as_ref() else {
            continue;
        };
        if let Some(skill_name) = call_id_to_skill.get(tcid) {
            if let Some(c) = msg.content.as_mut() {
                *c = skill_pruned_marker(skill_name);
                skill_names.push(skill_name.clone());
                pruned += 1;
            }
        }
    }
    (pruned, skill_names)
}

/// 压力降级：当上下文紧张时，降级保护尾部区域内的大工具输出
///
/// 在 `tail_start` 之后的区域，将超过 `min_chars` 的工具输出降级为占位符。
/// 返回降级的数量。
pub(crate) fn pressure_demote_tail(
    messages: &mut [ChatMessage],
    tail_start: usize,
    min_chars: usize,
) -> usize {
    let mut demoted = 0;
    for msg in messages.iter_mut().skip(tail_start) {
        if msg.role != Role::Tool {
            continue;
        }
        let Some(content) = msg.content.as_ref() else {
            continue;
        };
        // 已是占位符的跳过
        if is_tool_placeholder(content) {
            continue;
        }
        if content.chars().count() >= min_chars {
            if let Some(c) = msg.content.as_mut() {
                let original_len = c.chars().count();
                *c = format!(
                    "{} (original {original_len} chars)",
                    PRUNED_TOOL_PLACEHOLDER
                );
                demoted += 1;
            }
        }
    }
    demoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prune_tool_outputs() {
        let mut msgs = vec![ChatMessage::tool("tc_1", "x".repeat(5000))];
        let n = prune_tool_outputs(&mut msgs, 100);
        assert_eq!(n, 1);
        assert!(msgs[0].content.as_ref().unwrap().contains("[truncated"));
    }

    #[test]
    fn test_dedupe_tool_results() {
        let mut msgs = vec![
            ChatMessage::tool("tc_1", "same content"),
            ChatMessage::tool("tc_2", "same content"),
        ];
        let n = dedupe_tool_results(&mut msgs);
        assert_eq!(n, 1);
        // 旧的（tc_1）被替换
        assert_eq!(msgs[0].content.as_ref().unwrap(), DUPLICATE_TOOL_PLACEHOLDER);
        assert_eq!(msgs[1].content.as_ref().unwrap(), "same content");
    }

    #[test]
    fn test_skill_pruned_marker_roundtrip() {
        let marker = skill_pruned_marker("my_skill");
        assert!(marker.starts_with(SKILL_PRUNED_MARKER_PREFIX));
        let names = extract_pruned_skill_names(&marker);
        assert_eq!(names, vec!["my_skill".to_string()]);
    }

    #[test]
    fn test_reinject_pruned_skill_markers() {
        let summary = "## Completed Actions\n1. did something";
        let names = vec!["skill_a".to_string(), "skill_b".to_string()];
        let result = reinject_pruned_skill_markers(summary, &names);
        assert!(result.contains("## Pruned Skills"));
        assert!(result.contains("skill_a"));
        assert!(result.contains("skill_b"));
        // 已存在的不重复注入
        let result2 = reinject_pruned_skill_markers(&result, &names);
        assert_eq!(result2.matches("skill_a").count(), 1);
    }
}