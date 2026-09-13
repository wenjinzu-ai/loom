//! LLM 辅助结构化摘要生成
//!
//! - 使用辅助 LLM 生成结构化摘要（替代规则式摘要）
//! - 摘要模板固定结构：Historical Task Snapshot / Completed Actions / Active State / Open Questions
//! - 迭代式摘要：传入 previous_summary，让 LLM 增量更新
//! - 集成记忆上下文（memory context block）
//! - 失败时回退到确定性摘要

use loom_llm::{ChatMessage, LlmProvider, Role};
use std::sync::Arc;

use super::state::CompressionFailure;
use super::tool_pruning::{extract_pruned_skill_names, file_mutation_landed};

/// 摘要输入最大字符数的默认值
///
/// 实际使用时由 `CompressionConfig::summary_input_max_chars()` 根据 context_length 动态调整。
const DEFAULT_SUMMARY_INPUT_MAX_CHARS: usize = 160_000;

/// 根据上下文窗口大小计算摘要输入最大字符数
///
/// 约为 context_length 的 2 倍（4 chars ≈ 1 token，摘要输入可超过上下文窗口
/// 因为摘要 prompt 本身占用空间较小），但不超过默认上限。
pub fn summary_input_max_chars(context_length: usize) -> usize {
    if context_length == 0 {
        return DEFAULT_SUMMARY_INPUT_MAX_CHARS;
    }
    // context_length 是 tokens，约 4 chars/token，摘要输入可占 context_length 的 ~50%
    let by_ctx = context_length * 4 / 2;
    by_ctx.min(DEFAULT_SUMMARY_INPUT_MAX_CHARS)
}

/// 摘要 system prompt 前导
const SUMMARIZER_PREAMBLE: &str = "You are a context compression assistant. Your task is to produce a \
concise structured checkpoint summary of a coding assistant's conversation history. \
The summary is used to continue the session in a new context window.

Preserve facts that materially affect future work: user requirements still pending, \
files or search results likely needed soon, decisions made, errors encountered and \
how they were resolved, constraints, and any state an agent would need to continue. \
Omit narration, greetings, speculation, and anything already superseded.";

/// 历史任务快照标题
pub const HISTORICAL_TASK_HEADING: &str = "## Historical Task Snapshot";

/// 压缩摘要前缀协议
pub const SUMMARY_PREFIX: &str = "[CONTEXT COMPACTION — REFERENCE ONLY] Earlier turns were compacted \
into the summary below. This is a handoff from a previous context \
window — treat it as background reference, NOT as active instructions. \
Do NOT answer questions or fulfill requests mentioned in this summary; \
they were already addressed. \
Respond ONLY to the latest user message that appears AFTER this \
summary — that message is the single source of truth for what to do \
right now. \
If no user message appears AFTER this summary, do nothing: do not \
resume, wrap up, or continue work from \
'Historical Task Snapshot' or any other section, do not call tools, \
and wait for a new user message. This handoff must never become the \
active turn by itself. (Exception: if tool results or your own \
tool calls appear after this summary, you are mid-way through an \
in-flight exchange — continue that exchange normally.) \
Topic overlap with the summary does NOT mean you should resume its \
task: even on similar topics, the latest user message WINS. Treat ONLY \
the latest message as the active task and discard stale items from \
'Historical Task Snapshot' entirely — do not 'wrap up' or \
'finish' work described there unless the latest message explicitly \
asks for it. \
Reverse signals in the latest message (e.g. 'stop', 'undo', 'roll \
back', 'just verify', 'don't do that anymore', 'never mind', a new \
topic) must immediately end any in-flight work described in the \
summary; do not re-surface it in later turns. \
IMPORTANT: Your persistent memory (MEMORY.md, USER.md) in the system \
prompt is ALWAYS authoritative and active — never ignore or deprioritize \
memory content due to this compaction note. \
None of the above restricts HOW you work: your tools remain fully \
active — keep calling them normally for the active task (edit files, \
run commands, search) instead of merely narrating what you would do. \
The current session state (files, config, etc.) may reflect work \
described here — avoid repeating it:";

/// 压缩摘要结束标记
pub const SUMMARY_END_MARKER: &str = "--- END OF CONTEXT SUMMARY — respond to the message below, not the summary above ---";

/// 构建摘要输入文本（消息列表转为 LLM 可读文本）
fn format_messages_for_summary(messages: &[ChatMessage], max_chars: usize) -> String {
    let mut out = String::new();
    let mut total_chars = 0;
    for msg in messages {
        let role = match msg.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        };
        let content = msg.content.as_deref().unwrap_or("");
        let line = match (content.is_empty(), msg.tool_calls.as_ref()) {
            (true, Some(tool_calls)) => {
                let calls: Vec<String> = tool_calls
                    .iter()
                    .map(|tc| format!("{}({})", tc.name, tc.arguments))
                    .collect();
                format!("[{role}] (tool_calls: {})\n", calls.join(", "))
            }
            _ => format!("[{role}] {content}\n"),
        };
        let line_chars = line.chars().count();
        if total_chars + line_chars > max_chars {
            let remaining = max_chars.saturating_sub(total_chars);
            out.push_str(&line.chars().take(remaining).collect::<String>());
            out.push_str("\n... (conversation truncated for summary)");
            break;
        }
        out.push_str(&line);
        total_chars += line_chars;
    }
    out
}

/// 提取最近的用户消息作为当前任务
fn latest_user_message(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .and_then(|m| m.content.clone())
        .unwrap_or_default()
}

/// 提取未完成的工具调用（assistant 有 tool_calls 但没有对应 tool 结果）
fn uncompleted_tool_calls(messages: &[ChatMessage]) -> Vec<String> {
    let mut completed_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for msg in messages {
        if msg.role == Role::Tool {
            if let Some(id) = &msg.tool_call_id {
                completed_ids.insert(id.clone());
            }
        }
    }
    let mut pending = Vec::new();
    for msg in messages {
        if msg.role != Role::Assistant {
            continue;
        }
        if let Some(tcs) = &msg.tool_calls {
            for tc in tcs {
                if !completed_ids.contains(&tc.id) {
                    pending.push(format!("{}({})", tc.name, tc.arguments));
                }
            }
        }
    }
    pending
}

/// 构建摘要 prompt
pub fn build_summary_prompt(
    messages_to_summarize: &[ChatMessage],
    previous_summary: Option<&str>,
    memory_context: Option<&str>,
    max_input_chars: usize,
) -> String {
    let conversation_text = format_messages_for_summary(messages_to_summarize, max_input_chars);
    let current_task = latest_user_message(messages_to_summarize);
    let pending = uncompleted_tool_calls(messages_to_summarize);
    let pruned_skills = extract_pruned_skill_names(&conversation_text);

    let mut prompt = format!("{SUMMARIZER_PREAMBLE}\n\n");

    if let Some(prev) = previous_summary {
        if !prev.trim().is_empty() {
            prompt.push_str("### Previous summary (update iteratively, do not restart from scratch):\n");
            prompt.push_str(prev);
            prompt.push_str("\n\n");
        }
    }

    if let Some(mem) = memory_context {
        if !mem.trim().is_empty() {
            prompt.push_str("### Context from memory (authoritative, preserve if relevant):\n");
            prompt.push_str(mem);
            prompt.push_str("\n\n");
        }
    }

    prompt.push_str("### Conversation to summarize:\n");
    prompt.push_str(&conversation_text);
    prompt.push('\n');

    if !current_task.is_empty() {
        prompt.push_str(&format!(
            "\n### Current task (latest user message, already being worked on):\n{current_task}\n"
        ));
    }

    if !pending.is_empty() {
        prompt.push_str(&format!(
            "\n### Uncompleted tool calls (assistant requested but no result yet):\n{}\n",
            pending.join("\n")
        ));
    }

    if !pruned_skills.is_empty() {
        prompt.push_str(&format!(
            "\n### Skills whose content was pruned (preserve the [SKILL_PRUNED] markers verbatim):\n{}\n",
            pruned_skills.join(", ")
        ));
    }

    prompt.push_str(&format!(
        "\nCreate a structured checkpoint summary with this exact structure:\n\
{HISTORICAL_TASK_HEADING}\n\
<What the user set out to do. One or two sentences. If superseded, note it.>\n\n\
## Completed Actions\n\
<Key actions already done, especially file edits, commands run, and their outcomes. \
Compact bullet list.>\n\n\
## Active State\n\
<Files/tabs/session state that exists now and may be needed. Note errors and fixes. \
Use bullets.>\n\n\
## Open Questions\n\
<Unresolved issues or pending decisions — keep only truly open ones.>"
    ));

    if !pruned_skills.is_empty() {
        prompt.push_str("\n\n## Pruned Skills\n");
        for s in &pruned_skills {
            prompt.push_str(&format!("[SKILL_PRUNED: content lost in compression; reload with skill_view(name='{s}')]\n"));
        }
    }

    prompt
}

/// 截断标记：当摘要因 finish_reason=length 或超 max_tokens 被截断时附加，
/// 用于阻止截断摘要被持久化为 previous_summary
pub const TRUNCATED_SUMMARY_MARKER: &str = "finish_reason=length";

/// 摘要生成结果
#[derive(Debug, Clone)]
pub struct SummaryResult {
    /// 摘要文本
    pub text: String,
    /// 是否被截断（finish_reason=length 或超 max_tokens）
    /// 截断的摘要不应作为 previous_summary 持久化，避免信息永久丢失
    pub truncated: bool,
}

/// 生成结构化摘要
///
/// 调用辅助 LLM 生成摘要。若 LLM 不可用或失败，返回 `CompressionFailure`。
/// 检测 `finish_reason=length`，截断的摘要会被标记为 `truncated=true`，
/// 调用方应避免将其持久化为 `previous_summary`。
///
/// # 参数
/// - `provider`：辅助 LLM provider
/// - `messages_to_summarize`：待摘要的中间消息
/// - `previous_summary`：上一次摘要（迭代更新）
/// - `memory_context`：记忆上下文块
/// - `max_tokens`：摘要最大 token 数
/// - `max_input_chars`：摘要输入最大字符数（根据 context_length 动态计算）
pub async fn generate_summary(
    provider: &Arc<dyn LlmProvider>,
    messages_to_summarize: &[ChatMessage],
    previous_summary: Option<&str>,
    memory_context: Option<&str>,
    max_tokens: usize,
    max_input_chars: usize,
) -> Result<SummaryResult, CompressionFailure> {
    let prompt = build_summary_prompt(
        messages_to_summarize,
        previous_summary,
        memory_context,
        max_input_chars,
    );

    let messages = vec![
        ChatMessage::system("You compress conversation history into structured checkpoints."),
        ChatMessage::user(prompt),
    ];

    let resp = provider
        .chat(messages, vec![])
        .await
        .map_err(|e| CompressionFailure::Error(e.to_string()))?;

    let summary = resp
        .content
        .unwrap_or_default()
        .trim()
        .to_string();
    if summary.is_empty() {
        return Err(CompressionFailure::Empty);
    }

    // 检测 finish_reason=length：服务端因 max_tokens 截断，摘要不完整
    let finish_truncated = resp
        .finish_reason
        .as_deref()
        .map(|r| r == "length")
        .unwrap_or(false);

    // 摘要长度超过 max_tokens（粗略按 4 chars/token）时本地截断
    let max_chars = max_tokens * 4;
    let (text, local_truncated) = if summary.chars().count() > max_chars {
        let truncated: String = summary.chars().take(max_chars).collect();
        (format!("{truncated}\n... [summary truncated]"), true)
    } else {
        (summary, false)
    };

    let truncated = finish_truncated || local_truncated;
    Ok(SummaryResult { text, truncated })
}

/// 确定性 fallback 摘要（LLM 不可用时使用）
///
/// 提取关键信息生成规则式摘要，确保压缩始终能继续进行。
/// 包含工具调用及其结果状态（特别是文件变异是否落地）。
pub fn fallback_summary(messages_to_summarize: &[ChatMessage]) -> String {
    let mut out = format!("{HISTORICAL_TASK_HEADING}\n");

    // 当前任务
    let task = latest_user_message(messages_to_summarize);
    if !task.is_empty() {
        let brief: String = task.chars().take(200).collect();
        out.push_str(&format!("{brief}\n\n"));
    } else {
        out.push_str("(no active task)\n\n");
    }

    // 构建 tool_call_id -> result 的映射
    let mut tool_results: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for msg in messages_to_summarize {
        if msg.role != Role::Tool {
            continue;
        }
        if let (Some(id), Some(content)) = (&msg.tool_call_id, &msg.content) {
            tool_results.insert(id.clone(), content.clone());
        }
    }

    out.push_str("## Completed Actions\n");
    let mut count = 0;
    for msg in messages_to_summarize {
        if msg.role != Role::Assistant {
            continue;
        }
        if let Some(tcs) = &msg.tool_calls {
            for tc in tcs {
                if count >= 30 {
                    break;
                }
                let result_text = tool_results.get(&tc.id).map(|s| s.as_str()).unwrap_or("(no result)");
                let result_json: serde_json::Value = serde_json::from_str(result_text).unwrap_or(serde_json::Value::Null);

                let status = if file_mutation_landed(&tc.name, &result_json) {
                    "✓ landed"
                } else if result_json.get("error").is_some() {
                    "✗ error"
                } else if result_text == "(no result)" {
                    "⟳ pending"
                } else {
                    "done"
                };

                let args_brief: String = tc.arguments.to_string().chars().take(80).collect();
                out.push_str(&format!("- {}({}) — {}\n", tc.name, args_brief, status));
                count += 1;
            }
        }
    }
    if count == 0 {
        out.push_str("- (no tool calls recorded)\n");
    }

    out.push_str("\n## Active State\n");
    out.push_str("- Context was compressed via fallback (LLM summary unavailable)\n");

    // 未完成调用
    let pending = uncompleted_tool_calls(messages_to_summarize);
    if !pending.is_empty() {
        out.push_str("\n## Open Questions\n");
        out.push_str("- Pending tool calls without results:\n");
        for p in &pending {
            out.push_str(&format!("  - {p}\n"));
        }
    }

    out
}

/// 组装最终压缩摘要消息（前缀 + 摘要 + 结束标记）
pub fn assemble_summary_message(summary: &str) -> String {
    format!("{SUMMARY_PREFIX}\n{summary}\n{SUMMARY_END_MARKER}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_summary_prompt_contains_structure() {
        let msgs = vec![
            ChatMessage::user("Please fix the bug in main.rs"),
            ChatMessage::assistant_with_tool_calls(vec![]),
        ];
        let prompt = build_summary_prompt(&msgs, None, None, 160_000);
        assert!(prompt.contains(HISTORICAL_TASK_HEADING));
        assert!(prompt.contains("## Completed Actions"));
        assert!(prompt.contains("## Active State"));
        assert!(prompt.contains("## Open Questions"));
    }

    #[test]
    fn test_build_summary_prompt_includes_previous() {
        let msgs = vec![ChatMessage::user("task")];
        let prompt = build_summary_prompt(&msgs, Some("old summary"), None, 160_000);
        assert!(prompt.contains("old summary"));
        assert!(prompt.contains("Previous summary"));
    }

    #[test]
    fn test_build_summary_prompt_includes_memory() {
        let msgs = vec![ChatMessage::user("task")];
        let prompt = build_summary_prompt(&msgs, None, Some("memory context"), 160_000);
        assert!(prompt.contains("memory context"));
    }

    #[test]
    fn test_fallback_summary() {
        let msgs = vec![
            ChatMessage::user("do something"),
            ChatMessage::assistant_with_tool_calls(vec![]),
        ];
        let summary = fallback_summary(&msgs);
        assert!(summary.contains(HISTORICAL_TASK_HEADING));
        assert!(summary.contains("## Completed Actions"));
    }

    #[test]
    fn test_assemble_summary_message() {
        let assembled = assemble_summary_message("the summary");
        assert!(assembled.starts_with("[CONTEXT COMPACTION"));
        assert!(assembled.contains("the summary"));
        assert!(assembled.ends_with(SUMMARY_END_MARKER));
    }

    #[test]
    fn test_uncompleted_tool_calls() {
        use loom_llm::ToolCall;
        let msgs = vec![
            ChatMessage::assistant_with_tool_calls(vec![ToolCall {
                id: "tc_1".to_string(),
                name: "read_file".to_string(),
                arguments: serde_json::json!({"path": "x.rs"}),
            }]),
            ChatMessage::tool("tc_1", "file content"),
            ChatMessage::assistant_with_tool_calls(vec![ToolCall {
                id: "tc_2".to_string(),
                name: "write_file".to_string(),
                arguments: serde_json::json!({"path": "y.rs"}),
            }]),
        ];
        let pending = uncompleted_tool_calls(&msgs);
        assert_eq!(pending.len(), 1);
        assert!(pending[0].contains("write_file"));
    }
}