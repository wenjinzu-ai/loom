use loom_llm::{ChatMessage, Role, ToolCall};
use std::time::Instant;

use crate::hitl::AgentEvent;
use crate::result::ToolTrace;

/// 工具轨迹保留的最大条目数（最近 N 条工具调用结果）
pub const TOOL_TRACE_MAX_ENTRIES: usize = 10;

/// 构建 Finished 事件，附带执行时长和工具轨迹
pub fn finished_event(
    messages: &[ChatMessage],
    iterations: usize,
    tool_calls_made: usize,
    tool_names: &[String],
    response: String,
    start: Instant,
) -> AgentEvent {
    let duration_ms = start.elapsed().as_millis() as u64;
    let tool_trace = ToolTrace::from_messages(messages, TOOL_TRACE_MAX_ENTRIES);
    let tool_trace_json = serde_json::to_value(tool_trace).ok();
    AgentEvent::Finished {
        response,
        iterations,
        tool_calls_made,
        tool_names: tool_names.to_vec(),
        duration_ms,
        tool_trace: tool_trace_json,
    }
}

/// 从工具调用列表中查找 interrupt 调用
pub fn find_interrupt_call(tool_calls: &[ToolCall]) -> Option<&ToolCall> {
    tool_calls.iter().find(|tc| tc.name == "interrupt")
}

/// 提取最近一轮的 (user_content, assistant_content)，用于记忆同步
pub fn last_turn_pair(messages: &[ChatMessage]) -> (String, String) {
    let user = messages
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .and_then(|m| m.content.clone())
        .unwrap_or_default();
    let assistant = messages
        .iter()
        .rev()
        .find(|m| m.role == Role::Assistant)
        .and_then(|m| m.content.clone())
        .unwrap_or_default();
    (user, assistant)
}