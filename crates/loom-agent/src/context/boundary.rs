//! 压缩边界计算
//!
//! 压缩窗口计算：
//! - 头部保护：system + 前 N 条消息（protect_first_n）
//! - 尾部保护：基于 token 预算（lean）或固定消息数（legacy）
//! - 边界对齐到非 tool 消息，避免拆分 assistant tool_call / tool tool_response 对

use loom_llm::{ChatMessage, Role};

use super::config::TailMode;
use super::token::estimate_tokens;

/// 压缩窗口（中间可摘要区域）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressWindow {
    /// 头部保护区域的结束索引（不含）
    pub compress_start: usize,
    /// 尾部保护区域的起始索引
    pub compress_end: usize,
}

/// 计算头部保护大小
///
/// 头部保护大小计算：
/// 保护 system prompt + 前 protect_first_n 条非 system 消息。
pub fn protect_head_size(messages: &[ChatMessage], protect_first_n: usize) -> usize {
    let mut head = 0;
    // 跳过所有开头的 system 消息
    while head < messages.len() && messages[head].role == Role::System {
        head += 1;
    }
    // 再保护 protect_first_n 条消息
    let mut count = 0;
    while head < messages.len() && count < protect_first_n {
        head += 1;
        count += 1;
    }
    head
}

/// 计算尾部保护起始索引（基于 token 预算）
///
/// 尾部预算遍历（lean 模式）：
/// 从尾部向前累加 token，直到超过 tail_token_budget，
/// 且至少保留 min_tail_user_messages 个 user 消息。
pub fn tail_start_by_budget(
    messages: &[ChatMessage],
    tail_token_budget: usize,
    min_tail_user_messages: usize,
) -> usize {
    let n = messages.len();
    let mut tokens = 0usize;
    let mut user_count = 0usize;
    let mut idx = n;

    while idx > 0 {
        idx -= 1;
        let msg_tokens = estimate_tokens(&messages[idx..idx + 1]);
        // 如果加上这条消息会超过预算，且已满足最小 user 消息数，则停止
        if tokens + msg_tokens > tail_token_budget && user_count >= min_tail_user_messages {
            idx += 1;
            break;
        }
        tokens += msg_tokens;
        if messages[idx].role == Role::User {
            user_count += 1;
        }
    }

    // 边界对齐：向前对齐到非 tool 消息（避免拆分 tool 对）
    snap_boundary_forward(messages, idx, n)
}

/// 计算尾部保护起始索引（legacy 模式：固定消息数）
///
/// 保留最后 N 条消息，但至少保留
/// min_tail_user_messages 个 user 消息。
pub fn tail_start_by_count(
    messages: &[ChatMessage],
    protect_last_n: usize,
    min_tail_user_messages: usize,
) -> usize {
    let n = messages.len();
    let mut idx = n.saturating_sub(protect_last_n);

    // 确保至少保留 min_tail_user_messages 个 user 消息
    let user_indices: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role == Role::User)
        .map(|(i, _)| i)
        .collect();
    if user_indices.len() > min_tail_user_messages {
        let min_user_start = user_indices[user_indices.len() - min_tail_user_messages];
        idx = idx.min(min_user_start);
    }

    snap_boundary_forward(messages, idx, n)
}

/// 将边界向前对齐到非 tool 消息
///
/// tool 消息总是跟在它响应的 assistant 消息之后，
/// 落在 tool 消息上的边界会切断配对，因此向前对齐。
fn snap_boundary_forward(messages: &[ChatMessage], idx: usize, max_idx: usize) -> usize {
    let mut i = idx;
    while i < max_idx && messages[i].role == Role::Tool {
        i += 1;
    }
    if i < max_idx {
        i
    } else {
        idx
    }
}

/// 计算压缩窗口
///
/// 综合头部保护和尾部保护，返回中间可摘要区域。
/// 若无可摘要区域（compress_start >= compress_end），返回 None。
///
/// 根据 `tail_mode` 选择尾部保护策略：
/// - `Lean`：基于 token 预算（`tail_token_budget`）
/// - `Legacy`：基于固定消息数（`protect_last_n`）
pub fn compute_compress_window(
    messages: &[ChatMessage],
    protect_first_n: usize,
    tail_mode: TailMode,
    tail_token_budget: usize,
    protect_last_n: usize,
    min_tail_user_messages: usize,
) -> Option<CompressWindow> {
    if messages.len() < 4 {
        return None;
    }
    let compress_start = protect_head_size(messages, protect_first_n);
    let compress_end = match tail_mode {
        TailMode::Lean => {
            tail_start_by_budget(messages, tail_token_budget, min_tail_user_messages)
        }
        TailMode::Legacy => {
            tail_start_by_count(messages, protect_last_n, min_tail_user_messages)
        }
    };

    if compress_start >= compress_end {
        return None;
    }
    Some(CompressWindow {
        compress_start,
        compress_end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msgs(n: usize) -> Vec<ChatMessage> {
        let mut msgs = vec![
            ChatMessage::system("system prompt".to_string()),
            ChatMessage::user("Goal: test".to_string()),
        ];
        for i in 0..n {
            msgs.push(ChatMessage::assistant(format!("response {i}")));
            msgs.push(ChatMessage::tool(format!("tc_{i}"), format!("result {i}")));
        }
        msgs
    }

    #[test]
    fn test_protect_head_size() {
        let msgs = make_msgs(5);
        // system + 3 条 = 4
        assert_eq!(protect_head_size(&msgs, 3), 4);
    }

    #[test]
    fn test_compute_compress_window() {
        // 使用较大的工具输出 + 多个 user 消息，使 token 预算有意义
        let mut msgs = vec![
            ChatMessage::system("system prompt".to_string()),
            ChatMessage::user("Goal: test".to_string()),
        ];
        for i in 0..30 {
            msgs.push(ChatMessage::assistant(format!("response {i}")));
            msgs.push(ChatMessage::tool(format!("tc_{i}"), format!("result {i} {}", "x".repeat(500))));
            if i % 5 == 4 {
                msgs.push(ChatMessage::user(format!("follow up question {i}")));
            }
        }
        let window = compute_compress_window(&msgs, 3, TailMode::Lean, 500, 20, 1);
        assert!(window.is_some());
        let w = window.unwrap();
        assert!(w.compress_start < w.compress_end);
        assert!(w.compress_start >= 1); // 至少跳过 system
    }

    #[test]
    fn test_compute_compress_window_legacy() {
        let msgs = make_msgs(10);
        // Legacy 模式使用 protect_last_n=4，应比 Lean 保留更多尾部消息
        let window = compute_compress_window(&msgs, 2, TailMode::Legacy, 500, 4, 1);
        assert!(window.is_some());
        let w = window.unwrap();
        // 尾部至少保留 4 条消息
        assert!(msgs.len() - w.compress_end >= 4);
    }

    #[test]
    fn test_snap_boundary_skips_tools() {
        let msgs = vec![
            ChatMessage::assistant("a"),
            ChatMessage::tool("tc1", "r1"),
            ChatMessage::tool("tc2", "r2"),
            ChatMessage::user("next"),
        ];
        // idx=1 指向 tool，应向前跳到 idx=3 (user)
        let result = snap_boundary_forward(&msgs, 1, 4);
        assert_eq!(result, 3);
    }
}