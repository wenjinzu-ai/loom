//! Token 估算与追踪
//!
//! token 状态追踪机制：
//! - 粗略估算：4 字符 ≈ 1 token
//! - 精确追踪：从 LLM 响应的 usage 中更新 last_prompt_tokens

use loom_llm::ChatMessage;

/// 粗略估算消息列表的 token 数（4 字符 ≈ 1 token）
///
/// 估算公式：`(len(text) + 3) // 4`
pub(crate) fn estimate_tokens(messages: &[ChatMessage]) -> usize {
    let total_chars: usize = messages.iter().map(message_chars).sum();
    total_chars.div_ceil(4)
}

/// 单条消息的字符数估算
fn message_chars(msg: &ChatMessage) -> usize {
    let content_chars = msg
        .content
        .as_ref()
        .map(|c| c.chars().count())
        .unwrap_or(0);
    let tool_chars: usize = msg
        .tool_calls
        .as_ref()
        .map(|tcs| {
            tcs.iter()
                .map(|tc| tc.name.chars().count() + tc.arguments.to_string().chars().count())
                .sum()
        })
        .unwrap_or(0);
    let role_chars = msg.role.to_string().chars().count();
    // +4 为消息结构开销（role 字段、分隔符等）
    role_chars + content_chars + tool_chars + 4
}

/// Token 使用追踪器
///
/// token 状态字段：`last_prompt_tokens`, `last_completion_tokens`, `last_total_tokens`
#[derive(Debug, Clone, Default)]
pub(crate) struct TokenTracker {
    /// 上一次请求的 prompt tokens（来自 provider usage）
    pub last_prompt_tokens: usize,
    /// 上一次响应的 completion tokens
    pub last_completion_tokens: usize,
    /// 上一次请求的 total tokens
    pub last_total_tokens: usize,
    /// 上一次更新 usage 时的消息数（用于检测消息是否变化）
    last_message_count: usize,
}

impl TokenTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// 从 LLM 响应 usage 更新 token 统计
    pub fn update_from_response(&mut self, prompt_tokens: usize, completion_tokens: usize) {
        self.last_prompt_tokens = prompt_tokens;
        self.last_completion_tokens = completion_tokens;
        self.last_total_tokens = prompt_tokens + completion_tokens;
    }

    /// 记录当前消息数（在更新 usage 时同步调用）
    pub fn note_message_count(&mut self, count: usize) {
        self.last_message_count = count;
    }

    /// 重置 token 缓存（压缩导致消息结构变化时调用）
    pub fn reset_cache(&mut self) {
        self.last_prompt_tokens = 0;
        self.last_message_count = 0;
    }

    /// 获取当前用于判断是否压缩的 token 数
    ///
    /// 优先使用精确的 last_prompt_tokens；若消息数自上次更新后已变化
    /// （说明新增了工具输出等内容），则回退到粗略估算，避免用过期值
    /// 导致压缩判断滞后。
    pub fn current_tokens(&self, messages: &[ChatMessage]) -> usize {
        if self.last_prompt_tokens > 0 && messages.len() == self.last_message_count {
            self.last_prompt_tokens
        } else {
            estimate_tokens(messages)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_tokens_basic() {
        let msgs = vec![
            ChatMessage::system("system prompt"),
            ChatMessage::user("hello world"),
        ];
        let tokens = estimate_tokens(&msgs);
        assert!(tokens > 0);
    }

    #[test]
    fn test_token_tracker_prefers_usage() {
        let mut tracker = TokenTracker::new();
        let msgs = vec![ChatMessage::user("test")];
        // 无 usage 时回退估算
        assert_eq!(tracker.current_tokens(&msgs), estimate_tokens(&msgs));
        // 有 usage 且消息数匹配时使用精确值
        tracker.update_from_response(5000, 100);
        tracker.note_message_count(msgs.len());
        assert_eq!(tracker.current_tokens(&msgs), 5000);
        // 消息数变化（新增了工具输出等）后回退估算，避免用过期值
        let more_msgs = vec![ChatMessage::user("test"), ChatMessage::assistant("resp")];
        assert_eq!(tracker.current_tokens(&more_msgs), estimate_tokens(&more_msgs));
    }
}