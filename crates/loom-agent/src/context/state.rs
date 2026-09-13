//! 压缩状态管理
//!
//! 内部状态：
//! - `previous_summary`：迭代式摘要，每次压缩基于上一次摘要更新
//! - cooldown：摘要失败后冷却，防止反复失败阻塞
//! - terminal failure：连续失败达到上限后标记，后续跳过 LLM 摘要
//! - stats：压缩统计信息

use std::time::{Duration, Instant};

/// 压缩失败类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionFailure {
    /// LLM 摘要返回空
    Empty,
    /// LLM 调用报错
    Error(String),
}

/// 压缩统计信息（最近一次压缩的快照，非累积值）
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// 本次压缩修剪的工具输出数
    pub tool_outputs_pruned: usize,
    /// 本次压缩去重的工具输出数
    pub tool_outputs_deduped: usize,
    /// 本次压缩降级的尾部工具输出数
    pub tail_outputs_demoted: usize,
    /// 本次压缩修剪的大技能结果数
    pub skill_results_pruned: usize,
    /// 压缩前消息数
    pub messages_before: usize,
    /// 压缩后消息数
    pub messages_after: usize,
    /// 压缩前估算 tokens
    pub tokens_before: usize,
    /// 压缩后估算 tokens
    pub tokens_after: usize,
    /// 是否使用了 LLM 摘要（false=fallback 摘要）
    pub used_llm_summary: bool,
}

/// 可序列化的压缩状态快照
///
/// 用于持久化到 checkpoint，支持 resume 后恢复压缩状态。
/// `cooldown_remaining_secs` 存储剩余冷却秒数，恢复时重建 `Instant`。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompressionStateSnapshot {
    /// 上一次生成的摘要内容（迭代更新用）
    #[serde(default)]
    pub previous_summary: Option<String>,
    /// 剩余冷却秒数（0 或 None 表示无冷却）
    #[serde(default)]
    pub cooldown_remaining_secs: Option<u64>,
    /// 连续摘要失败次数
    #[serde(default)]
    pub consecutive_failures: usize,
    /// 是否进入 terminal failure 状态
    #[serde(default)]
    pub terminal_failure: bool,
}

/// 压缩状态（跨压缩调用持久）
#[derive(Debug)]
pub struct CompressionState {
    /// 上一次生成的摘要内容（迭代更新用）
    pub previous_summary: Option<String>,
    /// 冷却结束时间（摘要失败后设置）
    cooldown_until: Option<Instant>,
    /// 连续摘要失败次数
    pub consecutive_failures: usize,
    /// 是否进入 terminal failure 状态（不再尝试 LLM 摘要）
    pub terminal_failure: bool,
}

impl Default for CompressionState {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionState {
    pub fn new() -> Self {
        Self {
            previous_summary: None,
            cooldown_until: None,
            consecutive_failures: 0,
            terminal_failure: false,
        }
    }

    /// 导出可序列化快照（用于持久化到 checkpoint）
    pub fn snapshot(&self) -> CompressionStateSnapshot {
        let cooldown_remaining_secs = self.cooldown_until.and_then(|until| {
            let now = Instant::now();
            if until > now {
                Some((until - now).as_secs())
            } else {
                None
            }
        });
        CompressionStateSnapshot {
            previous_summary: self.previous_summary.clone(),
            cooldown_remaining_secs,
            consecutive_failures: self.consecutive_failures,
            terminal_failure: self.terminal_failure,
        }
    }

    /// 从快照恢复状态
    pub fn restore(snapshot: CompressionStateSnapshot) -> Self {
        let cooldown_until = snapshot
            .cooldown_remaining_secs
            .filter(|s| *s > 0)
            .map(|s| Instant::now() + Duration::from_secs(s));
        Self {
            previous_summary: snapshot.previous_summary,
            cooldown_until,
            consecutive_failures: snapshot.consecutive_failures,
            terminal_failure: snapshot.terminal_failure,
        }
    }

    /// 检查当前是否在冷却期内
    pub fn is_cooling_down(&self) -> bool {
        match self.cooldown_until {
            Some(until) => Instant::now() < until,
            None => false,
        }
    }

    /// 记录摘要成功
    pub fn mark_success(&mut self, summary: String) {
        self.previous_summary = Some(summary);
        self.consecutive_failures = 0;
        self.cooldown_until = None;
        self.terminal_failure = false;
    }

    /// 记录摘要失败，进入冷却
    pub fn mark_failure(&mut self, cooldown_secs: u64) {
        self.consecutive_failures += 1;
        self.cooldown_until = Some(Instant::now() + Duration::from_secs(cooldown_secs));
        // 连续失败 3 次进入 terminal failure
        if self.consecutive_failures >= 3 {
            self.terminal_failure = true;
        }
    }

    /// 是否应该跳过 LLM 摘要（terminal failure 或冷却中）
    pub fn should_skip_llm_summary(&self) -> bool {
        self.terminal_failure || self.is_cooling_down()
    }

    /// 重置状态（新会话时调用）
    pub fn reset(&mut self) {
        self.previous_summary = None;
        self.cooldown_until = None;
        self.consecutive_failures = 0;
        self.terminal_failure = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cooldown() {
        let mut state = CompressionState::new();
        assert!(!state.is_cooling_down());
        state.mark_failure(3600);
        assert!(state.is_cooling_down());
        assert!(state.should_skip_llm_summary());
    }

    #[test]
    fn test_terminal_failure() {
        let mut state = CompressionState::new();
        state.mark_failure(0);
        state.mark_failure(0);
        assert!(!state.terminal_failure);
        state.mark_failure(0);
        assert!(state.terminal_failure);
        assert!(state.should_skip_llm_summary());
    }

    #[test]
    fn test_success_resets_failures() {
        let mut state = CompressionState::new();
        state.mark_failure(0);
        state.mark_failure(0);
        state.mark_failure(0);
        assert!(state.terminal_failure);
        state.mark_success("summary".to_string());
        assert!(!state.terminal_failure);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.previous_summary.as_deref(), Some("summary"));
    }
}