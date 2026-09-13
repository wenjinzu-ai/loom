//! ContextEngine trait — 可插拔上下文引擎抽象
//!
//! - 定义统一的压缩接口 `compress`
//! - 允许替换不同压缩策略实现（默认 `ContextCompressor`）
//! - 所有引擎共享相同的状态字段和生命周期方法

use async_trait::async_trait;
use loom_llm::ChatMessage;

use super::state::CompressionStats;

/// 引擎运行时状态
#[derive(Debug, Clone, Default)]
pub struct EngineStatus {
    /// 最近一次请求的 prompt tokens
    pub last_prompt_tokens: usize,
    /// 当前压缩阈值（tokens）
    pub threshold_tokens: usize,
    /// 使用率百分比（last_prompt_tokens / threshold_tokens）
    pub usage_percent: f64,
    /// 是否即将需要压缩
    pub approaching_limit: bool,
}

/// 压缩预检结果
#[derive(Debug, Clone)]
pub struct CompressPreflight {
    /// 是否应该压缩
    pub should_compress: bool,
    /// 原因描述
    pub reason: String,
}

/// 上下文引擎 trait
///
/// 所有上下文压缩引擎必须实现此 trait。
#[async_trait]
pub trait ContextEngine: Send + Sync {
    /// 压缩消息列表
    ///
    /// # 参数
    /// - `messages`：当前完整消息列表（含 system / user / assistant / tool）
    /// - `memory_context`：可选的记忆上下文块（压缩前从 memory provider 提取的洞察）
    ///
    /// # 返回
    /// - `Ok(Some(compressed))`：压缩后的消息列表
    /// - `Ok(None)`：无需压缩
    /// - `Err(e)`：压缩失败
    async fn compress(
        &self,
        messages: Vec<ChatMessage>,
        memory_context: Option<String>,
    ) -> anyhow::Result<Option<Vec<ChatMessage>>>;

    /// 从 LLM 响应更新 token 使用统计
    fn record_token_usage(&self, prompt_tokens: usize, completion_tokens: usize);

    /// 获取压缩统计
    fn stats(&self) -> CompressionStats;

    /// 重置引擎状态（新会话时调用）
    fn reset(&self);

    /// 判断当前是否需要压缩
    fn should_compress(&self, messages: &[ChatMessage]) -> bool;

    /// 每次请求前选择/替换上下文（检索/主题路由/分支切换）
    ///
    /// 返回 `None` 表示不替换，返回 `Some(messages)` 表示用新上下文替换。
    /// 默认实现返回 `None`（不替换）。
    async fn select_context(
        &self,
        _messages: Vec<ChatMessage>,
    ) -> anyhow::Result<Option<Vec<ChatMessage>>> {
        Ok(None)
    }

    /// 轮次完成后观察，更新路由状态
    ///
    /// 默认 no-op。
    async fn on_turn_complete(&self, _messages: &[ChatMessage]) -> anyhow::Result<()> {
        Ok(())
    }

    /// API 调用前的廉价粗略检查（基于估算 token）
    ///
    /// 默认委托给 `should_compress`。
    fn should_compress_preflight(&self, messages: &[ChatMessage]) -> CompressPreflight {
        let should = self.should_compress(messages);
        CompressPreflight {
            should_compress: should,
            reason: if should {
                "token threshold exceeded".to_string()
            } else {
                "within budget".to_string()
            },
        }
    }

    /// 仅修剪工具结果（低成本，不调 LLM）
    ///
    /// 在不触发完整压缩的情况下独立修剪工具输出，降低 token 占用。
    /// 默认返回原消息。
    fn prune_tool_results_only(&self, messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
        messages
    }

    /// 获取引擎运行时状态
    ///
    /// 用于 UI 展示当前上下文使用率。
    /// 接收 `messages` 以便在消息数变化时回退到 token 估算值，
    /// 避免使用过期的 `last_prompt_tokens`。
    fn get_status(&self, _messages: &[ChatMessage]) -> EngineStatus {
        EngineStatus::default()
    }

    /// 模型切换时重算阈值（支持 per-model 阈值覆盖）
    ///
    /// 默认 no-op。
    fn update_model(&self, _model: &str, _context_length: Option<usize>) {}

    /// 会话开始
    ///
    /// 默认 no-op。
    async fn on_session_start(&self, _session_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// 会话重置（/reset）
    ///
    /// 默认调用 `reset`。
    async fn on_session_reset(&self) -> anyhow::Result<()> {
        self.reset();
        Ok(())
    }
}