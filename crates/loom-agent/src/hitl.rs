//! Human-in-the-Loop 机制
//!
//! - Agent 执行中调用 `interrupt(value)` 暂停，保存 checkpoint
//! - 返回 `AgentEvent::Interrupt`，携带暴露给用户的值
//! - 用户提供输入后，调用 `resume()` 从 checkpoint 恢复执行
//! - resume 值通过 pending_writes 传递，成为 interrupt 的返回值

use serde::{Deserialize, Serialize};
use serde_json::Value;

use loom_core::{CheckpointConfig, CheckpointSaver, Result};

/// Agent 执行事件
///
/// `Command` 和 interrupt 机制：
/// - `Finished`: 正常完成，返回最终响应
/// - `Interrupt`: 执行暂停，等待用户输入后可恢复
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentEvent {
    /// 正常完成
    Finished {
        response: String,
        iterations: usize,
        tool_calls_made: usize,
        tool_names: Vec<String>,
        /// 执行时长（毫秒）
        duration_ms: u64,
        /// 工具轨迹（序列化的 ToolTrace）
        tool_trace: Option<serde_json::Value>,
    },
    /// 中断等待人工输入
    Interrupt {
        /// 暴露给用户的值（如待审批的内容、询问的问题）
        value: Value,
        /// 可恢复的检查点 ID
        checkpoint_id: String,
        /// 线程 ID
        thread_id: String,
    },
}

/// 恢复执行的命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeCommand {
    /// 线程 ID
    pub thread_id: String,
    /// 检查点 ID（从哪个检查点恢复）
    pub checkpoint_id: String,
    /// 用户提供的恢复值（成为 interrupt 的返回值）
    pub resume_value: Value,
}

impl ResumeCommand {
    pub fn new(
        thread_id: impl Into<String>,
        checkpoint_id: impl Into<String>,
        resume_value: Value,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpoint_id: checkpoint_id.into(),
            resume_value,
        }
    }
}

/// 从检查点元组中提取并清除恢复值（一次性消费）
///
/// 在恢复执行时，检查点 saver 中可能存储了 pending resume 值。
/// 此函数读取后立即清除该值，确保不会被重复消费。
///
/// `config` 需携带 `tenant_id` / `user_id` 以实现多租户隔离。
pub async fn consume_resume_value(
    saver: &dyn CheckpointSaver,
    config: &CheckpointConfig,
) -> Result<Option<Value>> {
    let tuple = saver.get_tuple(config).await?;
    let value = tuple.and_then(|t| t.pending_resume);
    if value.is_some() {
        saver.clear_resume(config).await?;
    }
    Ok(value)
}