use crate::context::CompressionStateSnapshot;
use loom_llm::ChatMessage;
use std::sync::Arc;
use uuid::Uuid;

/// Agent 运行结果
#[derive(Debug)]
pub struct AgentRunResult {
    pub final_response: String,
    pub iterations: usize,
    pub tool_calls_made: usize,
    pub tool_names: Vec<String>,
    /// 执行时长（毫秒）
    pub duration_ms: u64,
    /// 工具轨迹（序列化的 ToolTrace）
    pub tool_trace: Option<serde_json::Value>,
}

/// 带完整对话历史的运行结果（用于多轮会话保存）
#[derive(Debug)]
pub struct AgentRunResultWithHistory {
    pub final_response: String,
    pub iterations: usize,
    pub tool_calls_made: usize,
    pub tool_names: Vec<String>,
    pub duration_ms: u64,
    pub tool_trace: Option<serde_json::Value>,
    /// 本轮结束后的完整消息历史（含 system prompt）
    pub history: Vec<ChatMessage>,
}

/// Agent 运行结果（含 HITL 中断状态）
#[derive(Debug)]
pub enum AgentRunOutcome {
    /// 正常完成
    Finished(AgentRunResultWithHistory),
    /// 中断等待人工输入
    Interrupt {
        /// 暴露给用户的值
        value: serde_json::Value,
        /// 可恢复的检查点 ID
        checkpoint_id: String,
        /// 线程 ID
        thread_id: String,
        /// 中断时的消息历史
        history: Vec<ChatMessage>,
    },
}

/// Agent 执行状态快照（持久化到 checkpoint 的 channel_values）
///
/// 统一管理需要保存的状态字段，新增状态只需扩展此结构体，
/// 避免散落的 json! 构造，提升开闭性。
///
/// 包含完整的运行时上下文，支持从任意 checkpoint 断点续执行：
/// - messages / iterations / tool_calls_made / tool_names：对话与计数状态
/// - toolsets / parent_toolsets / delegate_depth：工具可见性与委派深度
/// - scope_json：租户/用户作用域（用于多租户隔离和记忆加载）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AgentState {
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) iterations: usize,
    pub(crate) tool_calls_made: usize,
    pub(crate) tool_names: Vec<String>,
    /// 工具集白名单（空表示全部可用）
    #[serde(default)]
    pub(crate) toolsets: Vec<String>,
    /// 父 Agent 的工具集
    #[serde(default)]
    pub(crate) parent_toolsets: Vec<String>,
    /// 委派深度（0 表示顶层 Agent）
    #[serde(default)]
    pub(crate) delegate_depth: u32,
    /// 父 Agent ID（用于子 Agent 工具执行时的委派上下文）
    #[serde(default)]
    pub(crate) parent_agent_id: Option<Uuid>,
    /// 租户/用户作用域 JSON（MemoryScope 的序列化形式）
    #[serde(default)]
    pub(crate) scope_json: Option<String>,
    /// 最后一条用户消息内容（用于恢复时判断是否需要注入 continuation）
    #[serde(default)]
    pub(crate) last_user_message: Option<String>,
    /// 压缩状态快照（previous_summary / 失败计数 / terminal_failure）
    /// resume 后恢复，保证迭代式摘要链不断裂
    #[serde(default)]
    pub(crate) compression_state: Option<CompressionStateSnapshot>,
}

impl AgentState {
    /// 从运行时上下文构造持久化状态
    ///
    /// 将 LoopContext 中的可变状态与作用域/压缩状态聚合为快照。
    pub(crate) fn from_runtime(
        ctx: &LoopContext,
        scope: &loom_core::MemoryScope,
        compression_state: Option<CompressionStateSnapshot>,
    ) -> Self {
        Self {
            messages: ctx.messages.clone(),
            iterations: ctx.iterations,
            tool_calls_made: ctx.tool_calls_made,
            tool_names: ctx.tool_names.clone(),
            toolsets: ctx.toolsets.clone(),
            parent_toolsets: ctx.parent_toolsets.clone(),
            delegate_depth: ctx.delegate_depth,
            parent_agent_id: ctx.parent_agent_id,
            scope_json: Some(serde_json::to_string(scope).unwrap_or_default()),
            last_user_message: ctx.messages.iter().rev().find_map(|m| {
                if m.role == loom_llm::Role::User {
                    m.content.clone()
                } else {
                    None
                }
            }),
            compression_state,
        }
    }
}

/// Agent 循环执行上下文
///
/// 将 run_goal_with_event 与 continue_loop 共享的运行时参数聚合，
/// 减少方法签名长度（参数对象模式）。
pub(crate) struct LoopContext {
    pub messages: Vec<ChatMessage>,
    pub iterations: usize,
    pub tool_calls_made: usize,
    pub tool_names: Vec<String>,
    pub thread_id: String,
    pub last_checkpoint_id: Option<String>,
    pub agent_id: Uuid,
    pub parent_agent_id: Option<Uuid>,
    pub delegate_depth: u32,
    /// 工具集白名单（空表示全部可用）
    pub toolsets: Vec<String>,
    /// 父 Agent 的工具集（用于子 Agent 工具集交集）
    pub parent_toolsets: Vec<String>,
    /// 活动状态共享句柄（由隔离后端注入，None 表示非子 Agent 上下文）
    /// 每轮迭代和工具执行时更新，供父端心跳陈旧检测采样
    pub activity_state: Option<Arc<std::sync::Mutex<loom_core::ActivitySummary>>>,
}