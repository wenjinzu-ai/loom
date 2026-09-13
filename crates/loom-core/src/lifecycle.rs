use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent::AgentOutput;
use crate::error::Result;
use crate::isolation::{ActivitySummary, IsolationLevel};
use crate::message::AgentMessage;
use crate::storage::MemoryScope;

/// Agent 状态机
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentState {
    Created,
    Starting,
    Running,
    Paused,
    Stopping,
    Stopped,
    Failed,
    Destroyed,
}

impl AgentState {
    pub fn can_transition_to(&self, next: AgentState) -> bool {
        use AgentState::*;
        matches!(
            (self, next),
            (Created, Starting)
                | (Created, Running) // 即时启动的快速路径（无异步启动阶段）
                | (Starting, Running)
                | (Starting, Failed)
                | (Running, Paused)
                | (Running, Stopping)
                | (Running, Failed)
                | (Paused, Running)
                | (Paused, Stopping)
                | (Paused, Failed) // 暂停期间失败
                | (Stopping, Stopped)
                | (Stopping, Failed) // 停止过程中失败
                | (Stopped, Running) // 重启已停止的 Agent
                | (Stopped, Destroyed)
                | (Failed, Running) // 失败后重试
                | (Failed, Destroyed)
        )
    }

    /// 判断当前状态是否为终态（不可再转换到运行态）
    pub fn is_terminal(&self) -> bool {
        matches!(self, AgentState::Destroyed)
    }

    /// 判断当前状态是否处于运行中（Running 或 Starting）
    pub fn is_active(&self) -> bool {
        matches!(self, AgentState::Starting | AgentState::Running)
    }
}

impl std::fmt::Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Agent 句柄
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHandle {
    pub agent_id: Uuid,
    pub state: AgentState,
    pub isolation: IsolationLevel,
    pub parent_agent_id: Option<Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub goal: String,
}

/// Agent 启动请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLaunchRequest {
    pub capability_id: Option<Uuid>,
    pub goal: String,
    pub context: String,
    pub toolsets: Vec<String>,
    pub isolation: IsolationLevel,
    pub timeout: Option<Duration>,
    pub parent_agent_id: Option<Uuid>,
    pub config: serde_json::Value,
    /// 委派深度（顶层为 0，由父 Agent 传递）
    pub delegate_depth: u32,
    /// 记忆作用域（tenant_id + user_id），子 Agent 继承父 Agent 的租户/用户隔离
    pub scope: Option<MemoryScope>,
    /// 父 Agent 的工具集（用于子 Agent 工具集交集，空表示父 Agent 可使用全部工具）
    pub parent_toolsets: Vec<String>,
    /// 会话 ID（用于后台子 Agent 结果跨轮次回注）
    pub session_id: Option<String>,
}

impl Default for AgentLaunchRequest {
    fn default() -> Self {
        Self {
            capability_id: None,
            goal: String::new(),
            context: String::new(),
            toolsets: vec![],
            isolation: IsolationLevel::default(),
            timeout: None,
            parent_agent_id: None,
            config: serde_json::Value::Null,
            delegate_depth: 0,
            scope: None,
            parent_toolsets: vec![],
            session_id: None,
        }
    }
}

/// Agent 生命周期管理器 trait
#[async_trait]
pub trait AgentLifecycleManager: Send + Sync {
    async fn launch(&self, req: AgentLaunchRequest) -> Result<AgentHandle>;
    async fn stop(&self, id: &Uuid) -> Result<()>;
    async fn pause(&self, id: &Uuid) -> Result<()>;
    async fn resume(&self, id: &Uuid) -> Result<()>;
    async fn destroy(&self, id: &Uuid) -> Result<()>;
    async fn get_status(&self, id: &Uuid) -> Result<AgentHandle>;
    async fn list(&self) -> Result<Vec<AgentHandle>>;
    async fn send_message(&self, id: &Uuid, msg: AgentMessage) -> Result<()>;
    /// 采样 Agent 活动摘要（供父端心跳陈旧检测）
    async fn get_activity_summary(&self, id: &Uuid) -> Result<ActivitySummary>;
    /// 等待指定 Agent 完成，返回其最终输出
    async fn wait_for_result(&self, id: &Uuid) -> Result<AgentOutput>;
}