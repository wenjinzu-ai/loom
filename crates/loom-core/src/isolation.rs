use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent::AgentOutput;
use crate::error::Result;
use crate::message::AgentMessage;
use crate::storage::MemoryScope;

/// 隔离级别
/// 协程级默认，进程/容器/Wasm 按需升级
/// 注意：线程不是独立隔离级别，它是 Coroutine 级别的并发实现（spawn_blocking）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum IsolationLevel {
    /// 协程级：同进程 tokio task，零开销，可信 Agent
    #[default]
    Coroutine,
    /// 进程级：子进程 + IPC，内存隔离
    Process,
    /// 容器级：Docker/containerd，文件系统/网络隔离
    Container,
    /// WASM 沙箱：wasmtime，最强隔离，不可信代码
    Wasm,
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "coroutine" => Ok(IsolationLevel::Coroutine),
            "process" => Ok(IsolationLevel::Process),
            "container" => Ok(IsolationLevel::Container),
            "wasm" => Ok(IsolationLevel::Wasm),
            _ => Err(format!("unknown isolation level: {}", s)),
        }
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::Coroutine => write!(f, "coroutine"),
            IsolationLevel::Process => write!(f, "process"),
            IsolationLevel::Container => write!(f, "container"),
            IsolationLevel::Wasm => write!(f, "wasm"),
        }
    }
}

/// Agent 启动规格
#[derive(Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    pub agent_id: Uuid,
    pub capability_id: Option<Uuid>,
    pub goal: String,
    pub context: String,
    pub toolsets: Vec<String>,
    pub isolation: IsolationLevel,
    pub timeout: Option<Duration>,
    pub config: serde_json::Value,
    /// 父 Agent ID（None 表示顶层 Agent）
    pub parent_agent_id: Option<Uuid>,
    /// 委派深度（顶层为 0）
    pub delegate_depth: u32,
    /// 记忆作用域（tenant_id + user_id），子 Agent 继承父 Agent 的租户/用户隔离
    pub scope: Option<MemoryScope>,
    /// 父 Agent 的工具集（用于子 Agent 工具集交集，空表示父 Agent 可使用全部工具）
    pub parent_toolsets: Vec<String>,
    /// 会话 ID（用于后台子 Agent 结果跨轮次回注到父会话）
    /// 顶层 Agent 从 chat 请求继承，子 Agent 从父 Agent 继承。
    /// 为 None 时表示一次性/无状态运行，后台委派会回退为同步执行。
    pub session_id: Option<String>,
    /// 活动状态共享句柄（协程后端注入，跨进程后端为 None）
    /// 用于父端心跳陈旧检测：子 Agent 循环每轮更新此字段，
    /// 父端通过 runtime.activity_summary() 采样。
    #[serde(skip)]
    pub activity_state: Option<Arc<std::sync::Mutex<ActivitySummary>>>,
}

impl std::fmt::Debug for AgentSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentSpec")
            .field("agent_id", &self.agent_id)
            .field("capability_id", &self.capability_id)
            .field("goal", &self.goal)
            .field("context", &self.context)
            .field("toolsets", &self.toolsets)
            .field("isolation", &self.isolation)
            .field("timeout", &self.timeout)
            .field("config", &self.config)
            .field("parent_agent_id", &self.parent_agent_id)
            .field("delegate_depth", &self.delegate_depth)
            .field("scope", &self.scope)
            .field("parent_toolsets", &self.parent_toolsets)
            .field("session_id", &self.session_id)
            .field(
                "activity_state",
                &self.activity_state.as_ref().map(|_| "ActivitySummary(..)"),
            )
            .finish()
    }
}

/// 健康状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub alive: bool,
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    pub diagnostic: Option<String>,
}

/// Agent 活动摘要（供父端心跳陈旧检测使用）
///
/// 父 Agent 通过定期采样这些字段判断子 Agent 是"还在跑长任务"还是"已卡死"。
/// 判定规则：
/// - iterations / current_tool / last_activity_ts 任一推进 → 视为有进展，重置陈旧计数
/// - 否则陈旧计数 +1；达到阈值（空闲态短、工具中长）则判定卡死
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivitySummary {
    /// 已完成的迭代轮数（每轮一次 LLM 调用）
    pub iterations: u64,
    /// 已完成的 LLM API 调用次数
    pub api_call_count: u64,
    /// 当前正在执行的工具名（None 表示不在执行工具，处于空闲/等待 LLM 状态）
    pub current_tool: Option<String>,
    /// 最后一次活动时间戳（迭代推进或工具切换时刷新）
    pub last_activity_ts: Option<chrono::DateTime<chrono::Utc>>,
}

/// Agent 运行时句柄
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    async fn send(&self, msg: AgentMessage) -> Result<()>;
    async fn send_stream(
        &self,
        msg: AgentMessage,
    ) -> Result<BoxStream<'static, Result<serde_json::Value>>>;
    async fn stop(&self) -> Result<()>;
    async fn health(&self) -> HealthStatus;
    /// 采样 Agent 的活动摘要，供父端心跳陈旧检测使用
    async fn activity_summary(&self) -> Result<ActivitySummary>;
    /// 等待 Agent 完成，返回其结构化输出
    async fn wait(&self) -> Result<AgentOutput>;
}

/// 隔离后端
#[async_trait]
pub trait IsolationBackend: Send + Sync {
    fn level(&self) -> IsolationLevel;
    async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>>;
}

/// 根据 Agent 可信度自动选择隔离级别
pub fn select_isolation(
    requires_fs: bool,
    requires_network: bool,
    untrusted: bool,
) -> IsolationLevel {
    if untrusted {
        IsolationLevel::Wasm
    } else if requires_network {
        IsolationLevel::Container
    } else if requires_fs {
        IsolationLevel::Process
    } else {
        IsolationLevel::Coroutine
    }
}