use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::Result;
use crate::storage::MemoryScope;

/// 能力类型：统一抽象 Tool / Agent / Resource
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityKind {
    /// 无状态函数：同步/异步，一次性返回
    Tool,
    /// 有状态实体：长运行、流式、可持久化
    Agent,
    /// 数据源：只读、可订阅
    Resource,
}

/// 能力来源
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CapabilitySource {
    /// 内置 Rust 实现
    Native { module: String },
    /// MCP 协议接入的工具
    Mcp {
        server_url: String,
        tool_name: String,
    },
    /// A2A 协议接入的 Agent
    A2a {
        agent_url: String,
        agent_card: Value,
    },
}

/// 能力规格描述
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitySpec {
    pub id: Uuid,
    pub name: String,
    pub kind: CapabilityKind,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,

    /// Agent 特有：是否流式输出
    #[serde(default)]
    pub streaming: bool,
    /// Agent 特有：是否长任务（返回 handle，轮询状态）
    #[serde(default)]
    pub long_running: bool,
    /// Agent 特有：是否有状态
    #[serde(default)]
    pub stateful: bool,
    /// 最大并发实例数
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    /// 超时时间（秒），None 表示不限
    #[serde(default)]
    pub timeout: Option<u64>,

    /// 来源
    pub source: CapabilitySource,

    /// 需要的权限
    #[serde(default)]
    pub required_permissions: Vec<String>,

    /// 标签（用于工具集分组）
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_max_concurrency() -> usize {
    1
}

/// 执行结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CapabilityExecution {
    /// 同步返回结果
    Sync(Value),
    /// 长任务，返回 handle
    Async { task_id: String },
}

/// 流式输出块
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CapabilityChunk {
    /// 数据块
    Data(Value),
    /// 完成
    Done,
    /// 错误
    Error(String),
}

impl Default for CapabilitySpec {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: String::new(),
            kind: CapabilityKind::Tool,
            description: String::new(),
            input_schema: Value::Object(serde_json::Map::new()),
            output_schema: Value::Object(serde_json::Map::new()),
            streaming: false,
            long_running: false,
            stateful: false,
            max_concurrency: 1,
            timeout: None,
            source: CapabilitySource::Native {
                module: String::new(),
            },
            required_permissions: vec![],
            tags: vec![],
        }
    }
}

/// 能力注册表 trait
#[async_trait]
pub trait CapabilityRegistry: Send + Sync {
    async fn register(&self, spec: CapabilitySpec) -> Result<Uuid>;
    async fn unregister(&self, id: &Uuid) -> Result<()>;
    async fn get(&self, id: &Uuid) -> Result<CapabilitySpec>;
    async fn get_by_name(&self, name: &str) -> Result<CapabilitySpec>;
    async fn list(&self) -> Result<Vec<CapabilitySpec>>;
    async fn list_by_kind(&self, kind: CapabilityKind) -> Result<Vec<CapabilitySpec>>;
    async fn list_by_tags(&self, tags: &[String]) -> Result<Vec<CapabilitySpec>>;
}

/// 工具执行上下文：携带请求级的作用域和会话信息
///
/// 在工具调用链路中从 AgentLoop 一路传递到 ToolSet::execute，
/// 使工具能感知当前用户/租户和会话，实现数据隔离。
#[derive(Debug, Clone, Default)]
pub struct ToolContext {
    /// 记忆作用域（tenant_id + user_id）
    pub scope: MemoryScope,
    /// 会话 ID（用于 memory 目标的会话级隔离）
    pub session_id: String,
}

impl ToolContext {
    pub fn new(scope: MemoryScope, session_id: impl Into<String>) -> Self {
        Self {
            scope,
            session_id: session_id.into(),
        }
    }
}

/// 能力执行器：根据 CapabilitySpec 调度到对应 Adapter 执行
#[async_trait]
pub trait CapabilityExecutor: Send + Sync {
    async fn execute(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<CapabilityExecution>;
    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>>;
}