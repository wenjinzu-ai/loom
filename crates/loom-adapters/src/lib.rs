//! Loom Adapters — 协议适配层
//!
//! 把外部系统（Native / MCP / A2A）的能力适配为内部 Capability。
//! 核心思想：调用 Capability = 调用工具，不管背后是 Tool 还是 Agent。

pub mod a2a;
pub mod mcp;
pub mod native;

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{
    CapabilityExecution, CapabilityExecutor, CapabilitySource, CapabilitySpec, Result, ToolContext,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// 适配器类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdapterKind {
    Native,
    Mcp,
    A2a,
}

/// 适配器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterConfig {
    pub kind: AdapterKind,
    pub endpoint: String,
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub token: String,
}

/// 协议适配器：把外部系统的能力适配为内部 Capability
#[async_trait]
pub trait Adapter: Send + Sync {
    fn adapter_type(&self) -> &str;

    /// 发现并注册外部能力
    async fn discover(&self, config: &AdapterConfig) -> Result<Vec<CapabilitySpec>>;

    /// 执行一个 Capability（同步）
    async fn execute(&self, spec: &CapabilitySpec, args: Value, ctx: &ToolContext) -> Result<CapabilityExecution>;

    /// 流式执行
    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>>;
}

/// 适配器管理器：持有所有适配器，按 CapabilitySource 分发执行
///
/// 实现 `CapabilityExecutor`，供编排引擎调用。
pub struct AdapterManager {
    native: Arc<dyn Adapter>,
    mcp_clients: RwLock<HashMap<String, Arc<dyn Adapter>>>,
    a2a_clients: RwLock<HashMap<String, Arc<dyn Adapter>>>,
}

impl AdapterManager {
    pub fn new(native: Arc<dyn Adapter>) -> Self {
        Self {
            native,
            mcp_clients: RwLock::new(HashMap::new()),
            a2a_clients: RwLock::new(HashMap::new()),
        }
    }

    /// 注册一个 MCP 适配器（按 server_url 索引）
    pub fn register_mcp(&self, server_url: &str, adapter: Arc<dyn Adapter>) {
        self.mcp_clients
            .write()
            .insert(server_url.to_string(), adapter);
    }

    /// 注册一个 A2A 适配器（按 agent_url 索引）
    pub fn register_a2a(&self, agent_url: &str, adapter: Arc<dyn Adapter>) {
        self.a2a_clients
            .write()
            .insert(agent_url.to_string(), adapter);
    }

    fn select_adapter(&self, source: &CapabilitySource) -> Result<Arc<dyn Adapter>> {
        match source {
            CapabilitySource::Native { .. } => Ok(self.native.clone()),
            CapabilitySource::Mcp { server_url, .. } => self
                .mcp_clients
                .read()
                .get(server_url)
                .cloned()
                .ok_or_else(|| {
                    loom_core::LoomError::AdapterNotFound(format!("mcp:{}", server_url))
                }),
            CapabilitySource::A2a { agent_url, .. } => self
                .a2a_clients
                .read()
                .get(agent_url)
                .cloned()
                .ok_or_else(|| loom_core::LoomError::AdapterNotFound(format!("a2a:{}", agent_url))),
        }
    }
}

#[async_trait]
impl CapabilityExecutor for AdapterManager {
    async fn execute(&self, spec: &CapabilitySpec, args: Value, ctx: &ToolContext) -> Result<CapabilityExecution> {
        let adapter = self.select_adapter(&spec.source)?;
        adapter.execute(spec, args, ctx).await
    }

    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let adapter = self.select_adapter(&spec.source)?;
        adapter.execute_stream(spec, args, ctx).await
    }
}