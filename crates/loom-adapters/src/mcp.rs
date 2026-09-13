//! MCP 协议适配器
//!
//! 把外部 MCP Server 的工具适配为内部 Capability。
//! MCP 工具被当作普通 Tool 调用。

use async_trait::async_trait;
use loom_core::{CapabilityExecution, CapabilityKind, CapabilitySource, CapabilitySpec, Result, ToolContext};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{Adapter, AdapterConfig};

/// MCP 客户端（简化实现，实际应使用 MCP SDK）
pub struct McpClient {
    endpoint: String,
}

impl McpClient {
    async fn connect(endpoint: &str, _auth: Option<crate::AuthConfig>) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.to_string(),
        })
    }

    async fn list_tools(&self) -> Result<Vec<McpTool>> {
        tracing::debug!("listing MCP tools from {}", self.endpoint);
        Ok(vec![])
    }

    async fn call_tool(&self, name: &str, args: Value) -> Result<Value> {
        tracing::debug!("calling MCP tool {} with args {:?}", name, args);
        Ok(json!({"tool": name, "args": args, "mock": true}))
    }
}

#[derive(Debug, Clone)]
struct McpTool {
    name: String,
    description: String,
    input_schema: Value,
}

pub struct McpAdapter {
    clients: RwLock<HashMap<String, Arc<McpClient>>>,
}

impl McpAdapter {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for McpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Adapter for McpAdapter {
    fn adapter_type(&self) -> &str {
        "mcp"
    }

    async fn discover(&self, config: &AdapterConfig) -> Result<Vec<CapabilitySpec>> {
        let client = McpClient::connect(&config.endpoint, config.auth.clone()).await?;
        let tools = client.list_tools().await?;

        let client = Arc::new(client);
        self.clients
            .write()
            .insert(config.endpoint.clone(), client.clone());

        let specs = tools
            .into_iter()
            .map(|tool| CapabilitySpec {
                name: tool.name.clone(),
                kind: CapabilityKind::Tool,
                description: tool.description,
                input_schema: tool.input_schema,
                output_schema: json!({}),
                source: CapabilitySource::Mcp {
                    server_url: config.endpoint.clone(),
                    tool_name: tool.name,
                },
                tags: vec!["mcp".into()],
                ..Default::default()
            })
            .collect();

        Ok(specs)
    }

    async fn execute(&self, spec: &CapabilitySpec, args: Value, _ctx: &ToolContext) -> Result<CapabilityExecution> {
        if let CapabilitySource::Mcp {
            server_url,
            tool_name,
        } = &spec.source
        {
            let client = {
                let clients = self.clients.read();
                clients
                    .get(server_url)
                    .cloned()
                    .ok_or_else(|| loom_core::LoomError::Other("MCP server not connected".into()))?
            };
            let result = client.call_tool(tool_name, args).await?;
            Ok(CapabilityExecution::Sync(result))
        } else {
            Err(loom_core::LoomError::CapabilityNotFound(spec.name.clone()))
        }
    }

    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<futures::stream::BoxStream<'static, Result<Value>>> {
        let result = self.execute(spec, args, ctx).await?;
        if let CapabilityExecution::Sync(v) = result {
            Ok(Box::pin(futures::stream::once(async move { Ok(v) })))
        } else {
            Err(loom_core::LoomError::Other(
                "async handle not supported".into(),
            ))
        }
    }
}