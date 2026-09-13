//! A2A 协议适配器
//!
//! 把外部 A2A Agent 适配为内部 Capability。
//! 调用 A2A Agent 和调用普通工具方式完全一致。
//! A2A Agent 被注册为 CapabilityKind::Agent 类型。

use async_trait::async_trait;
use loom_core::{CapabilityExecution, CapabilityKind, CapabilitySource, CapabilitySpec, Result, ToolContext};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{Adapter, AdapterConfig};

/// A2A 客户端（简化实现）
pub struct A2aClient {
    endpoint: String,
}

impl A2aClient {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
        }
    }

    /// 获取 Agent Card（GET /.well-known/agent.json）
    async fn get_agent_card(&self) -> Result<Value> {
        tracing::debug!("fetching agent card from {}", self.endpoint);
        Ok(json!({
            "name": "remote-agent",
            "description": "A remote A2A agent",
            "inputSchema": {"type": "object"},
            "outputSchema": {"type": "object"}
        }))
    }

    /// 创建任务（POST /tasks）
    async fn create_task(&self, input: Value) -> Result<Value> {
        tracing::debug!("creating A2A task with input {:?}", input);
        Ok(json!({"taskId": "mock-task-id", "status": "working"}))
    }

    /// 流式获取任务结果（GET /tasks/{id}/stream，SSE）
    async fn stream_task(
        &self,
        _task_id: &str,
    ) -> Result<futures::stream::BoxStream<'static, Result<Value>>> {
        let stream = futures::stream::once(async move {
            Ok(json!({"status": "completed", "result": {"mock": true}}))
        });
        Ok(Box::pin(stream))
    }

    /// 等待任务完成
    async fn wait_for_completion(&self, task_id: &str) -> Result<Value> {
        let mut stream = self.stream_task(task_id).await?;
        use futures::StreamExt;
        let mut last = None;
        while let Some(item) = stream.next().await {
            last = Some(item?);
        }
        last.ok_or_else(|| loom_core::LoomError::Other("no result from A2A agent".into()))
    }
}

pub struct A2aAdapter {
    clients: RwLock<HashMap<String, Arc<A2aClient>>>,
}

impl A2aAdapter {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for A2aAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Adapter for A2aAdapter {
    fn adapter_type(&self) -> &str {
        "a2a"
    }

    async fn discover(&self, config: &AdapterConfig) -> Result<Vec<CapabilitySpec>> {
        let client = A2aClient::new(&config.endpoint);
        let card = client.get_agent_card().await?;

        let client = Arc::new(client);
        self.clients
            .write()
            .insert(config.endpoint.clone(), client.clone());

        let name = card["name"].as_str().unwrap_or("remote-agent").to_string();
        let description = card["description"].as_str().unwrap_or("").to_string();
        let input_schema = card
            .get("inputSchema")
            .cloned()
            .unwrap_or(json!({"type": "object"}));
        let output_schema = card.get("outputSchema").cloned().unwrap_or(json!({}));

        let spec = CapabilitySpec {
            name: name.clone(),
            kind: CapabilityKind::Agent,
            description,
            input_schema,
            output_schema,
            streaming: true,
            long_running: true,
            stateful: true,
            source: CapabilitySource::A2a {
                agent_url: config.endpoint.clone(),
                agent_card: card,
            },
            tags: vec!["a2a".into(), "remote".into()],
            ..Default::default()
        };

        Ok(vec![spec])
    }

    async fn execute(&self, spec: &CapabilitySpec, args: Value, _ctx: &ToolContext) -> Result<CapabilityExecution> {
        if let CapabilitySource::A2a { agent_url, .. } = &spec.source {
            let client = {
                let clients = self.clients.read();
                clients
                    .get(agent_url)
                    .cloned()
                    .ok_or_else(|| loom_core::LoomError::Other("A2A agent not connected".into()))?
            };
            let task = client.create_task(args).await?;
            let task_id = task["taskId"].as_str().unwrap_or("");
            let result = client.wait_for_completion(task_id).await?;
            Ok(CapabilityExecution::Sync(result))
        } else {
            Err(loom_core::LoomError::CapabilityNotFound(spec.name.clone()))
        }
    }

    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        _ctx: &ToolContext,
    ) -> Result<futures::stream::BoxStream<'static, Result<Value>>> {
        if let CapabilitySource::A2a { agent_url, .. } = &spec.source {
            let client = {
                let clients = self.clients.read();
                clients
                    .get(agent_url)
                    .cloned()
                    .ok_or_else(|| loom_core::LoomError::Other("A2A agent not connected".into()))?
            };
            let task = client.create_task(args).await?;
            let task_id = task["taskId"].as_str().unwrap_or("").to_string();
            client.stream_task(&task_id).await
        } else {
            Err(loom_core::LoomError::CapabilityNotFound(spec.name.clone()))
        }
    }
}