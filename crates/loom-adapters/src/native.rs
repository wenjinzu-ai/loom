//! 内置能力适配器
//!
//! 职责：把 [`loom_tools`] 中注册的工具集转成 `CapabilitySpec` 暴露出去，
//! 执行时按工具名路由到对应 ToolSet。
//!
//! 注意：`spawn_agent`（系统级委派能力）不在此处理，由 loom-agent 的 AgentLoop
//! 直接调用 lifecycle 实现，避免循环依赖。

use async_trait::async_trait;
use loom_core::{CapabilityExecution, CapabilityKind, CapabilitySource, CapabilitySpec, Result, ToolContext};
use loom_tools::{builtins, ToolRegistry};
use serde_json::Value;

use crate::{Adapter, AdapterConfig};

pub struct NativeAdapter {
    tools: ToolRegistry,
}

impl NativeAdapter {
    pub fn new() -> Self {
        Self { tools: builtins() }
    }

    /// 注入自定义工具注册表（测试或按需加载时用）
    pub fn with_tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
        self
    }
}

impl Default for NativeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Adapter for NativeAdapter {
    fn adapter_type(&self) -> &str {
        "native"
    }

    async fn discover(&self, _config: &AdapterConfig) -> Result<Vec<CapabilitySpec>> {
        let mut specs = vec![];

        // 从 loom-tools 注册表发现所有工具，转成 CapabilitySpec
        for tool in self.tools.discover() {
            specs.push(CapabilitySpec {
                name: tool.name.clone(),
                kind: CapabilityKind::Tool,
                description: tool.description,
                input_schema: tool.input_schema,
                output_schema: tool.output_schema,
                streaming: tool.streaming,
                source: CapabilitySource::Native { module: tool.name },
                tags: tool.tags,
                ..Default::default()
            });
        }

        Ok(specs)
    }

    async fn execute(&self, spec: &CapabilitySpec, args: Value, ctx: &ToolContext) -> Result<CapabilityExecution> {
        let tool_name = &spec.name;
        let set = self
            .tools
            .find_set(tool_name)
            .ok_or_else(|| loom_core::LoomError::CapabilityNotFound(tool_name.clone()))?;
        let result = set.execute(tool_name, args, ctx).await?;
        Ok(CapabilityExecution::Sync(result))
    }

    async fn execute_stream(
        &self,
        spec: &CapabilitySpec,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<futures::stream::BoxStream<'static, Result<Value>>> {
        let tool_name = &spec.name;
        let set = self
            .tools
            .find_set(tool_name)
            .ok_or_else(|| loom_core::LoomError::CapabilityNotFound(tool_name.clone()))?;
        set.execute_stream(tool_name, args, ctx).await
    }
}