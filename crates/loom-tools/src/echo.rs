//! Echo 工具集：回显输入，用于调试和测试链路

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};

pub struct EchoToolSet;

#[async_trait]
impl ToolSet for EchoToolSet {
    fn name(&self) -> &str {
        "echo"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "echo".into(),
            description: "回显输入（内置示例工具）".into(),
            input_schema: json!({"type": "object", "properties": {"message": {"type": "string"}}}),
            output_schema: json!({"type": "string"}),
            streaming: false,
            tags: vec!["core".into()],
        }]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "echo" => {
                let msg = args["message"].as_str().unwrap_or("").to_string();
                Ok(json!(msg))
            }
            _ => Err(loom_core::LoomError::CapabilityNotFound(tool_name.into())),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(stream::once(async move { Ok(v) })))
    }
}