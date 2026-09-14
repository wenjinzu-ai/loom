use futures::future::join_all;
use loom_core::{CapabilityExecutor, CapabilityRegistry, CapabilitySpec, ToolContext};
use loom_llm::{ToolCall, ToolDefinition};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::delegation::DelegationManager;

/// 将 CapabilitySpec 转换为 LLM 工具定义
pub fn capability_to_tool_def(spec: &CapabilitySpec) -> ToolDefinition {
    ToolDefinition {
        name: spec.name.clone(),
        description: spec.description.clone(),
        parameters: spec.input_schema.clone(),
    }
}

/// 工具调用结果
pub struct ToolCallResult {
    pub tool_call_id: String,
    pub name: String,
    pub content: String,
    pub is_error: bool,
}

/// 执行单个工具调用（用于并发执行）
pub async fn execute_tool_call(
    tool_call: &ToolCall,
    registry: &dyn CapabilityRegistry,
    executor: &dyn CapabilityExecutor,
    ctx: &ToolContext,
) -> ToolCallResult {
    tracing::debug!(
        "[tool_dispatch] executing tool: name={}, args={}",
        tool_call.name,
        tool_call.arguments
    );
    let start = std::time::Instant::now();

    match registry.get_by_name(&tool_call.name).await {
        Ok(spec) => {
            let args = if tool_call.arguments.is_null() {
                Value::Object(serde_json::Map::new())
            } else {
                tool_call.arguments.clone()
            };
            match executor.execute(&spec, args, ctx).await {
                Ok(exec) => {
                    let content = match exec {
                        loom_core::CapabilityExecution::Sync(v) => v.to_string(),
                        loom_core::CapabilityExecution::Async { task_id } => {
                            format!(r#"{{"task_id":"{task_id}"}}"#)
                        }
                    };
                    tracing::debug!(
                        "[tool_dispatch] tool result: name={}, content_len={}, elapsed_ms={}, content={}",
                        tool_call.name,
                        content.len(),
                        start.elapsed().as_millis(),
                        content
                    );
                    ToolCallResult {
                        tool_call_id: tool_call.id.clone(),
                        name: tool_call.name.clone(),
                        content,
                        is_error: false,
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "[tool_dispatch] tool error: name={}, error={}",
                        tool_call.name,
                        e
                    );
                    ToolCallResult {
                        tool_call_id: tool_call.id.clone(),
                        name: tool_call.name.clone(),
                        content: format!("Error executing tool: {e}"),
                        is_error: true,
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(
                "[tool_dispatch] tool not found: name={}, error={}",
                tool_call.name,
                e
            );
            ToolCallResult {
                tool_call_id: tool_call.id.clone(),
                name: tool_call.name.clone(),
                content: format!("Tool not found: {e}"),
                is_error: true,
            }
        }
    }
}

/// 分发工具调用：spawn_agent 走 lifecycle，其他走 CapabilityExecutor
/// 所有工具调用并发执行（concurrent tool execution）
///
/// `parent_toolsets`：当前 Agent 的工具集，作为子 Agent 的 `parent_toolsets` 传递，
/// 确保子 Agent 只能使用父 Agent 也拥有的工具集。
pub async fn dispatch_tool_calls(
    tool_calls: &[ToolCall],
    agent_id: Uuid,
    parent_agent_id: Option<Uuid>,
    delegate_depth: u32,
    parent_toolsets: &[String],
    ctx: &ToolContext,
    delegation: &DelegationManager,
    registry: &dyn CapabilityRegistry,
    executor: &dyn CapabilityExecutor,
    tool_semaphore: &Arc<Semaphore>,
    cancel_token: Option<tokio_util::sync::CancellationToken>,
) -> Vec<ToolCallResult> {
    let futures: Vec<_> = tool_calls
        .iter()
        .map(|tc| {
            let tc = tc.clone();
            let tool_semaphore = tool_semaphore.clone();
            let ctx = ctx.clone();
            let parent_toolsets = parent_toolsets.to_vec();
            let cancel_token = cancel_token.clone();
            async move {
                // 限制单轮并发工具数，防止 LLM 一次返回大量工具调用时耗尽连接池/资源
                let _permit = tool_semaphore.acquire_owned().await.ok();
                if tc.name == "spawn_agent" {
                    tracing::debug!("[tool_dispatch] executing tool: name={}, args={}", tc.name, tc.arguments);
                    match delegation
                        .execute_spawn_agent(
                            &tc.arguments,
                            agent_id,
                            parent_agent_id,
                            delegate_depth,
                            parent_toolsets,
                            Some(ctx.scope.clone()),
                            Some(ctx.session_id.clone()),
                            cancel_token.clone(),
                        )
                        .await
                    {
                        Ok(v) => {
                            tracing::debug!("[tool_dispatch] tool result: name={}, content={}", tc.name, v);
                            ToolCallResult {
                                tool_call_id: tc.id.clone(),
                                name: tc.name.clone(),
                                content: v.to_string(),
                                is_error: false,
                            }
                        }
                        Err(e) => {
                            tracing::debug!("[tool_dispatch] tool error: name={}, error={}", tc.name, e);
                            ToolCallResult {
                                tool_call_id: tc.id.clone(),
                                name: tc.name.clone(),
                                content: format!("spawn_agent error: {e}"),
                                is_error: true,
                            }
                        }
                    }
                } else {
                    execute_tool_call(&tc, registry, executor, &ctx).await
                }
            }
        })
        .collect();

    join_all(futures).await
}