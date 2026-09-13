//! DAG 编排引擎
//!
//! 执行 Node 工作流，支持顺序、并行、条件、循环、错误处理。
//! 不区分工具调用和 Agent 调用，统一通过 Capability Registry + Executor 执行。

use async_trait::async_trait;
use loom_core::{
    CapabilityExecutor, CapabilityRegistry, Node, OrchestrationEngine, WorkflowContext,
    WorkflowResult,
};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub struct DagOrchestrator {
    registry: Arc<dyn CapabilityRegistry>,
    executor: Arc<dyn CapabilityExecutor>,
}

impl DagOrchestrator {
    pub fn new(
        registry: Arc<dyn CapabilityRegistry>,
        executor: Arc<dyn CapabilityExecutor>,
    ) -> Self {
        Self { registry, executor }
    }

    fn execute_node<'a>(
        &'a self,
        node: Node,
        ctx: &'a mut WorkflowContext,
    ) -> Pin<Box<dyn Future<Output = loom_core::Result<Value>> + Send + 'a>> {
        Box::pin(async move {
            match node {
                Node::Call {
                    capability,
                    args,
                    alias,
                } => {
                    let spec = self.registry.get(&capability).await?;
                    tracing::debug!(
                        "[orchestration] call capability: name={}, alias={:?}",
                        spec.name,
                        alias
                    );

                    let tool_ctx = loom_core::ToolContext::default();
                    let execution = self.executor.execute(&spec, args, &tool_ctx).await?;
                    let result = match execution {
                        loom_core::CapabilityExecution::Sync(v) => v,
                        loom_core::CapabilityExecution::Async { task_id } => {
                            serde_json::json!({ "task_id": task_id })
                        }
                    };
                    tracing::debug!(
                        "[orchestration] call result: name={}, result_len={}",
                        spec.name,
                        result.to_string().len()
                    );

                    if let Some(alias) = alias {
                        ctx.set(&alias, result.clone());
                    } else {
                        ctx.last_output = Some(result.clone());
                    }
                    Ok(result)
                }
                Node::Sequence { nodes } => {
                    tracing::debug!("[orchestration] sequence: nodes={}", nodes.len());
                    let mut last = Value::Null;
                    for node in nodes {
                        last = self.execute_node(node, ctx).await?;
                    }
                    Ok(last)
                }
                Node::Parallel { nodes } => {
                    tracing::debug!("[orchestration] parallel: nodes={}", nodes.len());
                    let futures: Vec<_> = nodes
                        .into_iter()
                        .map(|node| {
                            let registry = self.registry.clone();
                            let executor = self.executor.clone();
                            async move {
                                let orch = DagOrchestrator { registry, executor };
                                let mut ctx = WorkflowContext::default();
                                orch.execute_node(node, &mut ctx).await
                            }
                        })
                        .collect();

                    let results = futures::future::try_join_all(futures).await?;
                    Ok(Value::Array(results))
                }
                Node::Branch {
                    condition,
                    branches,
                    default,
                } => {
                    let cond_val = evaluate_condition(&condition, ctx);
                    for (cond, branch) in branches {
                        if cond_val == cond {
                            return self.execute_node(branch, ctx).await;
                        }
                    }
                    if let Some(default) = default {
                        self.execute_node(*default, ctx).await
                    } else {
                        Ok(Value::Null)
                    }
                }
                Node::Loop {
                    body,
                    max_iterations,
                } => {
                    let body = *body;
                    let mut last = Value::Null;
                    for _ in 0..max_iterations {
                        last = self.execute_node(body.clone(), ctx).await?;
                    }
                    Ok(last)
                }
                Node::TryCatch { try_node, catch } => {
                    match self.execute_node(*try_node, ctx).await {
                        Ok(v) => Ok(v),
                        Err(_) => self.execute_node(*catch, ctx).await,
                    }
                }
            }
        })
    }
}

/// 简易条件求值：支持 `$alias` 取值、`==` 比较、字面量
fn evaluate_condition(condition: &str, ctx: &WorkflowContext) -> String {
    let cond = condition.trim();
    if let Some((left, right)) = cond.split_once("==") {
        let left = resolve(left.trim(), ctx);
        let right = resolve(right.trim(), ctx);
        return (left == right).to_string();
    }
    resolve(cond, ctx)
}

fn resolve(term: &str, ctx: &WorkflowContext) -> String {
    let term = term.trim();
    if let Some(alias) = term.strip_prefix('$') {
        ctx.get(alias)
            .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
            .unwrap_or_else(|| term.to_string())
    } else {
        term.trim_matches('"').to_string()
    }
}

#[async_trait]
impl OrchestrationEngine for DagOrchestrator {
    async fn execute(
        &self,
        workflow: Node,
        mut ctx: WorkflowContext,
    ) -> loom_core::Result<WorkflowResult> {
        let output = self.execute_node(workflow, &mut ctx).await?;
        Ok(WorkflowResult {
            output,
            context: ctx,
        })
    }
}