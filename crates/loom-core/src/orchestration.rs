use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::Result;

/// 编排节点：统一表达工具调用和 Agent 调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    /// 调用一个 Capability（工具或 Agent）
    Call {
        capability: Uuid,
        args: Value,
        alias: Option<String>,
    },
    /// 顺序执行
    Sequence { nodes: Vec<Node> },
    /// 并行执行
    Parallel { nodes: Vec<Node> },
    /// 条件分支
    Branch {
        condition: String,
        branches: Vec<(String, Node)>,
        default: Option<Box<Node>>,
    },
    /// 循环
    Loop {
        body: Box<Node>,
        max_iterations: usize,
    },
    /// 错误处理
    TryCatch {
        try_node: Box<Node>,
        catch: Box<Node>,
    },
}

/// 工作流上下文：节点间数据传递
#[derive(Debug, Clone, Default)]
pub struct WorkflowContext {
    pub outputs: std::collections::HashMap<String, Value>,
    pub last_output: Option<Value>,
}

impl WorkflowContext {
    pub fn set(&mut self, alias: &str, value: Value) {
        self.outputs.insert(alias.to_string(), value.clone());
        self.last_output = Some(value);
    }

    pub fn get(&self, alias: &str) -> Option<&Value> {
        self.outputs.get(alias)
    }
}

/// 工作流执行结果
#[derive(Debug, Clone)]
pub struct WorkflowResult {
    pub output: Value,
    pub context: WorkflowContext,
}

/// 编排引擎 trait
#[async_trait::async_trait]
pub trait OrchestrationEngine: Send + Sync {
    async fn execute(&self, workflow: Node, ctx: WorkflowContext) -> Result<WorkflowResult>;
}
