use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::isolation::AgentSpec;
use crate::Result;

/// Agent 执行错误分类
///
/// 区分不同类型的失败，便于上层（如委派管理器、陈旧检测）做出差异化决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentErrorKind {
    /// 无错误（成功）
    #[default]
    None,
    /// 工具执行错误
    ToolError,
    /// 模型 API 错误
    ModelError,
    /// 执行超时
    Timeout,
    /// 委派相关错误
    DelegationError,
    /// 检查点/恢复错误
    CheckpointError,
    /// 资源不足（token、并发等）
    ResourceExhausted,
    /// 被父中断取消
    Cancelled,
    /// 未知错误
    Unknown,
}

impl AgentErrorKind {
    /// 从错误消息文本推断错误分类
    pub fn from_error_message(err: &str) -> Self {
        let lower = err.to_lowercase();
        if lower.contains("timeout") || lower.contains("timed out") {
            Self::Timeout
        } else if lower.contains("cancel") {
            Self::Cancelled
        } else if lower.contains("tool") {
            Self::ToolError
        } else if lower.contains("api") || lower.contains("llm") || lower.contains("model") {
            Self::ModelError
        } else if lower.contains("delegat") || lower.contains("spawn") {
            Self::DelegationError
        } else if lower.contains("checkpoint") || lower.contains("resume") {
            Self::CheckpointError
        } else if lower.contains("resource") || lower.contains("quota") || lower.contains("token") {
            Self::ResourceExhausted
        } else if err.is_empty() {
            Self::None
        } else {
            Self::Unknown
        }
    }
}

/// Agent 执行输出
///
/// 替代原 `String` 返回值，包含摘要文本和执行元数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentOutput {
    /// 最终响应文本（摘要）
    pub summary: String,
    /// 迭代轮数
    pub iterations: usize,
    /// 工具调用总数
    pub tool_calls_made: usize,
    /// 调用过的工具名称列表（含重复）
    pub tool_names: Vec<String>,
    /// 是否成功完成
    pub success: bool,
    /// 错误消息（失败时）
    pub error_message: Option<String>,
    /// 错误分类（失败时），便于上层差异化处理
    #[serde(default)]
    pub error_kind: AgentErrorKind,
    /// 执行时长（毫秒）
    pub duration_ms: u64,
    /// 工具轨迹（序列化的 ToolTrace，用于父 Agent 回传）
    pub tool_trace: Option<serde_json::Value>,
}

impl AgentOutput {
    pub fn success(
        summary: String,
        iterations: usize,
        tool_calls_made: usize,
        tool_names: Vec<String>,
    ) -> Self {
        Self {
            summary,
            iterations,
            tool_calls_made,
            tool_names,
            success: true,
            error_message: None,
            error_kind: AgentErrorKind::None,
            duration_ms: 0,
            tool_trace: None,
        }
    }

    /// 构建成功输出，附带执行时长和工具轨迹
    pub fn success_with_meta(
        summary: String,
        iterations: usize,
        tool_calls_made: usize,
        tool_names: Vec<String>,
        duration_ms: u64,
        tool_trace: Option<serde_json::Value>,
    ) -> Self {
        Self {
            summary,
            iterations,
            tool_calls_made,
            tool_names,
            success: true,
            error_message: None,
            error_kind: AgentErrorKind::None,
            duration_ms,
            tool_trace,
        }
    }

    pub fn failure(err: impl Into<String>, iterations: usize, tool_calls_made: usize) -> Self {
        let msg = err.into();
        Self {
            summary: String::new(),
            iterations,
            tool_calls_made,
            tool_names: Vec::new(),
            success: false,
            error_kind: AgentErrorKind::from_error_message(&msg),
            error_message: Some(msg),
            duration_ms: 0,
            tool_trace: None,
        }
    }

    /// 构建失败输出，显式指定错误分类
    pub fn failure_with_kind(
        err: impl Into<String>,
        iterations: usize,
        tool_calls_made: usize,
        kind: AgentErrorKind,
    ) -> Self {
        Self {
            summary: String::new(),
            iterations,
            tool_calls_made,
            tool_names: Vec::new(),
            success: false,
            error_message: Some(err.into()),
            error_kind: kind,
            duration_ms: 0,
            tool_trace: None,
        }
    }
}

/// Agent 运行器 trait
///
/// isolation backend 通过它启动真正的 Agent 对话循环。
/// 实现在 loom-agent crate 中，避免 isolation 直接依赖 agent 层。
#[async_trait]
pub trait AgentRunner: Send + Sync {
    /// 运行 Agent 循环，返回结构化输出
    async fn run(&self, spec: &AgentSpec) -> Result<AgentOutput>;
}