//! Loom Agent — Agent 对话循环（大脑）
//!
//! LLM 决策 → 工具调用（含 spawn_agent 委派）→ 结果回填 → 循环
//!
//! AgentLoop 是 Agent OS 的核心，由 isolation backend 启动并运行。

pub mod agent_loop;
pub mod api_retry;
pub mod checkpoint;
pub mod config;
pub mod context;
pub mod delegation;
mod entrypoints;
pub mod hitl;
pub mod lifecycle;
pub mod loop_helpers;
mod loop_types;
pub mod prompt;
pub mod result;
pub mod session_store;
pub mod tools;

pub use agent_loop::{AgentLoop, AgentRunOutcome, AgentRunResult, AgentRunResultWithHistory};
pub use lifecycle::LifecycleManager;
pub use checkpoint::{in_memory_checkpoint_saver, make_checkpoint, CheckpointStore};
pub use context::{CompressionStats, ContextCompressor, ContextEngine};
pub use config::{
    AgentLoopConfig, ApiRetryConfig, CompressionConfig, ContextFilesConfig, GuardrailConfig,
    LivenessConfig, PromptConfig,
};
pub use delegation::{
    append_output_contract, build_retry_message, coerce_output_schema, extract_json_candidate,
    validate_output, MAX_SCHEMA_RETRIES,
};
pub use hitl::{AgentEvent, ResumeCommand};
pub use result::{
    ErrorClassification, SubagentResult, ToolExecutionEntry, ToolExecutionSummary, ToolTrace,
    ToolTraceEntry, UsageMetadata,
};
pub use session_store::{derive_title, in_memory_session_store, SessionStore, SessionStoreImpl, SessionSummary};

// 存储层统一由 loom-infra 提供，此处 re-export 方便消费方使用
pub use loom_core::JsonKeyValueStore;
pub use loom_infra::{InMemoryJsonStore, PostgresConfig, PostgresJsonStore};