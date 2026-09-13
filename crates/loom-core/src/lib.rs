//! Loom Core — Agent OS 核心抽象
//!
//! 定义 Capability、Registry、Lifecycle、Isolation、Orchestration、Message 等核心 trait 和类型。
//! 所有具体实现都在其他 crate 中。

pub mod agent;
pub mod capability;
pub mod checkpoint;
pub mod error;
pub mod isolation;
pub mod lifecycle;
pub mod message;
pub mod orchestration;
pub mod storage;

pub use agent::{AgentErrorKind, AgentOutput, AgentRunner};
pub use capability::{
    CapabilityChunk, CapabilityExecution, CapabilityExecutor, CapabilityKind, CapabilityRegistry,
    CapabilitySource, CapabilitySpec, ToolContext,
};
pub use checkpoint::{
    Checkpoint, CheckpointConfig, CheckpointMetadata, CheckpointSaver, CheckpointTuple,
};
pub use error::{LlmApiError, LlmErrorKind, LoomError, Result, ToolFailureKind};
pub use isolation::{
    select_isolation, ActivitySummary, AgentRuntime, AgentSpec, HealthStatus, IsolationBackend,
    IsolationLevel,
};
pub use lifecycle::{AgentHandle, AgentLaunchRequest, AgentLifecycleManager, AgentState};
pub use message::{AgentMessage, AgentResponse, Event, MessageBus, MessageContent};
pub use orchestration::{Node, OrchestrationEngine, WorkflowContext, WorkflowResult};
pub use storage::{JsonKeyValueStore, MemoryScope};