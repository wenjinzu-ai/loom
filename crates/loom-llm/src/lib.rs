//! Loom LLM — LLM Provider 抽象层
//!
//! 统一多厂商 LLM 接口（OpenAI 兼容、Anthropic 等），
//! 为 loom-agent 的对话循环提供模型调用能力。

pub mod provider;
pub mod types;

pub use provider::{LlmProvider, MockProvider, OpenAiConfig, OpenAiProvider};
pub use types::{ChatChunk, ChatMessage, ChatResponse, Role, TokenUsage, ToolCall, ToolDefinition};