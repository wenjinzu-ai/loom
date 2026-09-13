//! 上下文处理模块
//!
//! 上下文处理机制，按单一职责拆分：
//! - `config`：压缩配置（动态 token 预算）
//! - `engine`：ContextEngine trait（可插拔引擎）
//! - `token`：token 估算与追踪
//! - `tool_pruning`：工具输出多层修剪
//! - `boundary`：压缩边界计算
//! - `state`：压缩状态管理
//! - `summary`：LLM 辅助结构化摘要
//! - `compressor`：ContextCompressor 编排器

pub mod boundary;
pub mod compressor;
pub mod config;
pub mod engine;
pub mod state;
pub mod summary;
pub mod token;
pub mod tool_pruning;

pub use boundary::CompressWindow;
pub use compressor::ContextCompressor;
pub use config::{CompressionConfig, TailMode};
pub use engine::{CompressPreflight, ContextEngine, EngineStatus};
pub use state::{CompressionFailure, CompressionStats, CompressionState, CompressionStateSnapshot};
pub use summary::{assemble_summary_message, fallback_summary, generate_summary};