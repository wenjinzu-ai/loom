//! Loom Memory — 记忆子系统
//!
//! 提供三层抽象：
//!
//! 1. **MemoryStore** (`store`): 显式记忆存储（curated memory）
//!    - 双目标 `user` / `memory`，字符预算，add/replace/remove/apply_batch
//!    - 威胁扫描拒绝 prompt injection，批量原子操作，consolidation failure budget
//!    - 系统提示词块冻结快照（前缀缓存友好）
//!
//! 2. **MemoryProvider** (`provider`): 记忆提供者 trait
//!    - 完整生命周期钩子：prefetch / sync_turn / on_pre_compress / on_session_end 等
//!    - 支持扩展 mem0 / honcho 等外部记忆后端
//!
//! 3. **MemoryManager** (`manager`): 记忆管理器
//!    - builtin provider 永远第一位，最多 1 个 external provider
//!    - 扇出所有生命周期钩子，provider 间故障隔离
//!    - prefetch/sync 后台串行执行，shutdown 优雅排空
//!
//! 辅助模块：
//! - `threat`: prompt injection / exfiltration 威胁扫描
//! - `context`: `<memory-context>` 围栏 + sanitize + 流式 scrubber
//! - `trivial`: trivial prompt 过滤
//! - `builtin`: BuiltinMemoryProvider（包裹 MemoryStore）
//! - `external`: ExternalMemoryProvider（接入 mem0/honcho 等外部后端）

pub mod builtin;
pub mod context;
pub mod external;
pub mod manager;
pub mod provider;
pub mod store;
pub mod threat;
pub mod trivial;

pub use builtin::BuiltinMemoryProvider;
pub use context::{build_memory_context_block, sanitize_context, StreamingContextScrubber};
pub use external::{ExternalMemoryConfig, ExternalMemoryProvider, ExternalMemoryTransport, RecalledMemory};
pub use manager::MemoryManager;
pub use provider::{MemoryProvider, ProviderToolSchema, RecallStatus};
pub use store::{MemoryOp, MemoryStore, MemoryTarget, OpResult};
pub use threat::{scan_memory_content, scan_threat, ScanScope};
pub use trivial::is_trivial_prompt;