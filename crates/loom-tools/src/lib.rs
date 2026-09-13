//! Loom Tools — 内置工具集层
//!
//! 与协议适配层（loom-adapters）解耦：
//! - 这里只关心"工具是什么、怎么执行"
//! - native adapter 负责把 ToolSpec 转成 CapabilitySpec 并桥接执行
//!
//! 新增工具集：实现 [`ToolSet`] trait，在 [`registry::builtins`] 中注册即可。

pub mod clarify;
pub mod code;
pub mod cron;
pub mod echo;
pub mod filesystem;
pub mod kanban;
pub mod memory;
pub mod project;
pub mod skills;
pub mod todo;
pub mod web;

pub mod registry;
pub mod spec;

pub use registry::{
    builtins, builtins_with_kanban, builtins_with_memory, builtins_with_pool,
    builtins_with_pool_and_memory, ToolRegistry,
};
pub use spec::{ToolSet, ToolSpec};