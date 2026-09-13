//! 通用 JSON 键值存储实现
//!
//! 提供 [`loom_core::JsonKeyValueStore`] 的具体实现：
//! - [`memory::InMemoryJsonStore`]：内存实现，单进程/开发测试用
//! - [`postgres::PostgresJsonStore`]：PostgreSQL 实现，生产持久化用
//!
//! 扩展新存储后端：新增文件实现 `JsonKeyValueStore` trait，在 mod.rs 注册即可。

pub mod memory;
pub mod postgres;

pub use memory::InMemoryJsonStore;
pub use postgres::{PostgresConfig, PostgresJsonStore};