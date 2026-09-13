//! Loom Infra — 基础设施层
//!
//! 提供 core trait 的默认实现：
//! - registry: Capability 注册表（按 ID/名称/类型/标签查询）
//! - bus: 消息总线（点对点 / 请求响应 / 发布订阅）
//! - orchestration: DAG 编排引擎
//! - storage: JSON 键值存储（内存 / PostgreSQL）

pub mod bus;
pub mod orchestration;
pub mod registry;
pub mod storage;

pub use bus::InMemoryBus;
pub use orchestration::DagOrchestrator;
pub use registry::InMemoryRegistry;
pub use storage::{InMemoryJsonStore, PostgresConfig, PostgresJsonStore};