//! 通用 JSON 键值存储抽象（策略模式）
//!
//! `JsonKeyValueStore` 是 checkpoint / session_store / memory 等模块共享的存储策略接口。
//! 业务模块只需关心自身数据的序列化/反序列化，底层存储（内存 / PG / MySQL / Redis）
//! 通过实现该 trait 插拔。
//!
//! 具体实现见 `loom-infra::storage`（InMemory / Postgres）。
//! 扩展新存储后端：在 loom-infra 中新增实现即可，业务模块无需修改。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Result;

/// 记忆作用域：决定记忆的物理隔离边界
///
/// `tenant_id` 和 `user_id` 均可选，由使用者根据场景组合：
/// - 个人应用：`tenant_id=None, user_id=Some("alice")`
/// - 企业 SaaS：`tenant_id=Some("acme"), user_id=Some("bob")`
/// - 团队共享：`tenant_id=Some("acme"), user_id=None`
/// - 全局知识：`tenant_id=None, user_id=None`（慎用）
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryScope {
    /// 租户/组织/工作空间 ID（可选）
    pub tenant_id: Option<String>,
    /// 用户 ID（可选）
    pub user_id: Option<String>,
}

impl MemoryScope {
    pub fn new(tenant_id: Option<String>, user_id: Option<String>) -> Self {
        Self { tenant_id, user_id }
    }

    pub fn user(user_id: impl Into<String>) -> Self {
        Self {
            tenant_id: None,
            user_id: Some(user_id.into()),
        }
    }

    pub fn tenant(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: Some(tenant_id.into()),
            user_id: None,
        }
    }

    pub fn tenant_user(tenant_id: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            tenant_id: Some(tenant_id.into()),
            user_id: Some(user_id.into()),
        }
    }

    /// 生成存储命名空间，格式：`memory:{tenant_id}:{user_id}`
    /// 缺失的维度省略，如 `memory:alice`、`memory:acme`、`memory:global`
    pub fn namespace(&self) -> String {
        match (&self.tenant_id, &self.user_id) {
            (Some(t), Some(u)) => format!("memory:{t}:{u}"),
            (Some(t), None) => format!("memory:{t}"),
            (None, Some(u)) => format!("memory:{u}"),
            (None, None) => "memory:global".to_string(),
        }
    }
}

/// 通用 JSON 键值存储接口
///
/// - `namespace`: 逻辑分区（如 "checkpoint" / "session" / "memory"），不同分区互不干扰
/// - `key`: 分区内唯一键
/// - `value`: 任意 JSON 值
#[async_trait]
pub trait JsonKeyValueStore: Send + Sync {
    /// 读取指定键的值
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>>;

    /// 写入指定键的值（覆盖）
    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()>;

    /// 删除指定键，返回是否存在
    async fn delete(&self, namespace: &str, key: &str) -> Result<bool>;

    /// 列出分区内所有 (key, value) 对
    async fn list(&self, namespace: &str) -> Result<Vec<(String, Value)>>;
}