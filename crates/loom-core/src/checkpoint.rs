//! Checkpoint 与 Human-in-the-Loop 核心抽象
//!
//! - Checkpoint：工作流在某一时刻的完整状态快照
//! - Thread：独立的执行会话（thread_id 标识）
//! - CheckpointSaver：持久化接口，支持多种存储后端
//!
//! Human-in-the-Loop 通过 interrupt 实现：
//! - 执行中调用 interrupt(value) 暂停，保存 checkpoint
//! - 调用者获取 interrupt value，提供用户输入后通过 resume 恢复

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

/// 检查点配置：定位一个具体的检查点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// 线程 ID（会话标识，一个 thread 对应一个独立的执行会话）
    pub thread_id: String,
    /// 检查点 ID（同一 thread 内唯一，空表示获取最新）
    pub checkpoint_id: Option<String>,
    /// 租户 ID（多租户隔离，None 表示全局）
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（多租户隔离，None 表示全局）
    #[serde(default)]
    pub user_id: Option<String>,
}

impl CheckpointConfig {
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            checkpoint_id: None,
            tenant_id: None,
            user_id: None,
        }
    }

    pub fn with_checkpoint(mut self, checkpoint_id: impl Into<String>) -> Self {
        self.checkpoint_id = Some(checkpoint_id.into());
        self
    }

    pub fn with_tenant(mut self, tenant_id: Option<String>, user_id: Option<String>) -> Self {
        self.tenant_id = tenant_id;
        self.user_id = user_id;
        self
    }

    pub fn namespace(&self) -> String {
        match (&self.tenant_id, &self.user_id) {
            (Some(t), Some(u)) => format!("checkpoint:{t}:{u}"),
            (Some(t), None) => format!("checkpoint:{t}"),
            (None, Some(u)) => format!("checkpoint:{u}"),
            (None, None) => "checkpoint:global".to_string(),
        }
    }
}

/// 检查点元数据
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// 步骤序号
    #[serde(default)]
    pub step: u64,
    /// 运行来源（如 "loop"、"resume"）
    #[serde(default)]
    pub source: String,
    /// 自定义键值对（结构化数据，便于扩展）
    #[serde(default)]
    pub extra: HashMap<String, Value>,
}

/// 检查点：保存执行状态的快照
///
/// Checkpoint 核心字段：
/// - `channel_values`: 状态值（消息历史、迭代计数等）
/// - `channel_versions`: 各通道的版本号（用于检测变化）
/// - `pending_writes`: 待处理的写入（如 interrupt 的恢复值）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// 检查点唯一 ID
    pub id: String,
    /// 所属线程 ID
    pub thread_id: String,
    /// 父检查点 ID（构成检查点链）
    pub parent_id: Option<String>,
    /// 状态通道的值
    pub channel_values: Value,
    /// 通道版本号映射
    #[serde(default)]
    pub channel_versions: HashMap<String, u64>,
    /// 元数据
    #[serde(default)]
    pub metadata: CheckpointMetadata,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 租户 ID（多租户隔离）
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（多租户隔离）
    #[serde(default)]
    pub user_id: Option<String>,
}

impl Checkpoint {
    pub fn namespace(&self) -> String {
        match (&self.tenant_id, &self.user_id) {
            (Some(t), Some(u)) => format!("checkpoint:{t}:{u}"),
            (Some(t), None) => format!("checkpoint:{t}"),
            (None, Some(u)) => format!("checkpoint:{u}"),
            (None, None) => "checkpoint:global".to_string(),
        }
    }
}

/// 检查点元组（配置 + 检查点 + 可选的恢复值）
#[derive(Debug, Clone)]
pub struct CheckpointTuple {
    pub config: CheckpointConfig,
    pub checkpoint: Checkpoint,
    /// 恢复时传入的值（interrupt 的 resume 值）
    pub pending_resume: Option<Value>,
}

/// Checkpoint 持久化存储 trait
///
/// CheckpointSaver 持久化接口：
/// - `get`: 获取指定 thread 的最新检查点（或指定 checkpoint_id）
/// - `put`: 保存检查点
/// - `list`: 列出某 thread 的所有检查点
/// - `put_writes`: 保存中间写入（如 resume 值）
/// - `delete_thread`: 删除整个线程的检查点
#[async_trait]
pub trait CheckpointSaver: Send + Sync {
    /// 获取检查点
    async fn get(&self, config: &CheckpointConfig) -> Result<Option<Checkpoint>>;

    /// 获取检查点元组（含恢复值）
    async fn get_tuple(&self, config: &CheckpointConfig) -> Result<Option<CheckpointTuple>>;

    /// 保存检查点，返回保存后的检查点
    async fn put(&self, checkpoint: Checkpoint) -> Result<Checkpoint>;

    /// 保存中间写入（关联到指定检查点）
    async fn put_writes(&self, config: &CheckpointConfig, writes: Value) -> Result<()>;

    /// 列出某线程的所有检查点（按时间倒序）
    ///
    /// 通过 `config.tenant_id` / `config.user_id` 实现多租户隔离，
    /// 确保只能查询到当前租户/用户的 checkpoint。
    async fn list(&self, config: &CheckpointConfig) -> Result<Vec<Checkpoint>>;

    /// 清除指定检查点的 resume 值（一次性消费后调用）
    async fn clear_resume(&self, config: &CheckpointConfig) -> Result<()>;

    /// 删除某线程的所有检查点
    async fn delete_thread(&self, config: &CheckpointConfig) -> Result<()>;
}