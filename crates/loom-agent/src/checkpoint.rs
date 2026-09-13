//! Checkpoint 存储
//!
//! 基于 `JsonKeyValueStore` 策略实现，每个 checkpoint 独占一行（key=checkpoint_id），
//! 底层存储可插拔（内存 / PG / MySQL / Redis）。
//!
//! 存储模型（多租户隔离）：
//! - namespace=`checkpoint:{tenant}:{user}`, key=checkpoint_id → 单个 Checkpoint JSON
//! - namespace=`checkpoint_resume:{tenant}:{user}`, key=thread_id → JSON 对象 { checkpoint_id: resume_value }
//!
//! tenant_id / user_id 从 `Checkpoint` 和 `CheckpointConfig` 上的字段读取，
//! 实现物理隔离（不同租户/用户的 checkpoint 存储在不同 namespace 下）。
//!
//! 采用行存储（而非数组存储）的好处：
//! - 每个 checkpoint 可被独立查询、删除
//! - PG 后端可利用结构化列（thread_id、created_at、step）做 SQL 排查
//! - 避免整个 thread 的 checkpoint 数组在每次写入时全量序列化

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use loom_core::{
    Checkpoint, CheckpointConfig, CheckpointMetadata, CheckpointSaver, CheckpointTuple, Result,
};
use serde_json::Value;
use uuid::Uuid;

use loom_core::JsonKeyValueStore;
use loom_infra::InMemoryJsonStore;

/// 基于 JsonKeyValueStore 的 CheckpointSaver 实现
///
/// 存储布局：每个 checkpoint 一行，key = checkpoint.id
pub struct CheckpointStore {
    kv: Arc<dyn JsonKeyValueStore>,
}

impl CheckpointStore {
    pub fn new(kv: Arc<dyn JsonKeyValueStore>) -> Self {
        Self { kv }
    }

    /// 判断 checkpoint 是否属于当前 config 指定的租户/用户作用域
    ///
    /// 双重校验：除了 namespace 物理隔离外，还在结果层校验 tenant_id/user_id，
    /// 防止 namespace 构造错误或 global 命名空间下的数据混淆。
    fn belongs_to_scope(cp: &Checkpoint, config: &CheckpointConfig) -> bool {
        match (&config.tenant_id, &config.user_id) {
            (Some(t), Some(u)) => cp.tenant_id.as_deref() == Some(t) && cp.user_id.as_deref() == Some(u),
            (Some(t), None) => cp.tenant_id.as_deref() == Some(t),
            (None, Some(u)) => cp.user_id.as_deref() == Some(u),
            (None, None) => cp.tenant_id.is_none() && cp.user_id.is_none(),
        }
    }

    /// 加载指定 thread 下的所有 checkpoint（按 created_at 升序）
    async fn load_checkpoints(&self, config: &CheckpointConfig) -> Result<Vec<Checkpoint>> {
        let ns = config.namespace();
        let entries = self.kv.list(&ns).await?;
        let mut list: Vec<Checkpoint> = entries
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_value::<Checkpoint>(v).ok())
            .filter(|cp| cp.thread_id == config.thread_id)
            .filter(|cp| Self::belongs_to_scope(cp, config))
            .collect();
        list.sort_by_key(|a| a.created_at);
        Ok(list)
    }

    /// 加载指定 thread 的 resume map
    async fn load_resumes(&self, config: &CheckpointConfig) -> Result<std::collections::HashMap<String, Value>> {
        let ns = format!("{}_resume", config.namespace());
        let Some(v) = self.kv.get(&ns, &config.thread_id).await? else {
            return Ok(std::collections::HashMap::new());
        };
        let map: std::collections::HashMap<String, Value> =
            serde_json::from_value(v).unwrap_or_default();
        Ok(map)
    }

    async fn save_resumes(
        &self,
        config: &CheckpointConfig,
        map: &std::collections::HashMap<String, Value>,
    ) -> Result<()> {
        let ns = format!("{}_resume", config.namespace());
        let v = serde_json::to_value(map).unwrap_or(Value::Object(serde_json::Map::new()));
        self.kv.put(&ns, &config.thread_id, v).await
    }

    fn resolve<'a>(list: &'a [Checkpoint], config: &CheckpointConfig) -> Option<&'a Checkpoint> {
        match &config.checkpoint_id {
            Some(id) => list.iter().find(|c| c.id == *id),
            None => list.last(),
        }
    }
}

#[async_trait]
impl CheckpointSaver for CheckpointStore {
    async fn get(&self, config: &CheckpointConfig) -> Result<Option<Checkpoint>> {
        let list = self.load_checkpoints(config).await?;
        Ok(Self::resolve(&list, config).cloned())
    }

    async fn get_tuple(&self, config: &CheckpointConfig) -> Result<Option<CheckpointTuple>> {
        let list = self.load_checkpoints(config).await?;
        let Some(cp) = Self::resolve(&list, config).cloned() else {
            return Ok(None);
        };

        let resumes = self.load_resumes(config).await?;
        let pending = resumes.get(&cp.id).cloned();

        Ok(Some(CheckpointTuple {
            config: config.clone(),
            checkpoint: cp,
            pending_resume: pending,
        }))
    }

    async fn put(&self, mut checkpoint: Checkpoint) -> Result<Checkpoint> {
        if checkpoint.id.is_empty() {
            checkpoint.id = Uuid::new_v4().to_string();
        }
        if checkpoint.created_at.timestamp_millis() <= 0 {
            checkpoint.created_at = Utc::now();
        }

        let ns = checkpoint.namespace();
        let v = serde_json::to_value(&checkpoint).unwrap_or(Value::Null);
        self.kv.put(&ns, &checkpoint.id, v).await?;

        Ok(checkpoint)
    }

    async fn put_writes(&self, config: &CheckpointConfig, writes: Value) -> Result<()> {
        let list = self.load_checkpoints(config).await?;
        let cp_id = match &config.checkpoint_id {
            Some(id) => {
                if !list.iter().any(|c| &c.id == id) {
                    return Err(loom_core::LoomError::Other(format!(
                        "checkpoint {} not found in current scope (thread={}, tenant={:?}, user={:?})",
                        id, config.thread_id, config.tenant_id, config.user_id
                    )));
                }
                id.clone()
            }
            None => match list.last() {
                Some(cp) => cp.id.clone(),
                None => return Ok(()),
            },
        };

        let mut resumes = self.load_resumes(config).await?;
        resumes.insert(cp_id, writes);
        self.save_resumes(config, &resumes).await
    }

    async fn list(&self, config: &CheckpointConfig) -> Result<Vec<Checkpoint>> {
        let mut list = self.load_checkpoints(config).await?;
        list.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(list)
    }

    async fn clear_resume(&self, config: &CheckpointConfig) -> Result<()> {
        let cp_id = match &config.checkpoint_id {
            Some(id) => id.clone(),
            None => return Ok(()),
        };
        let mut resumes = self.load_resumes(config).await?;
        resumes.remove(&cp_id);
        self.save_resumes(config, &resumes).await
    }

    async fn delete_thread(&self, config: &CheckpointConfig) -> Result<()> {
        let ns = config.namespace();
        let entries = self.kv.list(&ns).await?;
        for (key, v) in entries {
            if let Ok(cp) = serde_json::from_value::<Checkpoint>(v) {
                if cp.thread_id == config.thread_id && Self::belongs_to_scope(&cp, config) {
                    self.kv.delete(&ns, &key).await?;
                }
            }
        }
        self.kv.delete(&format!("{ns}_resume"), &config.thread_id).await?;
        Ok(())
    }
}

/// 从状态值构建一个新检查点
pub fn make_checkpoint(
    thread_id: impl Into<String>,
    parent_id: Option<String>,
    channel_values: Value,
    metadata: CheckpointMetadata,
) -> Checkpoint {
    Checkpoint {
        id: Uuid::new_v4().to_string(),
        thread_id: thread_id.into(),
        parent_id,
        channel_values,
        channel_versions: std::collections::HashMap::new(),
        metadata,
        created_at: Utc::now(),
        tenant_id: None,
        user_id: None,
    }
}

/// 从状态值构建一个新检查点（带租户/用户作用域）
pub fn make_checkpoint_scoped(
    thread_id: impl Into<String>,
    parent_id: Option<String>,
    channel_values: Value,
    metadata: CheckpointMetadata,
    tenant_id: Option<String>,
    user_id: Option<String>,
) -> Checkpoint {
    Checkpoint {
        id: Uuid::new_v4().to_string(),
        thread_id: thread_id.into(),
        parent_id,
        channel_values,
        channel_versions: std::collections::HashMap::new(),
        metadata,
        created_at: Utc::now(),
        tenant_id,
        user_id,
    }
}

/// 内存 checkpoint saver（便捷构造，内部使用 InMemoryJsonStore）
pub fn in_memory_checkpoint_saver() -> Arc<dyn CheckpointSaver> {
    Arc::new(CheckpointStore::new(Arc::new(InMemoryJsonStore::new())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn make_saver() -> CheckpointStore {
        CheckpointStore::new(Arc::new(InMemoryJsonStore::new()))
    }

    #[tokio::test]
    async fn test_put_and_get_checkpoint() {
        let saver = make_saver();
        let cp = make_checkpoint(
            "thread-1",
            None,
            json!({"messages": [], "iterations": 1}),
            CheckpointMetadata {
                step: 1,
                source: "step".to_string(),
                extra: HashMap::new(),
            },
        );
        let saved = saver.put(cp.clone()).await.unwrap();
        assert_eq!(saved.thread_id, "thread-1");

        let config = CheckpointConfig::new("thread-1");
        let fetched = saver.get(&config).await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().id, saved.id);
    }

    #[tokio::test]
    async fn test_get_specific_checkpoint() {
        let saver = make_saver();
        let cp1 = make_checkpoint(
            "thread-1",
            None,
            json!({"iterations": 1}),
            CheckpointMetadata::default(),
        );
        let saved1 = saver.put(cp1).await.unwrap();

        let cp2 = make_checkpoint(
            "thread-1",
            Some(saved1.id.clone()),
            json!({"iterations": 2}),
            CheckpointMetadata::default(),
        );
        let saved2 = saver.put(cp2).await.unwrap();

        let config = CheckpointConfig::new("thread-1").with_checkpoint(&saved1.id);
        let fetched = saver.get(&config).await.unwrap();
        assert_eq!(fetched.unwrap().id, saved1.id);

        let config = CheckpointConfig::new("thread-1");
        let latest = saver.get(&config).await.unwrap();
        assert_eq!(latest.unwrap().id, saved2.id);
    }

    #[tokio::test]
    async fn test_put_writes_and_get_tuple() {
        let saver = make_saver();
        let cp = make_checkpoint(
            "thread-1",
            None,
            json!({"iterations": 1}),
            CheckpointMetadata::default(),
        );
        let saved = saver.put(cp).await.unwrap();

        let config = CheckpointConfig::new("thread-1").with_checkpoint(&saved.id);
        saver
            .put_writes(&config, json!({"approved": true}))
            .await
            .unwrap();

        let tuple = saver.get_tuple(&config).await.unwrap().unwrap();
        assert_eq!(tuple.pending_resume, Some(json!({"approved": true})));
    }

    #[tokio::test]
    async fn test_list_checkpoints() {
        let saver = make_saver();
        for i in 1..=3 {
            let cp = make_checkpoint(
                "thread-1",
                None,
                json!({"iterations": i}),
                CheckpointMetadata::default(),
            );
            saver.put(cp).await.unwrap();
        }

        let config = CheckpointConfig::new("thread-1");
        let list = saver.list(&config).await.unwrap();
        assert_eq!(list.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_thread() {
        let saver = make_saver();
        let cp = make_checkpoint(
            "thread-1",
            None,
            json!({"iterations": 1}),
            CheckpointMetadata::default(),
        );
        saver.put(cp).await.unwrap();

        let config = CheckpointConfig::new("thread-1");
        saver.delete_thread(&config).await.unwrap();

        let fetched = saver.get(&config).await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_nonexistent_thread() {
        let saver = make_saver();
        let config = CheckpointConfig::new("no-such-thread");
        let fetched = saver.get(&config).await.unwrap();
        assert!(fetched.is_none());

        let list = saver.list(&config).await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let saver = make_saver();
        let cp_a = make_checkpoint_scoped(
            "thread-1",
            None,
            json!({"iterations": 1}),
            CheckpointMetadata::default(),
            Some("tenant-a".to_string()),
            Some("user-1".to_string()),
        );
        saver.put(cp_a).await.unwrap();

        let cp_b = make_checkpoint_scoped(
            "thread-1",
            None,
            json!({"iterations": 2}),
            CheckpointMetadata::default(),
            Some("tenant-b".to_string()),
            Some("user-2".to_string()),
        );
        saver.put(cp_b).await.unwrap();

        let config_a =
            CheckpointConfig::new("thread-1").with_tenant(Some("tenant-a".to_string()), Some("user-1".to_string()));
        let list_a = saver.list(&config_a).await.unwrap();
        assert_eq!(list_a.len(), 1);
        assert_eq!(list_a[0].tenant_id.as_deref(), Some("tenant-a"));

        let config_b =
            CheckpointConfig::new("thread-1").with_tenant(Some("tenant-b".to_string()), Some("user-2".to_string()));
        let list_b = saver.list(&config_b).await.unwrap();
        assert_eq!(list_b.len(), 1);
        assert_eq!(list_b[0].tenant_id.as_deref(), Some("tenant-b"));

        let config_global = CheckpointConfig::new("thread-1");
        let list_global = saver.list(&config_global).await.unwrap();
        assert!(list_global.is_empty());
    }
}