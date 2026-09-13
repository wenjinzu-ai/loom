//! 内存实现的 JsonKeyValueStore
//!
//! 使用 `Arc<RwLock<HashMap<namespace, HashMap<key, value>>>>` 存储。
//! 适用于单进程、开发测试场景。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use loom_core::{JsonKeyValueStore, Result};

#[derive(Clone, Default)]
pub struct InMemoryJsonStore {
    inner: Arc<RwLock<HashMap<String, HashMap<String, Value>>>>,
}

impl InMemoryJsonStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl JsonKeyValueStore for InMemoryJsonStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        let store = self.inner.read().await;
        Ok(store.get(namespace).and_then(|m| m.get(key).cloned()))
    }

    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let mut store = self.inner.write().await;
        store
            .entry(namespace.to_string())
            .or_insert_with(HashMap::new)
            .insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<bool> {
        let mut store = self.inner.write().await;
        let existed = store
            .get_mut(namespace)
            .map(|m| m.remove(key).is_some())
            .unwrap_or(false);
        Ok(existed)
    }

    async fn list(&self, namespace: &str) -> Result<Vec<(String, Value)>> {
        let store = self.inner.read().await;
        Ok(store
            .get(namespace)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_put_get_delete() {
        let store = InMemoryJsonStore::new();
        assert!(store.get("ns", "k").await.unwrap().is_none());

        store.put("ns", "k", json!({"a": 1})).await.unwrap();
        assert_eq!(store.get("ns", "k").await.unwrap(), Some(json!({"a": 1})));

        assert!(store.delete("ns", "k").await.unwrap());
        assert!(!store.delete("ns", "k").await.unwrap());
        assert!(store.get("ns", "k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        let store = InMemoryJsonStore::new();
        store.put("a", "k", json!(1)).await.unwrap();
        store.put("b", "k", json!(2)).await.unwrap();

        assert_eq!(store.get("a", "k").await.unwrap(), Some(json!(1)));
        assert_eq!(store.get("b", "k").await.unwrap(), Some(json!(2)));
    }

    #[tokio::test]
    async fn test_list() {
        let store = InMemoryJsonStore::new();
        store.put("ns", "k1", json!(1)).await.unwrap();
        store.put("ns", "k2", json!(2)).await.unwrap();

        let list = store.list("ns").await.unwrap();
        assert_eq!(list.len(), 2);
    }
}