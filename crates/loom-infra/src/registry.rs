//! Capability 注册表实现
//!
//! 基于内存的注册表，支持按 ID、名称、类型、标签查询。

use async_trait::async_trait;
use loom_core::{CapabilityKind, CapabilityRegistry, CapabilitySpec, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Default)]
pub struct InMemoryRegistry {
    by_id: RwLock<HashMap<Uuid, CapabilitySpec>>,
    by_name: RwLock<HashMap<String, Uuid>>,
}

impl InMemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CapabilityRegistry for InMemoryRegistry {
    async fn register(&self, spec: CapabilitySpec) -> Result<Uuid> {
        let id = spec.id;
        let name = spec.name.clone();

        let mut by_name = self.by_name.write();
        if by_name.contains_key(&name) {
            return Err(loom_core::LoomError::CapabilityAlreadyRegistered(name));
        }
        by_name.insert(name.clone(), id);
        self.by_id.write().insert(id, spec);
        tracing::debug!("registered capability {} ({})", name, id);
        Ok(id)
    }

    async fn unregister(&self, id: &Uuid) -> Result<()> {
        if let Some(spec) = self.by_id.write().remove(id) {
            self.by_name.write().remove(&spec.name);
        }
        Ok(())
    }

    async fn get(&self, id: &Uuid) -> Result<CapabilitySpec> {
        self.by_id
            .read()
            .get(id)
            .cloned()
            .ok_or(loom_core::LoomError::CapabilityNotFound(id.to_string()))
    }

    async fn get_by_name(&self, name: &str) -> Result<CapabilitySpec> {
        let id = self
            .by_name
            .read()
            .get(name)
            .cloned()
            .ok_or(loom_core::LoomError::CapabilityNotFound(name.to_string()))?;
        self.get(&id).await
    }

    async fn list(&self) -> Result<Vec<CapabilitySpec>> {
        Ok(self.by_id.read().values().cloned().collect())
    }

    async fn list_by_kind(&self, kind: CapabilityKind) -> Result<Vec<CapabilitySpec>> {
        Ok(self
            .by_id
            .read()
            .values()
            .filter(|s| s.kind == kind)
            .cloned()
            .collect())
    }

    async fn list_by_tags(&self, tags: &[String]) -> Result<Vec<CapabilitySpec>> {
        Ok(self
            .by_id
            .read()
            .values()
            .filter(|s| tags.iter().any(|t| s.tags.contains(t)))
            .cloned()
            .collect())
    }
}
