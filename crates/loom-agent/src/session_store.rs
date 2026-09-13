//! 会话持久化存储
//!
//! 基于 `JsonKeyValueStore` 策略实现，会话的序列化逻辑不变，
//! 底层存储可插拔（内存 / PG / MySQL / Redis）。
//!
//! 存储模型（多租户隔离）：
//! - namespace=`session:{tenant}:{user}`, key=session_id → { title, messages, updated_at }
//!
//! tenant_id / user_id 由 `MemoryScope` 传入，实现物理隔离。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use loom_core::MemoryScope;
use loom_llm::ChatMessage;
use serde::{Deserialize, Serialize};

use loom_core::JsonKeyValueStore;
use loom_infra::InMemoryJsonStore;

/// 会话存储的持久化结构
#[derive(Serialize, Deserialize)]
struct SessionRecord {
    title: String,
    messages: Vec<ChatMessage>,
    updated_at: u64,
    /// 租户 ID（多租户隔离，与 namespace 中的 tenant 对应，用于双重校验）
    #[serde(default)]
    tenant_id: Option<String>,
    /// 用户 ID（多租户隔离，与 namespace 中的 user 对应，用于双重校验）
    #[serde(default)]
    user_id: Option<String>,
}

/// 会话摘要（用于列表展示）
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub title: String,
    pub message_count: usize,
    pub updated_at: u64,
}

/// 从消息中推导标题
pub fn derive_title(messages: &[ChatMessage]) -> String {
    for m in messages {
        if m.role == loom_llm::Role::System {
            if let Some(c) = &m.content {
                if let Some(title) = c.strip_prefix("[session-title] ") {
                    return title.trim().to_string();
                }
            }
        }
    }
    for m in messages {
        if m.role == loom_llm::Role::User {
            if let Some(content) = &m.content {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    let title: String = trimmed.chars().take(50).collect();
                    return if trimmed.chars().count() > 50 {
                        format!("{}…", title)
                    } else {
                        title
                    };
                }
            }
        }
    }
    "新会话".to_string()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 会话存储 trait
#[async_trait::async_trait]
pub trait SessionStore: Send + Sync {
    async fn get(&self, scope: &MemoryScope, session_id: &str) -> Option<Vec<ChatMessage>>;
    async fn save(&self, scope: &MemoryScope, session_id: &str, messages: &[ChatMessage]) -> anyhow::Result<()>;
    async fn delete(&self, scope: &MemoryScope, session_id: &str) -> anyhow::Result<bool>;
    async fn list(&self, scope: &MemoryScope) -> anyhow::Result<Vec<SessionSummary>>;
}

/// 基于 JsonKeyValueStore 的 SessionStore 实现
pub struct SessionStoreImpl {
    kv: Arc<dyn JsonKeyValueStore>,
}

impl SessionStoreImpl {
    pub fn new(kv: Arc<dyn JsonKeyValueStore>) -> Self {
        Self { kv }
    }

    fn namespace(scope: &MemoryScope) -> String {
        match (&scope.tenant_id, &scope.user_id) {
            (Some(t), Some(u)) => format!("session:{t}:{u}"),
            (Some(t), None) => format!("session:{t}"),
            (None, Some(u)) => format!("session:{u}"),
            (None, None) => "session:global".to_string(),
        }
    }

    /// 判断会话记录是否属于当前 scope 指定的租户/用户作用域
    ///
    /// 双重校验：除了 namespace 物理隔离外，还在结果层校验 tenant_id/user_id，
    /// 防止 namespace 构造错误或 global 命名空间下的数据混淆。
    fn belongs_to_scope(record: &SessionRecord, scope: &MemoryScope) -> bool {
        match (&scope.tenant_id, &scope.user_id) {
            (Some(t), Some(u)) => record.tenant_id.as_deref() == Some(t) && record.user_id.as_deref() == Some(u),
            (Some(t), None) => record.tenant_id.as_deref() == Some(t),
            (None, Some(u)) => record.user_id.as_deref() == Some(u),
            (None, None) => record.tenant_id.is_none() && record.user_id.is_none(),
        }
    }

    async fn load_record(&self, scope: &MemoryScope, session_id: &str) -> Option<SessionRecord> {
        let ns = Self::namespace(scope);
        let v = self.kv.get(&ns, session_id).await.ok()??;
        let record: SessionRecord = serde_json::from_value(v).ok()?;
        if Self::belongs_to_scope(&record, scope) {
            Some(record)
        } else {
            None
        }
    }

    async fn save_record(&self, scope: &MemoryScope, session_id: &str, record: &SessionRecord) -> anyhow::Result<()> {
        let ns = Self::namespace(scope);
        let v = serde_json::to_value(record)?;
        self.kv.put(&ns, session_id, v).await?;
        Ok(())
    }
}

#[async_trait]
impl SessionStore for SessionStoreImpl {
    async fn get(&self, scope: &MemoryScope, session_id: &str) -> Option<Vec<ChatMessage>> {
        self.load_record(scope, session_id).await.map(|r| r.messages)
    }

    async fn save(&self, scope: &MemoryScope, session_id: &str, messages: &[ChatMessage]) -> anyhow::Result<()> {
        let record = SessionRecord {
            title: derive_title(messages),
            messages: messages.to_vec(),
            updated_at: now_secs(),
            tenant_id: scope.tenant_id.clone(),
            user_id: scope.user_id.clone(),
        };
        self.save_record(scope, session_id, &record).await
    }

    async fn delete(&self, scope: &MemoryScope, session_id: &str) -> anyhow::Result<bool> {
        let ns = Self::namespace(scope);
        Ok(self.kv.delete(&ns, session_id).await?)
    }

    async fn list(&self, scope: &MemoryScope) -> anyhow::Result<Vec<SessionSummary>> {
        let ns = Self::namespace(scope);
        let entries = self.kv.list(&ns).await?;
        let mut summaries: Vec<SessionSummary> = entries
            .into_iter()
            .filter_map(|(session_id, v)| {
                let record: SessionRecord = serde_json::from_value(v).ok()?;
                if !Self::belongs_to_scope(&record, scope) {
                    return None;
                }
                Some(SessionSummary {
                    session_id,
                    title: record.title,
                    message_count: record.messages.len(),
                    updated_at: record.updated_at,
                })
            })
            .collect();
        summaries.sort_by_key(|a| std::cmp::Reverse(a.updated_at));
        Ok(summaries)
    }
}

/// 内存会话存储（便捷构造，内部使用 InMemoryJsonStore）
pub fn in_memory_session_store() -> Arc<dyn SessionStore> {
    Arc::new(SessionStoreImpl::new(Arc::new(InMemoryJsonStore::new())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> SessionStoreImpl {
        SessionStoreImpl::new(Arc::new(InMemoryJsonStore::new()))
    }

    fn default_scope() -> MemoryScope {
        MemoryScope::default()
    }

    #[tokio::test]
    async fn test_save_and_get() {
        let store = make_store();
        let scope = default_scope();
        let msgs = vec![ChatMessage {
            role: loom_llm::Role::User,
            content: Some("hello".to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];
        store.save(&scope, "s1", &msgs).await.unwrap();
        let fetched = store.get(&scope, "s1").await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].content, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_delete() {
        let store = make_store();
        let scope = default_scope();
        let msgs = vec![ChatMessage {
            role: loom_llm::Role::User,
            content: Some("hi".to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];
        store.save(&scope, "s1", &msgs).await.unwrap();
        assert!(store.delete(&scope, "s1").await.unwrap());
        assert!(!store.delete(&scope, "s1").await.unwrap());
        assert!(store.get(&scope, "s1").await.is_none());
    }

    #[tokio::test]
    async fn test_list() {
        let store = make_store();
        let scope = default_scope();
        let msgs = vec![ChatMessage {
            role: loom_llm::Role::User,
            content: Some("hi".to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];
        store.save(&scope, "s1", &msgs).await.unwrap();
        store.save(&scope, "s2", &msgs).await.unwrap();

        let list = store.list(&scope).await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let store = make_store();
        let scope_a = MemoryScope::new(Some("tenant-a".to_string()), Some("user-1".to_string()));
        let scope_b = MemoryScope::new(Some("tenant-b".to_string()), Some("user-2".to_string()));

        let msgs = vec![ChatMessage {
            role: loom_llm::Role::User,
            content: Some("hello from A".to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];
        store.save(&scope_a, "s1", &msgs).await.unwrap();

        // 租户 B 看不到租户 A 的会话
        assert!(store.get(&scope_b, "s1").await.is_none());
        assert_eq!(store.list(&scope_b).await.unwrap().len(), 0);

        // 租户 A 能看到自己的会话
        assert!(store.get(&scope_a, "s1").await.is_some());
        assert_eq!(store.list(&scope_a).await.unwrap().len(), 1);
    }

    #[test]
    fn test_derive_title_from_user_message() {
        let msgs = vec![ChatMessage {
            role: loom_llm::Role::User,
            content: Some("帮我写一个 Rust 程序".to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];
        assert_eq!(derive_title(&msgs), "帮我写一个 Rust 程序");
    }

    #[test]
    fn test_derive_title_from_system_tag() {
        let msgs = vec![
            ChatMessage {
                role: loom_llm::Role::System,
                content: Some("[session-title] 我的会话".to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: loom_llm::Role::User,
                content: Some("hello".to_string()),
                tool_calls: None,
                tool_call_id: None,
            },
        ];
        assert_eq!(derive_title(&msgs), "我的会话");
    }
}