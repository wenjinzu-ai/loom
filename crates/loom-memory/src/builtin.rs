//! Builtin Memory Provider
//!
//! 包裹 `MemoryStore`，实现 `MemoryProvider` trait。
//! 永远是 MemoryManager 中的第一个 provider，提供显式记忆的：
//! - `system_prompt_block`: 渲染 user/memory 块
//! - `prefetch`: 返回空（builtin 无向量召回）
//! - `sync_turn`: no-op（靠显式工具写入）
//! - `get_tool_schemas` / `handle_tool_call`: 暴露 memory 工具

use std::sync::Arc;

use async_trait::async_trait;
use loom_core::MemoryScope;
use serde_json::{json, Value};

use crate::provider::{MemoryProvider, ProviderToolSchema, RecallStatus};
use crate::store::{MemoryOp, MemoryStore, MemoryTarget};



pub struct BuiltinMemoryProvider {
    store: Arc<MemoryStore>,
}

impl BuiltinMemoryProvider {
    pub fn new(store: Arc<MemoryStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<MemoryStore> {
        &self.store
    }
}

#[async_trait]
impl MemoryProvider for BuiltinMemoryProvider {
    fn name(&self) -> &str {
        "builtin"
    }

    async fn is_available(&self) -> bool {
        true
    }

    async fn initialize(&self, scope: &MemoryScope, session_id: &str) -> anyhow::Result<()> {
        self.store.refresh_system_prompt_snapshot(scope, session_id).await?;
        Ok(())
    }

    async fn system_prompt_block(&self, scope: &MemoryScope, _session_id: &str) -> anyhow::Result<Option<String>> {
        let user = self.store.format_for_system_prompt(MemoryTarget::User, scope).await;
        let memory = self.store.format_for_system_prompt(MemoryTarget::Memory, scope).await;
        let blocks: Vec<String> = [user, memory].into_iter().flatten().collect();
        if blocks.is_empty() {
            Ok(None)
        } else {
            Ok(Some(blocks.join("\n\n")))
        }
    }

    async fn prefetch(&self, _scope: &MemoryScope, _query: &str, _session_id: &str) -> anyhow::Result<String> {
        Ok(String::new())
    }

    async fn recall_status(&self) -> anyhow::Result<Option<RecallStatus>> {
        Ok(None)
    }

    fn get_tool_schemas(&self) -> Vec<ProviderToolSchema> {
        vec![ProviderToolSchema {
            name: "memory".into(),
            description: "Persistent memory for storing facts, preferences, and context. Supports single actions (add/replace/remove) or batch operations. Two stores: 'user' (persistent user profile) and 'memory' (session/project context).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add", "replace", "remove", "clear"],
                        "description": "Action to perform"
                    },
                    "target": {
                        "type": "string",
                        "enum": ["user", "memory"],
                        "default": "memory",
                        "description": "Which memory store to modify"
                    },
                    "content": {
                        "type": "string",
                        "description": "Text to add or replace with"
                    },
                    "old_text": {
                        "type": "string",
                        "description": "Text to replace or remove"
                    },
                    "operations": {
                        "type": "array",
                        "description": "Batch operations: array of {action, content, old_text}. When provided, action/content/old_text are ignored.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {"type": "string", "enum": ["add", "replace", "remove"]},
                                "content": {"type": "string"},
                                "old_text": {"type": "string"}
                            }
                        }
                    }
                }
            }),
        }]
    }

    async fn handle_tool_call(&self, name: &str, args: Value, scope: &MemoryScope, session_id: &str) -> anyhow::Result<Value> {
        if name != "memory" {
            anyhow::bail!("unknown tool: {name}");
        }
        // 批量操作
        if let Some(ops) = args["operations"].as_array() {
            if ops.is_empty() {
                return Ok(json!({"success": false, "error": "operations list is empty."}));
            }
            let default_target = MemoryTarget::parse(args["target"].as_str().unwrap_or("memory"))
                .unwrap_or(MemoryTarget::Memory);
            // 按 target 分组，每个 target 独立原子提交
            let mut by_target: std::collections::HashMap<MemoryTarget, Vec<MemoryOp>> =
                std::collections::HashMap::new();
            for op in ops {
                let t = MemoryTarget::parse(op["target"].as_str().unwrap_or(""))
                    .unwrap_or(default_target);
                let parsed = MemoryOp {
                    action: op["action"].as_str().unwrap_or("add").to_string(),
                    content: op["content"].as_str().map(|s| s.to_string()),
                    old_text: op["old_text"].as_str().map(|s| s.to_string()),
                };
                by_target.entry(t).or_default().push(parsed);
            }
            let mut all_success = true;
            let mut messages = Vec::new();
            let mut last_target = default_target;
            for (t, group) in by_target {
                let r = self.store.apply_batch(t, group, scope, session_id).await?;
                if !r.success {
                    all_success = false;
                    messages.push(r.message);
                }
                last_target = t;
            }
            let mut result = json!({
                "success": all_success,
                "target": last_target.as_str(),
            });
            if !messages.is_empty() {
                result["message"] = json!(messages.join("; "));
            }
            return Ok(result);
        }

        // 单操作
        let action = args["action"].as_str().unwrap_or("add");
        let target = MemoryTarget::parse(args["target"].as_str().unwrap_or("memory"))
            .unwrap_or(MemoryTarget::Memory);
        let content = args["content"].as_str().unwrap_or("");
        let old_text = args["old_text"].as_str().unwrap_or("");

        let result = match action {
            "add" => self.store.add(target, content, scope, session_id).await?,
            "replace" => self.store.replace(target, old_text, content, scope, session_id).await?,
            "remove" => self.store.remove(target, old_text, scope, session_id).await?,
            "clear" => self.store.clear(target, scope, session_id).await?,
            _ => {
                return Ok(json!({"success": false, "error": format!("Unknown action '{action}'")}));
            }
        };
        Ok(result.to_json(target))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_core::MemoryScope;
    use loom_infra::InMemoryJsonStore;

    fn new_provider() -> BuiltinMemoryProvider {
        let store = Arc::new(MemoryStore::new(
            Arc::new(InMemoryJsonStore::new()),
        ));
        BuiltinMemoryProvider::new(store)
    }

    #[tokio::test]
    async fn test_builtin_name() {
        let p = new_provider();
        assert_eq!(p.name(), "builtin");
    }

    #[tokio::test]
    async fn test_tool_call_add() {
        let p = new_provider();
        let args = json!({"action": "add", "content": "user likes Rust"});
        let scope = MemoryScope::default();
        let r = p.handle_tool_call("memory", args, &scope, "sess-1").await.unwrap();
        assert!(r["success"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_tool_call_batch() {
        let p = new_provider();
        let args = json!({
            "operations": [
                {"action": "add", "content": "a"},
                {"action": "add", "content": "b"}
            ]
        });
        let scope = MemoryScope::default();
        let r = p.handle_tool_call("memory", args, &scope, "sess-1").await.unwrap();
        assert!(r["success"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_system_prompt_block_empty() {
        let p = new_provider();
        let scope = MemoryScope::default();
        assert!(p.system_prompt_block(&scope, "sess-1").await.unwrap().is_none());
    }
}