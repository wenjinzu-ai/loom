//! 记忆工具集
//!
//! 作为 [`loom_tools::spec::ToolSet`] 的适配层，将 memory 工具调用
//! 委托给 [`loom_memory::BuiltinMemoryProvider`]（显式记忆存储）。
//!
//! 存储目标：
//! - user: 用户级持久记忆
//! - memory: 会话/项目级记忆
//!
//! 底层存储可插拔（内存 / PG / MySQL / Redis），基于 `JsonKeyValueStore` 策略。

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{JsonKeyValueStore, Result, ToolContext};
use loom_memory::{BuiltinMemoryProvider, MemoryProvider};
pub use loom_memory::{MemoryStore, MemoryTarget};
use loom_infra::InMemoryJsonStore;
use serde_json::{json, Value};
use std::sync::Arc;

/// 记忆工具集：适配 [`loom_memory::BuiltinMemoryProvider`]
pub struct MemoryToolSet {
    provider: Arc<BuiltinMemoryProvider>,
}

impl MemoryToolSet {
    pub fn new() -> Self {
        Self::with_kv(Arc::new(InMemoryJsonStore::new()))
    }

    /// 从 JsonKeyValueStore 构建
    pub fn with_kv(kv: Arc<dyn JsonKeyValueStore>) -> Self {
        let store = Arc::new(MemoryStore::new(kv));
        Self {
            provider: Arc::new(BuiltinMemoryProvider::new(store)),
        }
    }

    /// 从已有 MemoryStore 构建
    pub fn with_store(store: Arc<MemoryStore>) -> Self {
        Self {
            provider: Arc::new(BuiltinMemoryProvider::new(store)),
        }
    }

    /// 从已有 BuiltinMemoryProvider 构建
    pub fn with_provider(provider: Arc<BuiltinMemoryProvider>) -> Self {
        Self { provider }
    }
}

impl Default for MemoryToolSet {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ToolSet for MemoryToolSet {
    fn name(&self) -> &str {
        "memory"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "memory".into(),
            description: "Persistent memory for storing facts, preferences, and context across the conversation. Supports single actions (add/replace/remove/clear) or batch operations. Two stores: 'user' (persistent user profile) and 'memory' (session/project context).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add", "replace", "remove", "clear"],
                        "description": "Action to perform: add (append content), replace (replace matching old_text with content), remove (remove exact text), clear (empty the store)"
                    },
                    "target": {
                        "type": "string",
                        "enum": ["user", "memory"],
                        "description": "Which memory store to modify: 'user' for persistent user preferences, 'memory' for session context (default: memory)",
                        "default": "memory"
                    },
                    "content": {
                        "type": "string",
                        "description": "Text to add or replace with"
                    },
                    "old_text": {
                        "type": "string",
                        "description": "Text to replace or remove (exact match)"
                    },
                    "operations": {
                        "type": "array",
                        "description": "Batch operations: array of {action, target, content, old_text} objects. When provided, action/target/content/old_text are ignored.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {"type": "string", "enum": ["add", "replace", "remove", "clear"]},
                                "target": {"type": "string", "enum": ["user", "memory"], "default": "memory"},
                                "content": {"type": "string"},
                                "old_text": {"type": "string"}
                            }
                        }
                    }
                },
                "required": []
            }),
            output_schema: json!({"type": "object"}),
            streaming: false,
            tags: vec!["memory".into()],
        }]
    }

    async fn execute(&self, tool_name: &str, args: Value, ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "memory" => {
                let mut result = self
                    .provider
                    .handle_tool_call("memory", args.clone(), &ctx.scope, &ctx.session_id)
                    .await
                    .map_err(|e| loom_core::LoomError::ToolExecution(e.to_string()))?;

                let store = self.provider.store();
                let user_entries = store
                    .list(MemoryTarget::User, &ctx.scope, &ctx.session_id)
                    .await?;
                let memory_entries = store
                    .list(MemoryTarget::Memory, &ctx.scope, &ctx.session_id)
                    .await?;

                let obj = result.as_object_mut().unwrap();
                obj.insert("user".into(), json!(user_entries.join("\n")));
                obj.insert("memory".into(), json!(memory_entries.join("\n")));

                if args["operations"].is_array() {
                    let count = args["operations"].as_array().unwrap().len();
                    obj.insert("operations".into(), json!(count));
                }

                Ok(result)
            }
            _ => Err(loom_core::LoomError::CapabilityNotFound(tool_name.into())),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(stream::once(async move { Ok(v) })))
    }
}