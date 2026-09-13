//! 待办事项工具集
//!
//! - todo_list: 任务列表管理，支持 merge 模式和父子任务
//!
//! 存储后端：
//! - 无数据库时使用内存存储（重启丢失）
//! - 有 PostgreSQL 时使用数据库持久化（跨重启保留）

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct TodoItem {
    pub id: String,
    pub content: String,
    pub status: String,
    pub parent: Option<String>,
}

pub const TODO_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS todo_items (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    parent      TEXT,
    position    INTEGER NOT NULL DEFAULT 0,
    updated_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_todo_items_parent ON todo_items(parent)
"#;

fn split_schema_statements(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
pub trait TodoBackend: Send + Sync {
    async fn list(&self) -> Result<Vec<TodoItem>>;
    async fn replace_all(&self, items: Vec<TodoItem>) -> Result<()>;
    async fn merge_items(&self, items: Vec<TodoItem>) -> Result<()>;
}

pub struct InMemoryTodoBackend {
    items: parking_lot::Mutex<Vec<TodoItem>>,
}

impl Default for InMemoryTodoBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTodoBackend {
    pub fn new() -> Self {
        Self {
            items: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl TodoBackend for InMemoryTodoBackend {
    async fn list(&self) -> Result<Vec<TodoItem>> {
        Ok(self.items.lock().clone())
    }

    async fn replace_all(&self, items: Vec<TodoItem>) -> Result<()> {
        *self.items.lock() = items;
        Ok(())
    }

    async fn merge_items(&self, items: Vec<TodoItem>) -> Result<()> {
        let mut state = self.items.lock();
        for item in items {
            if let Some(existing) = state.iter_mut().find(|i| i.id == item.id) {
                existing.content = item.content;
                existing.status = item.status;
                existing.parent = item.parent;
            } else {
                state.push(item);
            }
        }
        Ok(())
    }
}

pub struct PostgresTodoBackend {
    pool: PgPool,
}

impl PostgresTodoBackend {
    pub async fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        dbname: &str,
    ) -> anyhow::Result<Self> {
        let opts = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(dbname);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        let backend = Self { pool };
        backend.migrate().await?;
        Ok(backend)
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        for stmt in split_schema_statements(TODO_SCHEMA_SQL) {
            match sqlx::query(stmt).execute(&self.pool).await {
                Ok(_) => {}
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("already exists") || msg.contains("duplicate key") {
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl TodoBackend for PostgresTodoBackend {
    async fn list(&self) -> Result<Vec<TodoItem>> {
        let rows =
            sqlx::query("SELECT id, content, status, parent FROM todo_items ORDER BY position ASC")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| loom_core::LoomError::Other(format!("todo list: {e}")))?;
        let items: Vec<TodoItem> = rows
            .iter()
            .map(|r| TodoItem {
                id: r.try_get::<String, _>("id").unwrap_or_default(),
                content: r.try_get::<String, _>("content").unwrap_or_default(),
                status: r.try_get::<String, _>("status").unwrap_or_default(),
                parent: r.try_get::<Option<String>, _>("parent").unwrap_or(None),
            })
            .collect();
        Ok(items)
    }

    async fn replace_all(&self, items: Vec<TodoItem>) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("todo tx: {e}")))?;
        sqlx::query("DELETE FROM todo_items")
            .execute(&mut *tx)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("todo delete: {e}")))?;
        let now = now_ms();
        for (pos, item) in items.iter().enumerate() {
            sqlx::query(
                r#"INSERT INTO todo_items (id, content, status, parent, position, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6)"#,
            )
            .bind(&item.id)
            .bind(&item.content)
            .bind(&item.status)
            .bind(&item.parent)
            .bind(pos as i32)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("todo insert: {e}")))?;
        }
        tx.commit()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("todo commit: {e}")))?;
        Ok(())
    }

    async fn merge_items(&self, items: Vec<TodoItem>) -> Result<()> {
        let now = now_ms();
        for item in items {
            sqlx::query(
                r#"INSERT INTO todo_items (id, content, status, parent, position, updated_at)
                   VALUES ($1, $2, $3, $4, (SELECT COALESCE(MAX(position), -1) + 1 FROM todo_items), $5)
                   ON CONFLICT (id) DO UPDATE SET
                     content = EXCLUDED.content,
                     status = EXCLUDED.status,
                     parent = EXCLUDED.parent,
                     updated_at = EXCLUDED.updated_at"#,
            )
            .bind(&item.id)
            .bind(&item.content)
            .bind(&item.status)
            .bind(&item.parent)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("todo merge: {e}")))?;
        }
        Ok(())
    }
}

pub struct TodoToolSet {
    backend: Arc<dyn TodoBackend>,
}

impl Default for TodoToolSet {
    fn default() -> Self {
        Self::new()
    }
}

impl TodoToolSet {
    pub fn new() -> Self {
        Self {
            backend: Arc::new(InMemoryTodoBackend::new()),
        }
    }

    pub fn with_backend(backend: Arc<dyn TodoBackend>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl ToolSet for TodoToolSet {
    fn name(&self) -> &str {
        "todo"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "todo_list".into(),
            description: "Track a task list for multi-step work (3+ steps). Use for complex tasks with 3+ steps or when the user provides multiple tasks. Call with no parameters to read the current list. List order is priority. Only ONE item in_progress at a time. Mark an item completed only after the work is verified done.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "todos": {
                        "type": "array",
                        "description": "Task items to write. Omit to read the current list.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "description": "Unique identifier for the task"},
                                "content": {"type": "string", "description": "Task description"},
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "completed", "cancelled"],
                                    "description": "Task status"
                                },
                                "parent": {
                                    "type": "string",
                                    "description": "Optional id of another item, making this a nested subtask. Omit for top-level."
                                }
                            },
                            "required": ["id", "content", "status"]
                        }
                    },
                    "merge": {
                        "type": "boolean",
                        "description": "true: update existing items by id, add new ones. false (default): replace the entire list with a fresh plan.",
                        "default": false
                    }
                },
                "required": []
            }),
            output_schema: json!({"type": "object"}),
            streaming: false,
            tags: vec!["todo".into()],
        }]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "todo_list" => self.todo_list(&args).await,
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

impl TodoToolSet {
    async fn todo_list(&self, args: &Value) -> Result<Value> {
        let todos = args["todos"].as_array();
        let merge = args["merge"].as_bool().unwrap_or(false);

        if let Some(todos) = todos {
            let mut new_items: Vec<TodoItem> = Vec::new();
            for todo in todos {
                let id = match todo["id"].as_str() {
                    Some(id) if !id.is_empty() => id.to_string(),
                    _ => continue,
                };
                let content = match todo["content"].as_str() {
                    Some(c) => c.to_string(),
                    None => continue,
                };
                let status = todo["status"].as_str().unwrap_or("pending").to_string();
                let parent = todo["parent"].as_str().map(|s| s.to_string());

                new_items.push(TodoItem {
                    id,
                    content,
                    status,
                    parent,
                });
            }

            if merge {
                self.backend.merge_items(new_items).await?;
            } else {
                self.backend.replace_all(new_items).await?;
            }
        }

        // 重新加载并确保只有一个 in_progress
        let mut items = self.backend.list().await?;
        let mut found_in_progress = false;
        for item in items.iter_mut() {
            if item.status == "in_progress" {
                if found_in_progress {
                    item.status = "pending".to_string();
                } else {
                    found_in_progress = true;
                }
            }
        }
        // 如果修改了 in_progress 状态，需要持久化
        if todos.is_some() {
            self.backend.replace_all(items.clone()).await?;
        }

        let todos_json: Vec<Value> = items
            .iter()
            .map(|item| {
                let mut obj = json!({
                    "id": item.id,
                    "content": item.content,
                    "status": item.status,
                });
                if let Some(ref parent) = item.parent {
                    obj["parent"] = json!(parent);
                }
                obj
            })
            .collect();

        let total = items.len();
        let completed = items.iter().filter(|i| i.status == "completed").count();
        let in_progress = items.iter().filter(|i| i.status == "in_progress").count();

        Ok(json!({
            "todos": todos_json,
            "total": total,
            "completed": completed,
            "in_progress": in_progress,
        }))
    }
}