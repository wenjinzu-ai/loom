//! Kanban 工具集 — 结构化任务管理
//!
//! 基于 PostgreSQL 持久化，支持：
//! - kanban_create  / kanban_list / kanban_show
//! - kanban_complete / kanban_block / kanban_unblock
//! - kanban_comment / kanban_link
//!
//! 表结构：tasks / task_links / task_comments / task_events / task_runs

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::spec::{ToolSet, ToolSpec};

const KANBAN_LIST_DEFAULT_LIMIT: i64 = 50;
const KANBAN_LIST_MAX_LIMIT: i64 = 200;

pub const KANBAN_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS kanban_tasks (
    id                   TEXT PRIMARY KEY,
    title                TEXT NOT NULL,
    body                 TEXT,
    assignee             TEXT,
    status               TEXT NOT NULL,
    priority             INTEGER NOT NULL DEFAULT 0,
    created_by           TEXT,
    created_at           BIGINT NOT NULL,
    started_at           BIGINT,
    completed_at         BIGINT,
    tenant               TEXT,
    result               TEXT,
    summary              TEXT,
    metadata             JSONB,
    last_heartbeat_at    BIGINT
);

CREATE TABLE IF NOT EXISTS kanban_task_links (
    parent_id  TEXT NOT NULL,
    child_id   TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);

CREATE TABLE IF NOT EXISTS kanban_task_comments (
    id         BIGSERIAL PRIMARY KEY,
    task_id    TEXT NOT NULL,
    author     TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS kanban_task_events (
    id         BIGSERIAL PRIMARY KEY,
    task_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    payload    JSONB,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS kanban_task_runs (
    id           BIGSERIAL PRIMARY KEY,
    task_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   BIGINT NOT NULL,
    ended_at     BIGINT,
    summary      TEXT,
    metadata     JSONB,
    error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_kanban_tasks_status ON kanban_tasks(status);
CREATE INDEX IF NOT EXISTS idx_kanban_tasks_assignee ON kanban_tasks(assignee);
CREATE INDEX IF NOT EXISTS idx_kanban_tasks_tenant ON kanban_tasks(tenant);
CREATE INDEX IF NOT EXISTS idx_kanban_comments_task ON kanban_task_comments(task_id);
CREATE INDEX IF NOT EXISTS idx_kanban_events_task ON kanban_task_events(task_id);
"#;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 将包含多条 SQL 语句的 schema 字符串按分号拆分
fn split_schema_statements(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Kanban 数据访问层，封装 PostgreSQL 连接池
#[derive(Clone)]
pub struct KanbanStore {
    pool: PgPool,
}

impl KanbanStore {
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
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        for stmt in split_schema_statements(KANBAN_SCHEMA_SQL) {
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

    async fn add_event(&self, task_id: &str, kind: &str, payload: Option<Value>) -> Result<()> {
        sqlx::query("INSERT INTO kanban_task_events (task_id, kind, payload, created_at) VALUES ($1, $2, $3, $4)")
            .bind(task_id)
            .bind(kind)
            .bind(payload)
            .bind(now_ms())
            .execute(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("kanban event: {e}")))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_task(
        &self,
        title: &str,
        body: Option<&str>,
        assignee: &str,
        parents: &[String],
        tenant: Option<&str>,
        priority: i32,
        created_by: &str,
        initial_status: &str,
    ) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        let now = now_ms();
        let started_at = if initial_status == "running" {
            Some(now)
        } else {
            None
        };

        sqlx::query(
            r#"INSERT INTO kanban_tasks
               (id, title, body, assignee, status, priority, created_by, created_at, started_at, tenant)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
        )
        .bind(&id)
        .bind(title)
        .bind(body)
        .bind(assignee)
        .bind(initial_status)
        .bind(priority)
        .bind(created_by)
        .bind(now)
        .bind(started_at)
        .bind(tenant)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban create: {e}")))?;

        for parent in parents {
            sqlx::query(
                "INSERT INTO kanban_task_links (parent_id, child_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(parent)
            .bind(&id)
            .execute(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("kanban link: {e}")))?;
        }

        self.add_event(
            &id,
            "created",
            Some(json!({"title": title, "assignee": assignee})),
        )
        .await?;
        Ok(id)
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<Value>> {
        let row = sqlx::query("SELECT * FROM kanban_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("kanban get: {e}")))?;

        match row {
            Some(r) => Ok(Some(task_row_to_value(&r)?)),
            None => Ok(None),
        }
    }

    pub async fn list_tasks(
        &self,
        assignee: Option<&str>,
        status: Option<&str>,
        tenant: Option<&str>,
        include_archived: bool,
        limit: i64,
    ) -> Result<Vec<Value>> {
        let mut sql = String::from("SELECT * FROM kanban_tasks WHERE 1=1");
        let mut binds: Vec<&str> = Vec::new();

        if let Some(a) = assignee {
            sql.push_str(" AND assignee = $");
            sql.push_str(&(binds.len() + 1).to_string());
            binds.push(a);
        }
        if let Some(s) = status {
            sql.push_str(" AND status = $");
            sql.push_str(&(binds.len() + 1).to_string());
            binds.push(s);
        } else if !include_archived {
            sql.push_str(" AND status != 'archived'");
        }
        if let Some(t) = tenant {
            sql.push_str(" AND tenant = $");
            sql.push_str(&(binds.len() + 1).to_string());
            binds.push(t);
        }
        sql.push_str(" ORDER BY priority DESC, created_at DESC LIMIT $");
        sql.push_str(&(binds.len() + 1).to_string());

        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(*b);
        }
        q = q.bind(limit);

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("kanban list: {e}")))?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            result.push(task_row_to_value(row)?);
        }
        Ok(result)
    }

    pub async fn complete_task(
        &self,
        task_id: &str,
        summary: Option<&str>,
        result: Option<&str>,
        metadata: Option<Value>,
    ) -> Result<bool> {
        let now = now_ms();
        let res = sqlx::query(
            r#"UPDATE kanban_tasks
               SET status = 'done', completed_at = $1, summary = $2, result = $3, metadata = $4
               WHERE id = $5 AND status IN ('running', 'ready', 'blocked', 'todo')"#,
        )
        .bind(now)
        .bind(summary)
        .bind(result)
        .bind(metadata)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban complete: {e}")))?;

        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.add_event(
            task_id,
            "completed",
            Some(json!({"summary": summary, "result": result})),
        )
        .await?;
        Ok(true)
    }

    pub async fn block_task(
        &self,
        task_id: &str,
        reason: &str,
        kind: Option<&str>,
    ) -> Result<bool> {
        let res = sqlx::query(
            r#"UPDATE kanban_tasks SET status = 'blocked'
               WHERE id = $1 AND status IN ('running', 'ready')"#,
        )
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban block: {e}")))?;

        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.add_event(
            task_id,
            "blocked",
            Some(json!({"reason": reason, "kind": kind})),
        )
        .await?;
        Ok(true)
    }

    pub async fn unblock_task(&self, task_id: &str) -> Result<bool> {
        let has_open_parents: Option<i64> = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM kanban_task_links l
               JOIN kanban_tasks t ON t.id = l.parent_id
               WHERE l.child_id = $1 AND t.status != 'done'"#,
        )
        .bind(task_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban unblock check: {e}")))?;

        let new_status = if has_open_parents.unwrap_or(0) > 0 {
            "todo"
        } else {
            "ready"
        };

        let res = sqlx::query(
            r#"UPDATE kanban_tasks SET status = $1 WHERE id = $2 AND status = 'blocked'"#,
        )
        .bind(new_status)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban unblock: {e}")))?;

        if res.rows_affected() == 0 {
            return Ok(false);
        }
        self.add_event(task_id, "unblocked", Some(json!({"status": new_status})))
            .await?;
        Ok(true)
    }

    pub async fn add_comment(&self, task_id: &str, author: &str, body: &str) -> Result<i64> {
        let id: (i64,) = sqlx::query_as(
            "INSERT INTO kanban_task_comments (task_id, author, body, created_at) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(task_id)
        .bind(author)
        .bind(body)
        .bind(now_ms())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban comment: {e}")))?;
        Ok(id.0)
    }

    pub async fn list_comments(&self, task_id: &str) -> Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT * FROM kanban_task_comments WHERE task_id = $1 ORDER BY created_at ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban comments: {e}")))?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            result.push(json!({
                "id": row.try_get::<i64, _>("id").unwrap_or(0),
                "task_id": row.try_get::<String, _>("task_id").unwrap_or_default(),
                "author": row.try_get::<String, _>("author").unwrap_or_default(),
                "body": row.try_get::<String, _>("body").unwrap_or_default(),
                "created_at": row.try_get::<i64, _>("created_at").unwrap_or(0),
            }));
        }
        Ok(result)
    }

    pub async fn list_events(&self, task_id: &str) -> Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT * FROM kanban_task_events WHERE task_id = $1 ORDER BY created_at ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban events: {e}")))?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let payload: Option<Value> = row.try_get("payload").unwrap_or(None);
            result.push(json!({
                "id": row.try_get::<i64, _>("id").unwrap_or(0),
                "task_id": row.try_get::<String, _>("task_id").unwrap_or_default(),
                "kind": row.try_get::<String, _>("kind").unwrap_or_default(),
                "payload": payload,
                "created_at": row.try_get::<i64, _>("created_at").unwrap_or(0),
            }));
        }
        Ok(result)
    }

    pub async fn link_tasks(&self, parent_id: &str, child_id: &str) -> Result<()> {
        if parent_id == child_id {
            return Err(loom_core::LoomError::Other(
                "kanban_link: cannot link a task to itself".into(),
            ));
        }
        let creates_cycle: Option<i64> = sqlx::query_scalar(
            r#"WITH RECURSIVE ancestors AS (
                 SELECT parent_id FROM kanban_task_links WHERE child_id = $1
                 UNION
                 SELECT l.parent_id FROM kanban_task_links l
                 JOIN ancestors a ON l.child_id = a.parent_id
               ) SELECT COUNT(*) FROM ancestors WHERE parent_id = $2"#,
        )
        .bind(child_id)
        .bind(parent_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban link cycle check: {e}")))?;

        if creates_cycle.unwrap_or(0) > 0 {
            return Err(loom_core::LoomError::Other(
                "kanban_link: cycle detected".into(),
            ));
        }

        sqlx::query(
            "INSERT INTO kanban_task_links (parent_id, child_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(parent_id)
        .bind(child_id)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban link: {e}")))?;
        Ok(())
    }

    pub async fn parent_ids(&self, task_id: &str) -> Result<Vec<String>> {
        let ids: Vec<(String,)> = sqlx::query_as(
            "SELECT parent_id FROM kanban_task_links WHERE child_id = $1 ORDER BY parent_id",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban parents: {e}")))?;
        Ok(ids.into_iter().map(|r| r.0).collect())
    }

    pub async fn child_ids(&self, task_id: &str) -> Result<Vec<String>> {
        let ids: Vec<(String,)> = sqlx::query_as(
            "SELECT child_id FROM kanban_task_links WHERE parent_id = $1 ORDER BY child_id",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("kanban children: {e}")))?;
        Ok(ids.into_iter().map(|r| r.0).collect())
    }
}

fn task_row_to_value(row: &PgRow) -> Result<Value> {
    let metadata: Option<Value> = row.try_get("metadata").unwrap_or(None);
    Ok(json!({
        "id": row.try_get::<String, _>("id").map_err(|e| loom_core::LoomError::Other(format!("db: {e}")))?,
        "title": row.try_get::<String, _>("title").unwrap_or_default(),
        "body": row.try_get::<Option<String>, _>("body").unwrap_or(None),
        "assignee": row.try_get::<Option<String>, _>("assignee").unwrap_or(None),
        "status": row.try_get::<String, _>("status").unwrap_or_default(),
        "priority": row.try_get::<i32, _>("priority").unwrap_or(0),
        "created_by": row.try_get::<Option<String>, _>("created_by").unwrap_or(None),
        "created_at": row.try_get::<i64, _>("created_at").unwrap_or(0),
        "started_at": row.try_get::<Option<i64>, _>("started_at").unwrap_or(None),
        "completed_at": row.try_get::<Option<i64>, _>("completed_at").unwrap_or(None),
        "tenant": row.try_get::<Option<String>, _>("tenant").unwrap_or(None),
        "result": row.try_get::<Option<String>, _>("result").unwrap_or(None),
        "summary": row.try_get::<Option<String>, _>("summary").unwrap_or(None),
        "metadata": metadata,
    }))
}

pub struct KanbanToolSet {
    store: Arc<KanbanStore>,
}

impl KanbanToolSet {
    pub fn new(store: Arc<KanbanStore>) -> Self {
        Self { store }
    }
}

fn spec(name: &str, description: &str, properties: Value, required: Vec<&str>) -> ToolSpec {
    let mut props = properties.as_object().cloned().unwrap_or_default();
    props.insert("board".into(), json!({"type": "string", "description": "Optional board slug (reserved for multi-board support)."}));
    ToolSpec {
        name: name.into(),
        description: description.into(),
        input_schema: json!({
            "type": "object",
            "properties": props,
            "required": required,
        }),
        output_schema: json!({"type": "object"}),
        streaming: false,
        tags: vec!["kanban".into()],
    }
}

#[async_trait]
impl ToolSet for KanbanToolSet {
    fn name(&self) -> &str {
        "kanban"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            spec(
                "kanban_show",
                "Read a task's full state: title, body, assignee, status, parents, children, comments, and recent events.",
                json!({
                    "task_id": {"type": "string", "description": "Task id to read."}
                }),
                vec!["task_id"],
            ),
            spec(
                "kanban_list",
                "List Kanban tasks with filters (assignee, status, tenant). Default 50, max 200.",
                json!({
                    "assignee": {"type": "string", "description": "Optional assignee filter."},
                    "status": {"type": "string", "enum": ["triage","todo","ready","running","blocked","done","archived"], "description": "Optional status filter."},
                    "tenant": {"type": "string", "description": "Optional tenant/project filter."},
                    "include_archived": {"type": "boolean", "description": "Include archived tasks. Default false."},
                    "limit": {"type": "integer", "description": "Max rows (default 50, max 200)."}
                }),
                vec![],
            ),
            spec(
                "kanban_create",
                "Create a new kanban task. The dispatcher picks up tasks with an assignee.",
                json!({
                    "title": {"type": "string", "description": "Short task title (required)."},
                    "assignee": {"type": "string", "description": "Profile name that should execute this task (required)."},
                    "body": {"type": "string", "description": "Full spec, acceptance criteria, links."},
                    "parents": {"type": "array", "items": {"type": "string"}, "description": "Parent task ids; child stays todo until all parents done."},
                    "tenant": {"type": "string", "description": "Optional namespace for multi-project isolation."},
                    "priority": {"type": "integer", "description": "Dispatcher tiebreaker. Higher = picked sooner."},
                    "initial_status": {"type": "string", "enum": ["todo","ready","running"], "description": "Initial status (default running)."}
                }),
                vec!["title", "assignee"],
            ),
            spec(
                "kanban_complete",
                "Mark the current task done with a structured handoff. Provide summary (preferred) or result.",
                json!({
                    "task_id": {"type": "string", "description": "Task id to complete."},
                    "summary": {"type": "string", "description": "Human-readable handoff, 1-3 sentences."},
                    "result": {"type": "string", "description": "Short result log line."},
                    "metadata": {"type": "object", "description": "Structured facts (changed_files, tests_run, findings, etc)."}
                }),
                vec!["task_id"],
            ),
            spec(
                "kanban_block",
                "Transition the task to blocked with a reason a human will read.",
                json!({
                    "task_id": {"type": "string", "description": "Task id to block."},
                    "reason": {"type": "string", "description": "Reason for blocking (required)."},
                    "kind": {"type": "string", "description": "Optional block kind."}
                }),
                vec!["task_id", "reason"],
            ),
            spec(
                "kanban_unblock",
                "Unblock a task. Moves to ready when all parents are done, or todo while parents remain open.",
                json!({
                    "task_id": {"type": "string", "description": "Blocked task id to unblock."}
                }),
                vec!["task_id"],
            ),
            spec(
                "kanban_comment",
                "Append a comment to a task's thread.",
                json!({
                    "task_id": {"type": "string", "description": "Task id to comment on."},
                    "body": {"type": "string", "description": "Comment body (required)."}
                }),
                vec!["task_id", "body"],
            ),
            spec(
                "kanban_link",
                "Add a parent→child dependency edge. Cycles and self-links are rejected.",
                json!({
                    "parent_id": {"type": "string", "description": "Parent task id."},
                    "child_id": {"type": "string", "description": "Child task id."}
                }),
                vec!["parent_id", "child_id"],
            ),
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "kanban_show" => self.kanban_show(&args).await,
            "kanban_list" => self.kanban_list(&args).await,
            "kanban_create" => self.kanban_create(&args).await,
            "kanban_complete" => self.kanban_complete(&args).await,
            "kanban_block" => self.kanban_block(&args).await,
            "kanban_unblock" => self.kanban_unblock(&args).await,
            "kanban_comment" => self.kanban_comment(&args).await,
            "kanban_link" => self.kanban_link(&args).await,
            _ => Err(loom_core::LoomError::Other(format!(
                "kanban: unknown tool '{tool_name}'"
            ))),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(futures::stream::once(async move { Ok(v) })))
    }
}

impl KanbanToolSet {
    async fn kanban_show(&self, args: &Value) -> Result<Value> {
        let task_id = args["task_id"].as_str().unwrap_or("");
        if task_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_show: task_id is required".into(),
            ));
        }
        let task = self.store.get_task(task_id).await?;
        let Some(task) = task else {
            return Ok(json!({"success": false, "error": format!("task not found: {task_id}")}));
        };
        let parents = self.store.parent_ids(task_id).await?;
        let children = self.store.child_ids(task_id).await?;
        let comments = self.store.list_comments(task_id).await?;
        let events = self.store.list_events(task_id).await?;
        Ok(json!({
            "success": true,
            "task": task,
            "parents": parents,
            "children": children,
            "comments": comments,
            "events": events,
        }))
    }

    async fn kanban_list(&self, args: &Value) -> Result<Value> {
        let assignee = args["assignee"].as_str();
        let status = args["status"].as_str();
        let tenant = args["tenant"].as_str();
        let include_archived = args["include_archived"].as_bool().unwrap_or(false);
        let limit = args["limit"]
            .as_i64()
            .unwrap_or(KANBAN_LIST_DEFAULT_LIMIT)
            .clamp(1, KANBAN_LIST_MAX_LIMIT);

        let tasks = self
            .store
            .list_tasks(assignee, status, tenant, include_archived, limit)
            .await?;
        Ok(json!({
            "success": true,
            "tasks": tasks,
            "count": tasks.len(),
            "limit": limit,
        }))
    }

    async fn kanban_create(&self, args: &Value) -> Result<Value> {
        let title = args["title"].as_str().unwrap_or("").trim();
        let assignee = args["assignee"].as_str().unwrap_or("").trim();
        if title.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_create: title is required".into(),
            ));
        }
        if assignee.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_create: assignee is required".into(),
            ));
        }
        let body = args["body"].as_str();
        let parents: Vec<String> = args["parents"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tenant = args["tenant"].as_str();
        let priority = args["priority"].as_i64().unwrap_or(0) as i32;
        let initial_status = args["initial_status"].as_str().unwrap_or("running");
        let created_by = std::env::var("LOOM_PROFILE").unwrap_or_else(|_| "worker".into());

        let id = self
            .store
            .create_task(
                title,
                body,
                assignee,
                &parents,
                tenant,
                priority,
                &created_by,
                initial_status,
            )
            .await?;
        let task = self.store.get_task(&id).await?.unwrap_or(json!({"id": id}));
        Ok(json!({
            "success": true,
            "task_id": id,
            "task": task,
        }))
    }

    async fn kanban_complete(&self, args: &Value) -> Result<Value> {
        let task_id = args["task_id"].as_str().unwrap_or("");
        if task_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_complete: task_id is required".into(),
            ));
        }
        let summary = args["summary"].as_str();
        let result = args["result"].as_str();
        if summary.is_none() && result.is_none() {
            return Err(loom_core::LoomError::Other(
                "kanban_complete: provide at least one of summary or result".into(),
            ));
        }
        let metadata = args.get("metadata").cloned();
        let ok = self
            .store
            .complete_task(task_id, summary, result, metadata)
            .await?;
        if !ok {
            return Ok(
                json!({"success": false, "error": format!("could not complete {task_id} (unknown id or already terminal)")}),
            );
        }
        Ok(json!({"success": true, "task_id": task_id}))
    }

    async fn kanban_block(&self, args: &Value) -> Result<Value> {
        let task_id = args["task_id"].as_str().unwrap_or("");
        let reason = args["reason"].as_str().unwrap_or("");
        if task_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_block: task_id is required".into(),
            ));
        }
        if reason.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_block: reason is required".into(),
            ));
        }
        let kind = args["kind"].as_str();
        let ok = self.store.block_task(task_id, reason, kind).await?;
        if !ok {
            return Ok(
                json!({"success": false, "error": format!("could not block {task_id} (unknown id or not in running/ready)")}),
            );
        }
        Ok(json!({"success": true, "task_id": task_id, "status": "blocked"}))
    }

    async fn kanban_unblock(&self, args: &Value) -> Result<Value> {
        let task_id = args["task_id"].as_str().unwrap_or("");
        if task_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_unblock: task_id is required".into(),
            ));
        }
        let ok = self.store.unblock_task(task_id).await?;
        if !ok {
            return Ok(
                json!({"success": false, "error": format!("could not unblock {task_id} (not blocked or unknown)")}),
            );
        }
        let task = self.store.get_task(task_id).await?;
        Ok(json!({
            "success": true,
            "task_id": task_id,
            "status": task.as_ref().and_then(|t| t.get("status")).cloned().unwrap_or(json!("ready")),
        }))
    }

    async fn kanban_comment(&self, args: &Value) -> Result<Value> {
        let task_id = args["task_id"].as_str().unwrap_or("");
        let body = args["body"].as_str().unwrap_or("");
        if task_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_comment: task_id is required".into(),
            ));
        }
        if body.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_comment: body is required".into(),
            ));
        }
        let author = std::env::var("LOOM_PROFILE").unwrap_or_else(|_| "worker".into());
        let comment_id = self.store.add_comment(task_id, &author, body).await?;
        Ok(json!({"success": true, "task_id": task_id, "comment_id": comment_id}))
    }

    async fn kanban_link(&self, args: &Value) -> Result<Value> {
        let parent_id = args["parent_id"].as_str().unwrap_or("");
        let child_id = args["child_id"].as_str().unwrap_or("");
        if parent_id.is_empty() || child_id.is_empty() {
            return Err(loom_core::LoomError::Other(
                "kanban_link: both parent_id and child_id are required".into(),
            ));
        }
        self.store.link_tasks(parent_id, child_id).await?;
        Ok(json!({"success": true, "parent_id": parent_id, "child_id": child_id}))
    }
}