//! Cron 定时任务工具集
//!
//! - cronjob_manage: 管理定时任务（创建/列出/更新/删除/立即触发）
//!
//! 任务存储在 PostgreSQL 中，支持 cron 表达式调度。
//! 实际调度执行由后台 scheduler 循环负责。

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::sync::Arc;

pub const CRON_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS cron_jobs (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    schedule        TEXT NOT NULL,
    prompt          TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    delivery        TEXT DEFAULT 'session',
    created_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL,
    last_run_at     BIGINT,
    next_run_at     BIGINT,
    run_count       INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE TABLE IF NOT EXISTS cron_executions (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES cron_jobs(id) ON DELETE CASCADE,
    started_at      BIGINT NOT NULL,
    finished_at     BIGINT,
    status          TEXT NOT NULL DEFAULT 'running',
    output          TEXT,
    error           TEXT,
    trigger_type    TEXT DEFAULT 'scheduled'
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_enabled ON cron_jobs(enabled);
CREATE INDEX IF NOT EXISTS idx_cron_executions_job ON cron_executions(job_id)
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

pub struct CronStore {
    pool: PgPool,
}

impl CronStore {
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
        for stmt in split_schema_statements(CRON_SCHEMA_SQL) {
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

    pub async fn create_job(
        &self,
        name: &str,
        schedule: &str,
        prompt: &str,
        description: Option<&str>,
        delivery: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();
        let next_run = compute_next_run(schedule, now);
        sqlx::query(
            r#"INSERT INTO cron_jobs
               (id, name, description, schedule, prompt, enabled, delivery, created_at, updated_at, next_run_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
        )
        .bind(&id)
        .bind(name)
        .bind(description)
        .bind(schedule)
        .bind(prompt)
        .bind(true)
        .bind(delivery)
        .bind(now)
        .bind(now)
        .bind(next_run)
        .execute(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("cron create: {e}")))?;
        Ok(id)
    }

    pub async fn list_jobs(&self) -> Result<Value> {
        let rows = sqlx::query(
            r#"SELECT id, name, description, schedule, enabled, delivery, created_at, last_run_at, next_run_at, run_count, last_error
               FROM cron_jobs ORDER BY created_at DESC"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("cron list: {e}")))?;

        let jobs: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "id": r.try_get::<String, _>("id").unwrap_or_default(),
                    "name": r.try_get::<String, _>("name").unwrap_or_default(),
                    "description": r.try_get::<Option<String>, _>("description").unwrap_or(None),
                    "schedule": r.try_get::<String, _>("schedule").unwrap_or_default(),
                    "enabled": r.try_get::<bool, _>("enabled").unwrap_or(false),
                    "delivery": r.try_get::<Option<String>, _>("delivery").unwrap_or(None),
                    "created_at": r.try_get::<i64, _>("created_at").unwrap_or(0),
                    "last_run_at": r.try_get::<Option<i64>, _>("last_run_at").unwrap_or(None),
                    "next_run_at": r.try_get::<Option<i64>, _>("next_run_at").unwrap_or(None),
                    "run_count": r.try_get::<i32, _>("run_count").unwrap_or(0),
                    "last_error": r.try_get::<Option<String>, _>("last_error").unwrap_or(None),
                })
            })
            .collect();
        Ok(json!(jobs))
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<Value>> {
        let row = sqlx::query(
            r#"SELECT id, name, description, schedule, prompt, enabled, delivery, created_at, last_run_at, next_run_at, run_count
               FROM cron_jobs WHERE id = $1"#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("cron get: {e}")))?;

        Ok(row.map(|r| {
            json!({
                "id": r.try_get::<String, _>("id").unwrap_or_default(),
                "name": r.try_get::<String, _>("name").unwrap_or_default(),
                "description": r.try_get::<Option<String>, _>("description").unwrap_or(None),
                "schedule": r.try_get::<String, _>("schedule").unwrap_or_default(),
                "prompt": r.try_get::<String, _>("prompt").unwrap_or_default(),
                "enabled": r.try_get::<bool, _>("enabled").unwrap_or(false),
                "delivery": r.try_get::<Option<String>, _>("delivery").unwrap_or(None),
                "created_at": r.try_get::<i64, _>("created_at").unwrap_or(0),
                "last_run_at": r.try_get::<Option<i64>, _>("last_run_at").unwrap_or(None),
                "next_run_at": r.try_get::<Option<i64>, _>("next_run_at").unwrap_or(None),
                "run_count": r.try_get::<i32, _>("run_count").unwrap_or(0),
            })
        }))
    }

    pub async fn update_job(
        &self,
        job_id: &str,
        name: Option<&str>,
        schedule: Option<&str>,
        prompt: Option<&str>,
        description: Option<Option<&str>>,
        enabled: Option<bool>,
    ) -> Result<bool> {
        let now = now_ms();
        let mut set_clauses: Vec<String> = Vec::new();
        let mut idx = 1;

        if name.is_some() {
            set_clauses.push(format!("name = ${idx}"));
            idx += 1;
        }
        if schedule.is_some() {
            set_clauses.push(format!("schedule = ${idx}"));
            idx += 1;
        }
        if prompt.is_some() {
            set_clauses.push(format!("prompt = ${idx}"));
            idx += 1;
        }
        if description.is_some() {
            set_clauses.push(format!("description = ${idx}"));
            idx += 1;
        }
        if set_clauses.is_empty() && enabled.is_none() {
            return Ok(false);
        }

        set_clauses.push(format!("updated_at = ${idx}"));
        idx += 1;
        if enabled.is_some() {
            set_clauses.push(format!("enabled = ${idx}"));
            idx += 1;
        }

        let sql = format!(
            "UPDATE cron_jobs SET {} WHERE id = ${idx}",
            set_clauses.join(", ")
        );

        let mut query = sqlx::query(&sql);
        if let Some(n) = name {
            query = query.bind(n);
        }
        if let Some(s) = schedule {
            query = query.bind(s);
        }
        if let Some(p) = prompt {
            query = query.bind(p);
        }
        if let Some(d) = description {
            query = query.bind(d);
        }
        query = query.bind(now);
        if let Some(e) = enabled {
            query = query.bind(e);
        }
        query = query.bind(job_id);

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("cron update: {e}")))?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn remove_job(&self, job_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM cron_jobs WHERE id = $1")
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("cron remove: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn record_execution(
        &self,
        job_id: &str,
        status: &str,
        output: Option<&str>,
        error: Option<&str>,
        trigger_type: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();
        let finished = if status != "running" { Some(now) } else { None };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("cron exec tx: {e}")))?;

        sqlx::query(
            r#"INSERT INTO cron_executions (id, job_id, started_at, finished_at, status, output, error, trigger_type)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        )
        .bind(&id)
        .bind(job_id)
        .bind(now)
        .bind(finished)
        .bind(status)
        .bind(output)
        .bind(error)
        .bind(trigger_type)
        .execute(&mut *tx)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("cron exec insert: {e}")))?;

        if status == "success" || status == "failed" {
            let run_count_inc = if status == "success" { 1 } else { 0 };
            sqlx::query(
                r#"UPDATE cron_jobs SET last_run_at = $1, run_count = run_count + $2, last_error = $3 WHERE id = $4"#,
            )
            .bind(now)
            .bind(run_count_inc)
            .bind(error)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("cron exec update job: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| loom_core::LoomError::Other(format!("cron exec commit: {e}")))?;

        Ok(id)
    }

    pub async fn list_executions(&self, job_id: &str, limit: i32) -> Result<Value> {
        let rows = sqlx::query(
            r#"SELECT id, job_id, started_at, finished_at, status, output, error, trigger_type
               FROM cron_executions WHERE job_id = $1 ORDER BY started_at DESC LIMIT $2"#,
        )
        .bind(job_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| loom_core::LoomError::Other(format!("cron executions list: {e}")))?;

        let execs: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "id": r.try_get::<String, _>("id").unwrap_or_default(),
                    "job_id": r.try_get::<String, _>("job_id").unwrap_or_default(),
                    "started_at": r.try_get::<i64, _>("started_at").unwrap_or(0),
                    "finished_at": r.try_get::<Option<i64>, _>("finished_at").unwrap_or(None),
                    "status": r.try_get::<String, _>("status").unwrap_or_default(),
                    "output": r.try_get::<Option<String>, _>("output").unwrap_or(None),
                    "error": r.try_get::<Option<String>, _>("error").unwrap_or(None),
                    "trigger_type": r.try_get::<Option<String>, _>("trigger_type").unwrap_or(None),
                })
            })
            .collect();
        Ok(json!(execs))
    }
}

fn compute_next_run(schedule: &str, _from: i64) -> Option<i64> {
    // 简化版 cron 解析：支持 "every N minutes/hours/days" 格式
    // 完整 cron 表达式解析较复杂，这里提供基础支持
    let sched = schedule.trim().to_lowercase();
    let now = Utc::now();

    if let Some(rest) = sched.strip_prefix("every ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(n) = parts[0].parse::<i64>() {
                let dur = match parts[1] {
                    "minute" | "minutes" | "min" | "mins" => chrono::Duration::minutes(n),
                    "hour" | "hours" | "hr" | "hrs" => chrono::Duration::hours(n),
                    "day" | "days" => chrono::Duration::days(n),
                    "week" | "weeks" => chrono::Duration::weeks(n),
                    _ => return None,
                };
                return Some((now + dur).timestamp_millis());
            }
        }
    }

    // 每日 HH:MM 格式
    if let Some(time_str) = sched.strip_prefix("daily at ") {
        let time_parts: Vec<&str> = time_str.split(':').collect();
        if time_parts.len() == 2 {
            if let (Ok(h), Ok(m)) = (time_parts[0].parse::<u32>(), time_parts[1].parse::<u32>()) {
                let mut next = now
                    .with_hour(h)
                    .and_then(|d| d.with_minute(m))
                    .and_then(|d| d.with_second(0))
                    .unwrap_or(now);
                if next <= now {
                    next += chrono::Duration::days(1);
                }
                return Some(next.timestamp_millis());
            }
        }
    }

    // 默认：1小时后
    Some((now + chrono::Duration::hours(1)).timestamp_millis())
}

pub struct CronToolSet {
    store: Arc<CronStore>,
}

impl CronToolSet {
    pub fn new(store: Arc<CronStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ToolSet for CronToolSet {
    fn name(&self) -> &str {
        "cron"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "cronjob_manage".into(),
            description: "Manage scheduled cron jobs. Actions: create, list, update, remove, run_now, executions. Schedule format: 'every N minutes/hours/days' or 'daily at HH:MM'.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["create", "list", "update", "remove", "run_now", "executions"],
                        "description": "Action to perform"
                    },
                    "job_id": {
                        "type": "string",
                        "description": "Job ID (required for update/remove/run_now/executions)"
                    },
                    "name": {
                        "type": "string",
                        "description": "Job name (required for create)"
                    },
                    "schedule": {
                        "type": "string",
                        "description": "Cron schedule expression. Supports 'every N minutes/hours/days' or 'daily at HH:MM' (required for create)"
                    },
                    "prompt": {
                        "type": "string",
                        "description": "Prompt to execute when the job runs (required for create)"
                    },
                    "description": {
                        "type": "string",
                        "description": "Job description"
                    },
                    "enabled": {
                        "type": "boolean",
                        "description": "Whether the job is enabled (for update)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max executions to return (for executions action, default 10)"
                    }
                },
                "required": ["action"]
            }),
            output_schema: json!({"type": "object"}),
            streaming: false,
            tags: vec!["cron".into()],
        }]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "cronjob_manage" => self.manage(&args).await,
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

impl CronToolSet {
    async fn manage(&self, args: &Value) -> Result<Value> {
        let action = args["action"].as_str().unwrap_or("list");

        match action {
            "create" => {
                let name = args["name"]
                    .as_str()
                    .ok_or_else(|| loom_core::LoomError::Other("create requires 'name'".into()))?;
                let schedule = args["schedule"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("create requires 'schedule'".into())
                })?;
                let prompt = args["prompt"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("create requires 'prompt'".into())
                })?;
                let description = args["description"].as_str();
                let delivery = args["delivery"].as_str().unwrap_or("session");

                let id = self
                    .store
                    .create_job(name, schedule, prompt, description, delivery)
                    .await?;

                Ok(json!({
                    "success": true,
                    "action": "create",
                    "job_id": id,
                    "message": format!("Job '{name}' created with schedule '{schedule}'"),
                }))
            }

            "list" => {
                let jobs = self.store.list_jobs().await?;
                Ok(json!({
                    "success": true,
                    "action": "list",
                    "jobs": jobs,
                    "count": jobs.as_array().map(|a| a.len()).unwrap_or(0),
                }))
            }

            "update" => {
                let job_id = args["job_id"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("update requires 'job_id'".into())
                })?;
                let name = args["name"].as_str();
                let schedule = args["schedule"].as_str();
                let prompt = args["prompt"].as_str();
                let description = args["description"].as_str().map(Some);
                let enabled = args["enabled"].as_bool();

                let updated = self
                    .store
                    .update_job(job_id, name, schedule, prompt, description, enabled)
                    .await?;

                if updated {
                    Ok(json!({
                        "success": true,
                        "action": "update",
                        "job_id": job_id,
                        "message": "Job updated successfully",
                    }))
                } else {
                    Ok(json!({
                        "success": false,
                        "action": "update",
                        "error": format!("Job '{job_id}' not found or no changes"),
                    }))
                }
            }

            "remove" => {
                let job_id = args["job_id"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("remove requires 'job_id'".into())
                })?;
                let removed = self.store.remove_job(job_id).await?;
                if removed {
                    Ok(json!({
                        "success": true,
                        "action": "remove",
                        "job_id": job_id,
                        "message": "Job removed",
                    }))
                } else {
                    Ok(json!({
                        "success": false,
                        "action": "remove",
                        "error": format!("Job '{job_id}' not found"),
                    }))
                }
            }

            "run_now" => {
                let job_id = args["job_id"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("run_now requires 'job_id'".into())
                })?;
                let job = self.store.get_job(job_id).await?;
                match job {
                    Some(j) => {
                        let exec_id = self
                            .store
                            .record_execution(job_id, "queued", None, None, "manual")
                            .await?;
                        Ok(json!({
                            "success": true,
                            "action": "run_now",
                            "job_id": job_id,
                            "execution_id": exec_id,
                            "message": format!("Job '{}' triggered for immediate execution", j["name"]),
                        }))
                    }
                    None => Ok(json!({
                        "success": false,
                        "action": "run_now",
                        "error": format!("Job '{job_id}' not found"),
                    })),
                }
            }

            "executions" => {
                let job_id = args["job_id"].as_str().ok_or_else(|| {
                    loom_core::LoomError::Other("executions requires 'job_id'".into())
                })?;
                let limit = args["limit"].as_i64().unwrap_or(10) as i32;
                let execs = self.store.list_executions(job_id, limit).await?;
                Ok(json!({
                    "success": true,
                    "action": "executions",
                    "job_id": job_id,
                    "executions": execs,
                }))
            }

            _ => Ok(json!({
                "success": false,
                "error": format!("Unknown action '{action}'"),
            })),
        }
    }
}