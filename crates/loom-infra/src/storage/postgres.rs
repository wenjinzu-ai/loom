//! PostgreSQL 实现的 JsonKeyValueStore
//!
//! 按业务分表存储，通过 namespace 前缀路由到对应物理表：
//! - `session:*`           → `sessions` 表
//! - `checkpoint_resume:*` → `checkpoint_resumes` 表
//! - `checkpoint:*`        → `checkpoints` 表
//! - `memory:*`            → `memories` 表
//! - `async_delegations`   → `delegation_tasks` 表
//! - 其他                  → `kv_store` 通用表
//!
//! 多租户隔离：sessions / checkpoints / checkpoint_resumes / memories / delegation_tasks
//! 五张表不再存储 namespace 列，仅用 `tenant_id` + `user_id` 两列做物理隔离，
//! 主键为 `(tenant_id, user_id, key)`。tenant_id/user_id 为空字符串表示全局。
//! 通用兜底表 `kv_store` 仍保留 namespace 列。
//!
//! 每张业务表除了 `value JSONB`（完整数据）外，还提取常用查询字段为结构化列
//! （如 sessions.title、checkpoints.thread_id 等），便于直接 SQL 排查数据。

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

use loom_core::{JsonKeyValueStore, LoomError, Result};

/// PostgreSQL 连接配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_max_connections() -> u32 {
    5
}

fn default_connect_timeout() -> u64 {
    10
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "postgres".to_string(),
            max_connections: 5,
            connect_timeout_secs: 10,
        }
    }
}

impl PostgresConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("POSTGRES_HOST") {
            cfg.host = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_PORT") {
            if let Ok(port) = v.parse() {
                cfg.port = port;
            }
        }
        if let Ok(v) = std::env::var("POSTGRES_USER") {
            cfg.user = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_PASSWORD") {
            cfg.password = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_DBNAME") {
            cfg.dbname = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_MAX_CONNECTIONS") {
            if let Ok(n) = v.parse() {
                cfg.max_connections = n;
            }
        }
        cfg
    }

    pub fn database_url(&self) -> String {
        use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
        let user = utf8_percent_encode(&self.user, NON_ALPHANUMERIC);
        let password = utf8_percent_encode(&self.password, NON_ALPHANUMERIC);
        format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, self.dbname
        )
    }

    pub async fn connect(&self) -> anyhow::Result<PgPool> {
        let opts = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.user)
            .password(&self.password)
            .database(&self.dbname);

        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .acquire_timeout(Duration::from_secs(self.connect_timeout_secs))
            .connect_with(opts)
            .await?;
        Ok(pool)
    }
}

/// 建表 SQL：按业务分表 + 结构化列
///
/// 设计原则：
/// - `value` 列存完整 JSON（get 时直接返回，无需重建，最安全）
/// - 常用查询/排序/过滤字段提取为结构化列（便于排查和 SQL 查询）
/// - 复杂嵌套数据保留在 value 中（避免过度设计，保留灵活性）
/// - 业务表（sessions/checkpoints/checkpoint_resumes/memories/delegation_tasks）
///   不再存储 namespace，仅用 `tenant_id` + `user_id` 做隔离，
///   主键为 `(tenant_id, user_id, key)`，tenant_id/user_id 为空字符串表示全局作用域。
/// - 通用表（kv_store）仍保留 namespace 列（兜底，存未路由的 namespace）。
pub const KV_SCHEMA_SQL: &str = r#"
-- 通用 KV 表（兜底，存未路由的 namespace）
CREATE TABLE IF NOT EXISTS kv_store (
    namespace TEXT NOT NULL,
    key       TEXT NOT NULL,
    value     JSONB NOT NULL,
    PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS idx_kv_namespace ON kv_store(namespace);

-- 会话消息历史表
CREATE TABLE IF NOT EXISTS sessions (
    tenant_id     TEXT NOT NULL DEFAULT '',
    user_id       TEXT NOT NULL DEFAULT '',
    key           TEXT NOT NULL,
    value         JSONB NOT NULL,
    title         TEXT,
    message_count INTEGER DEFAULT 0,
    updated_at    BIGINT,
    PRIMARY KEY (tenant_id, user_id, key)
);
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at DESC);

-- Agent 检查点表
CREATE TABLE IF NOT EXISTS checkpoints (
    tenant_id      TEXT NOT NULL DEFAULT '',
    user_id        TEXT NOT NULL DEFAULT '',
    key            TEXT NOT NULL,
    value          JSONB NOT NULL,
    thread_id      TEXT NOT NULL,
    checkpoint_id  TEXT NOT NULL,
    parent_id      TEXT,
    step           BIGINT DEFAULT 0,
    source         TEXT,
    created_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, user_id, key)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_thread ON checkpoints(tenant_id, user_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_created ON checkpoints(created_at DESC);

-- 检查点恢复值表（HITL 中断恢复）
CREATE TABLE IF NOT EXISTS checkpoint_resumes (
    tenant_id     TEXT NOT NULL DEFAULT '',
    user_id       TEXT NOT NULL DEFAULT '',
    key           TEXT NOT NULL,
    value         JSONB NOT NULL,
    thread_id     TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id, key)
);

-- 记忆存储表（用户画像 + 会话记忆）
CREATE TABLE IF NOT EXISTS memories (
    tenant_id    TEXT NOT NULL DEFAULT '',
    user_id      TEXT NOT NULL DEFAULT '',
    key          TEXT NOT NULL,
    value        JSONB NOT NULL,
    target       TEXT NOT NULL,
    session_id   TEXT,
    entry_count  INTEGER DEFAULT 0,
    PRIMARY KEY (tenant_id, user_id, key)
);

-- 后台委派任务表（按租户隔离，主键 (tenant_id, user_id, key)）
-- 注：key 为 child_agent_id（UUID）全局唯一，delete 可仅用 key 定位；
--     recover_persisted 需恢复所有租户任务，list 返回全部记录。
CREATE TABLE IF NOT EXISTS delegation_tasks (
    tenant_id        TEXT NOT NULL DEFAULT '',
    user_id          TEXT NOT NULL DEFAULT '',
    key              TEXT NOT NULL,
    value            JSONB NOT NULL,
    child_agent_id   TEXT NOT NULL,
    delegation_id    TEXT,
    status           TEXT,
    owner_instance_id TEXT,
    created_at       TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, user_id, key)
);
CREATE INDEX IF NOT EXISTS idx_delegation_tasks_status ON delegation_tasks(status);
"#;

/// 基于 PostgreSQL 的 JsonKeyValueStore
pub struct PostgresJsonStore {
    pool: PgPool,
}

/// 根据 namespace 前缀路由到对应物理表
///
/// 顺序很重要：`checkpoint_resume` 必须在 `checkpoint` 之前匹配
fn resolve_table(namespace: &str) -> &'static str {
    if namespace.starts_with("session:") {
        "sessions"
    } else if namespace.starts_with("checkpoint_resume:") {
        "checkpoint_resumes"
    } else if namespace.starts_with("checkpoint:") {
        "checkpoints"
    } else if namespace.starts_with("memory:") {
        "memories"
    } else if namespace == "async_delegations" {
        "delegation_tasks"
    } else {
        "kv_store"
    }
}

/// 从 namespace 解析 tenant_id / user_id（返回空字符串表示全局）
///
/// namespace 格式（与 MemoryScope / CheckpointConfig.namespace() 保持一致）：
/// - `{prefix}:{tenant}:{user}` → tenant, user
/// - `{prefix}:{tenant}` → tenant, ""
/// - `{prefix}:{user}` → user, ""（单段无法区分 tenant/user，保守归入 tenant 列，
///   实际中 user_id 与 tenant_id 不会同名，不影响隔离一致性）
/// - `{prefix}:global` → "", ""
///
/// 由于业务表不再存储 namespace，此处解析出的 tenant_id/user_id 即作为
/// 主键的一部分，是物理隔离的唯一依据。
fn parse_scope_from_namespace(namespace: &str) -> (String, String) {
    let parts: Vec<&str> = namespace.splitn(3, ':').collect();
    match parts.as_slice() {
        [_, "global"] => (String::new(), String::new()),
        [_, tenant, user] => (tenant.to_string(), user.to_string()),
        [_, tenant_or_user] => (tenant_or_user.to_string(), String::new()),
        _ => (String::new(), String::new()),
    }
}

impl PostgresJsonStore {
    pub async fn new(config: &PostgresConfig) -> anyhow::Result<Self> {
        let pool = config.connect().await?;
        let store = Self { pool };
        store.migrate().await?;
        tracing::info!(
            "postgres kv store connected to {}:{}/{}",
            config.host,
            config.port,
            config.dbname
        );
        Ok(store)
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        // 使用事务包裹整个迁移过程，PostgreSQL 支持 DDL 事务回滚，
        // 中途失败可回滚到迁移前状态，避免留下不完整的表结构。
        let mut tx = self.pool.begin().await?;

        // 检测并清理结构不兼容的旧表。
        // 业务表新结构：无 namespace 列，必须有 tenant_id + user_id 列。
        // 以下两种情况都需要 drop 重建：
        //   1) 表含有旧的 namespace 列
        //   2) 表存在但没有 tenant_id 列（旧版表结构，如 checkpoints(id, thread_id, ...)、memories(target, entry, ...)）
        let biz_tables = [
            "sessions",
            "checkpoints",
            "checkpoint_resumes",
            "memories",
            "delegation_tasks",
        ];
        for table in biz_tables {
            let has_namespace: Option<bool> = sqlx::query_scalar(
                r#"SELECT EXISTS (
                       SELECT 1 FROM information_schema.columns
                       WHERE table_name = $1 AND column_name = 'namespace'
                   )"#,
            )
            .bind(table)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();

            let has_tenant_id: Option<bool> = sqlx::query_scalar(
                r#"SELECT EXISTS (
                       SELECT 1 FROM information_schema.columns
                       WHERE table_name = $1 AND column_name = 'tenant_id'
                   )"#,
            )
            .bind(table)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();

            let exists = has_namespace.is_some() || has_tenant_id.is_some();
            let needs_rebuild =
                exists && (has_namespace.unwrap_or(false) || !has_tenant_id.unwrap_or(false));

            if needs_rebuild {
                tracing::warn!(
                    "table '{}' has legacy structure (namespace={}, tenant_id={}), dropping for rebuild",
                    table,
                    has_namespace.unwrap_or(false),
                    has_tenant_id.unwrap_or(false)
                );
                let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", table);
                sqlx::query(&drop_sql).execute(&mut *tx).await?;
            }
        }

        // 清理重构前遗留的旧表
        sqlx::query("DROP TABLE IF EXISTS chat_sessions CASCADE")
            .execute(&mut *tx)
            .await?;

        for stmt in KV_SCHEMA_SQL.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl JsonKeyValueStore for PostgresJsonStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        let table = resolve_table(namespace);
        let row = if table == "kv_store" {
            let sql = format!("SELECT value FROM {table} WHERE namespace = $1 AND key = $2");
            sqlx::query(&sql)
                .bind(namespace)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv get: {e}")))?
        } else if table == "delegation_tasks" {
            // delegation_tasks 的 key 为 child_agent_id（UUID）全局唯一，
            // 且 namespace 固定为 "async_delegations" 不含 scope 信息，
            // 故仅用 key 定位记录。
            let sql = format!("SELECT value FROM {table} WHERE key = $1");
            sqlx::query(&sql)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv get: {e}")))?
        } else {
            let (tenant_id, user_id) = parse_scope_from_namespace(namespace);
            let sql = format!(
                "SELECT value FROM {table} WHERE tenant_id = $1 AND user_id = $2 AND key = $3"
            );
            sqlx::query(&sql)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv get: {e}")))?
        };

        match row {
            Some(r) => {
                let value: Value = r
                    .try_get("value")
                    .map_err(|e| LoomError::Other(format!("kv get decode: {e}")))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        match resolve_table(namespace) {
            "sessions" => self.put_session(namespace, key, value).await,
            "checkpoints" => self.put_checkpoint(namespace, key, value).await,
            "checkpoint_resumes" => self.put_checkpoint_resume(namespace, key, value).await,
            "memories" => self.put_memory(namespace, key, value).await,
            "delegation_tasks" => self.put_delegation(namespace, key, value).await,
            _ => self.put_generic(namespace, key, value).await,
        }
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<bool> {
        let table = resolve_table(namespace);
        let res = if table == "kv_store" {
            let sql = format!("DELETE FROM {table} WHERE namespace = $1 AND key = $2");
            sqlx::query(&sql)
                .bind(namespace)
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv delete: {e}")))?
        } else if table == "delegation_tasks" {
            // key（child_agent_id UUID）全局唯一，仅用 key 删除
            let sql = format!("DELETE FROM {table} WHERE key = $1");
            sqlx::query(&sql)
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv delete: {e}")))?
        } else {
            let (tenant_id, user_id) = parse_scope_from_namespace(namespace);
            let sql = format!(
                "DELETE FROM {table} WHERE tenant_id = $1 AND user_id = $2 AND key = $3"
            );
            sqlx::query(&sql)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv delete: {e}")))?
        };
        Ok(res.rows_affected() > 0)
    }

    async fn list(&self, namespace: &str) -> Result<Vec<(String, Value)>> {
        let table = resolve_table(namespace);
        let rows = if table == "kv_store" {
            let sql = format!("SELECT key, value FROM {table} WHERE namespace = $1");
            sqlx::query(&sql)
                .bind(namespace)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv list: {e}")))?
        } else if table == "delegation_tasks" {
            // recover_persisted 需恢复所有租户的后台任务，故返回全部记录
            let sql = format!("SELECT key, value FROM {table}");
            sqlx::query(&sql)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv list: {e}")))?
        } else {
            let (tenant_id, user_id) = parse_scope_from_namespace(namespace);
            let sql = format!(
                "SELECT key, value FROM {table} WHERE tenant_id = $1 AND user_id = $2"
            );
            sqlx::query(&sql)
                .bind(&tenant_id)
                .bind(&user_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| LoomError::Other(format!("kv list: {e}")))?
        };

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let key: String = row
                .try_get("key")
                .map_err(|e| LoomError::Other(format!("kv list key: {e}")))?;
            let value: Value = row
                .try_get("value")
                .map_err(|e| LoomError::Other(format!("kv list value: {e}")))?;
            result.push((key, value));
        }
        Ok(result)
    }
}

impl PostgresJsonStore {
    async fn put_generic(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO kv_store (namespace, key, value)
               VALUES ($1, $2, $3)
               ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value"#,
        )
        .bind(namespace)
        .bind(key)
        .bind(&value)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("kv put: {e}")))?;
        Ok(())
    }

    async fn put_session(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let title = value.get("title").and_then(|v| v.as_str()).map(|s| s.to_string());
        let message_count = value
            .get("messages")
            .and_then(|v| v.as_array())
            .map(|a| a.len() as i32);
        let updated_at = value.get("updated_at").and_then(|v| v.as_i64());
        // 与 get/delete/list 保持一致：tenant_id/user_id 统一从 namespace 解析，
        // 不读取 value 中的字段，避免写入与查询的作用域来源不一致。
        let (tenant_id, user_id) = parse_scope_from_namespace(namespace);

        sqlx::query(
            r#"INSERT INTO sessions (key, value, title, message_count, updated_at, tenant_id, user_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (tenant_id, user_id, key) DO UPDATE SET
                 value = EXCLUDED.value,
                 title = EXCLUDED.title,
                 message_count = EXCLUDED.message_count,
                 updated_at = EXCLUDED.updated_at"#,
        )
        .bind(key)
        .bind(&value)
        .bind(title)
        .bind(message_count)
        .bind(updated_at)
        .bind(&tenant_id)
        .bind(&user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("put_session: {e}")))?;
        Ok(())
    }

    async fn put_checkpoint(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let thread_id = value
            .get("thread_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let parent_id = value
            .get("parent_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // 与 get/delete/list 保持一致：tenant_id/user_id 统一从 namespace 解析。
        let (tenant_id, user_id) = parse_scope_from_namespace(namespace);
        let step = value
            .get("metadata")
            .and_then(|m| m.get("step"))
            .and_then(|v| v.as_u64())
            .map(|s| s as i64);
        let source = value
            .get("metadata")
            .and_then(|m| m.get("source"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let created_at: chrono::DateTime<Utc> = value
            .get("created_at")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_else(Utc::now);

        sqlx::query(
            r#"INSERT INTO checkpoints
               (key, value, thread_id, checkpoint_id, parent_id, tenant_id, user_id, step, source, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (tenant_id, user_id, key) DO UPDATE SET
                 value = EXCLUDED.value,
                 thread_id = EXCLUDED.thread_id,
                 parent_id = EXCLUDED.parent_id,
                 step = EXCLUDED.step,
                 source = EXCLUDED.source,
                 created_at = EXCLUDED.created_at"#,
        )
        .bind(key)
        .bind(&value)
        .bind(&thread_id)
        .bind(key)
        .bind(parent_id)
        .bind(&tenant_id)
        .bind(&user_id)
        .bind(step)
        .bind(source)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("put_checkpoint: {e}")))?;
        Ok(())
    }

    async fn put_checkpoint_resume(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let thread_id = key.to_string();
        // resume map 的 key 是 checkpoint_id，可能有多个；用逗号拼接所有 key
        // 作为辅助排查信息（完整数据仍在 value JSONB 中）。
        let checkpoint_ids: Vec<&str> = value
            .as_object()
            .map(|o| o.keys().map(|k| k.as_str()).collect())
            .unwrap_or_default();
        let checkpoint_id = checkpoint_ids.join(",");
        let (tenant_id, user_id) = parse_scope_from_namespace(namespace);

        sqlx::query(
            r#"INSERT INTO checkpoint_resumes (key, value, thread_id, checkpoint_id, tenant_id, user_id)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (tenant_id, user_id, key) DO UPDATE SET
                 value = EXCLUDED.value,
                 thread_id = EXCLUDED.thread_id,
                 checkpoint_id = EXCLUDED.checkpoint_id"#,
        )
        .bind(key)
        .bind(&value)
        .bind(thread_id)
        .bind(checkpoint_id)
        .bind(&tenant_id)
        .bind(&user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("put_checkpoint_resume: {e}")))?;
        Ok(())
    }

    async fn put_memory(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        // MemoryStore 的 value 直接是 Vec<String> JSON 数组
        // target 从 key 推断："user" → User，"session:{id}" → Memory
        let (target, session_id) = if key == "user" {
            ("user".to_string(), None)
        } else if let Some(sid) = key.strip_prefix("session:") {
            ("memory".to_string(), Some(sid.to_string()))
        } else {
            (key.to_string(), None)
        };
        let entry_count = value.as_array().map(|a| a.len() as i32);
        let (tenant_id, user_id) = parse_scope_from_namespace(namespace);

        sqlx::query(
            r#"INSERT INTO memories (key, value, target, session_id, entry_count, tenant_id, user_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (tenant_id, user_id, key) DO UPDATE SET
                 value = EXCLUDED.value,
                 target = EXCLUDED.target,
                 session_id = EXCLUDED.session_id,
                 entry_count = EXCLUDED.entry_count"#,
        )
        .bind(key)
        .bind(&value)
        .bind(target)
        .bind(session_id)
        .bind(entry_count)
        .bind(&tenant_id)
        .bind(&user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("put_memory: {e}")))?;
        Ok(())
    }

    async fn put_delegation(&self, _namespace: &str, key: &str, value: Value) -> Result<()> {
        let child_agent_id = value
            .get("child_agent_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let delegation_id = value
            .get("delegation_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let status = value
            .get("status")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let owner_instance_id = value
            .get("owner_instance_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let created_at: Option<chrono::DateTime<Utc>> = value
            .get("created_at")
            .and_then(|v| serde_json::from_value(v.clone()).ok());
        // tenant_id/user_id 从 value（BackgroundTask）中提取，缺省为空字符串（全局）
        let tenant_id = value
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let user_id = value
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        sqlx::query(
            r#"INSERT INTO delegation_tasks
               (tenant_id, user_id, key, value, child_agent_id, delegation_id, status, owner_instance_id, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (tenant_id, user_id, key) DO UPDATE SET
                 value = EXCLUDED.value,
                 child_agent_id = EXCLUDED.child_agent_id,
                 delegation_id = EXCLUDED.delegation_id,
                 status = EXCLUDED.status,
                 owner_instance_id = EXCLUDED.owner_instance_id,
                 created_at = EXCLUDED.created_at"#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(key)
        .bind(&value)
        .bind(child_agent_id)
        .bind(delegation_id)
        .bind(status)
        .bind(owner_instance_id)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| LoomError::Other(format!("put_delegation: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn make_store() -> Option<PostgresJsonStore> {
        if std::env::var("RUN_PG_TESTS").is_err() {
            return None;
        }
        let cfg = PostgresConfig::from_env();
        match PostgresJsonStore::new(&cfg).await {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("skipping pg test: {e}");
                None
            }
        }
    }

    #[tokio::test]
    async fn test_postgres_kv_put_get() {
        let Some(store) = make_store().await else {
            return;
        };
        let ns = format!("test-ns-{}", uuid::Uuid::new_v4());
        store.put(&ns, "k", json!({"a": 1})).await.unwrap();
        assert_eq!(store.get(&ns, "k").await.unwrap(), Some(json!({"a": 1})));

        store.put(&ns, "k", json!({"a": 2})).await.unwrap();
        assert_eq!(store.get(&ns, "k").await.unwrap(), Some(json!({"a": 2})));

        store.delete(&ns, "k").await.unwrap();
        assert!(store.get(&ns, "k").await.unwrap().is_none());
    }

    // ===== 场景1：业务表不再存储 namespace 列，仅用 tenant_id + user_id 隔离 =====
    #[tokio::test]
    async fn test_scenario1_schema_has_no_namespace_column() {
        let Some(store) = make_store().await else {
            return;
        };
        let biz_tables = ["sessions", "checkpoints", "checkpoint_resumes", "memories"];
        for table in biz_tables {
            let has_ns: Option<bool> = sqlx::query_scalar(
                r#"SELECT EXISTS (
                       SELECT 1 FROM information_schema.columns
                       WHERE table_name = $1 AND column_name = 'namespace'
                   )"#,
            )
            .bind(table)
            .fetch_optional(&store.pool)
            .await
            .unwrap()
            .flatten();
            assert_eq!(
                has_ns,
                Some(false),
                "table '{table}' should NOT have namespace column"
            );

            // 必须有 tenant_id / user_id 列
            let has_tenant: Option<bool> = sqlx::query_scalar(
                r#"SELECT EXISTS (
                       SELECT 1 FROM information_schema.columns
                       WHERE table_name = $1 AND column_name = 'tenant_id'
                   )"#,
            )
            .bind(table)
            .fetch_optional(&store.pool)
            .await
            .unwrap()
            .flatten();
            let has_user: Option<bool> = sqlx::query_scalar(
                r#"SELECT EXISTS (
                       SELECT 1 FROM information_schema.columns
                       WHERE table_name = $1 AND column_name = 'user_id'
                   )"#,
            )
            .bind(table)
            .fetch_optional(&store.pool)
            .await
            .unwrap()
            .flatten();
            assert_eq!(has_tenant, Some(true), "table '{table}' should have tenant_id");
            assert_eq!(has_user, Some(true), "table '{table}' should have user_id");
        }
        eprintln!("[场景1] PASS: 4张业务表均无 namespace 列，且有 tenant_id/user_id 列");
    }

    // ===== 场景2：sessions 跨租户隔离（同 key 不同租户互不干扰） =====
    #[tokio::test]
    async fn test_scenario2_session_tenant_isolation() {
        let Some(store) = make_store().await else {
            return;
        };
        let key = format!("sess-{}", uuid::Uuid::new_v4());
        let ns_a = "session:tenant-a:user-1";
        let ns_b = "session:tenant-b:user-2";

        store
            .put(ns_a, &key, json!({"title":"A会话","tenant_id":"tenant-a","user_id":"user-1","messages":[{"role":"user","content":"A的消息"}]}))
            .await
            .unwrap();
        store
            .put(ns_b, &key, json!({"title":"B会话","tenant_id":"tenant-b","user_id":"user-2","messages":[{"role":"user","content":"B的消息"}]}))
            .await
            .unwrap();

        let va = store.get(ns_a, &key).await.unwrap().unwrap();
        let vb = store.get(ns_b, &key).await.unwrap().unwrap();
        assert_eq!(va["title"], "A会话");
        assert_eq!(vb["title"], "B会话");

        // 列表隔离：A 只看到 A 的，B 只看到 B 的
        let list_a = store.list(ns_a).await.unwrap();
        let list_b = store.list(ns_b).await.unwrap();
        assert_eq!(list_a.len(), 1, "租户A应只看到1条");
        assert_eq!(list_b.len(), 1, "租户B应只看到1条");

        // 直接查表确认主键是 (tenant_id, user_id, key)，有两条不同租户的记录
        let cnt: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sessions WHERE key = $1",
        )
        .bind(&key)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(cnt, 2, "同 key 应存两条不同租户的记录");

        store.delete(ns_a, &key).await.unwrap();
        assert!(store.get(ns_a, &key).await.unwrap().is_none());
        assert!(store.get(ns_b, &key).await.unwrap().is_some(), "删除A不应影响B");
        store.delete(ns_b, &key).await.unwrap();
        eprintln!("[场景2] PASS: sessions 同 key 跨租户完全隔离");
    }

    // ===== 场景3：checkpoints 跨租户隔离（同 checkpoint_id 不同租户） =====
    #[tokio::test]
    async fn test_scenario3_checkpoint_tenant_isolation() {
        let Some(store) = make_store().await else {
            return;
        };
        let cp_id = format!("cp-{}", uuid::Uuid::new_v4());
        let ns_a = "checkpoint:tenant-a:user-1";
        let ns_b = "checkpoint:tenant-b:user-2";

        store
            .put(ns_a, &cp_id, json!({"id":cp_id,"thread_id":"t1","tenant_id":"tenant-a","user_id":"user-1","channel_values":{"k":"a"},"created_at":"2026-01-01T00:00:00Z"}))
            .await
            .unwrap();
        store
            .put(ns_b, &cp_id, json!({"id":cp_id,"thread_id":"t1","tenant_id":"tenant-b","user_id":"user-2","channel_values":{"k":"b"},"created_at":"2026-01-01T00:00:00Z"}))
            .await
            .unwrap();

        let va = store.get(ns_a, &cp_id).await.unwrap().unwrap();
        let vb = store.get(ns_b, &cp_id).await.unwrap().unwrap();
        assert_eq!(va["channel_values"]["k"], "a");
        assert_eq!(vb["channel_values"]["k"], "b");

        let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM checkpoints WHERE checkpoint_id = $1")
            .bind(&cp_id)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(cnt, 2, "同 checkpoint_id 跨租户应存两条");

        store.delete(ns_a, &cp_id).await.unwrap();
        assert!(store.get(ns_b, &cp_id).await.unwrap().is_some());
        store.delete(ns_b, &cp_id).await.unwrap();
        eprintln!("[场景3] PASS: checkpoints 同 checkpoint_id 跨租户隔离，删除互不影响");
    }

    // ===== 场景4：memories 跨租户隔离（同 storage key 不同租户） =====
    #[tokio::test]
    async fn test_scenario4_memory_tenant_isolation() {
        let Some(store) = make_store().await else {
            return;
        };
        let mkey = "user";
        let ns_a = "memory:tenant-a:user-1";
        let ns_b = "memory:tenant-b:user-2";

        store.put(ns_a, mkey, json!(["A的记忆1","A的记忆2"])).await.unwrap();
        store.put(ns_b, mkey, json!(["B的记忆1"])).await.unwrap();

        let va = store.get(ns_a, mkey).await.unwrap().unwrap();
        let vb = store.get(ns_b, mkey).await.unwrap().unwrap();
        assert_eq!(va.as_array().unwrap().len(), 2);
        assert_eq!(vb.as_array().unwrap().len(), 1);

        // list 隔离
        let list_a = store.list(ns_a).await.unwrap();
        let list_b = store.list(ns_b).await.unwrap();
        assert_eq!(list_a.len(), 1);
        assert_eq!(list_b.len(), 1);

        // 查表结构化列 target 被正确提取
        let target: String = sqlx::query_scalar(
            "SELECT target FROM memories WHERE tenant_id = 'tenant-a' AND user_id = 'user-1' AND key = $1",
        )
        .bind(mkey)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(target, "user");

        store.delete(ns_a, mkey).await.unwrap();
        assert!(store.get(ns_b, mkey).await.unwrap().is_some());
        store.delete(ns_b, mkey).await.unwrap();
        eprintln!("[场景4] PASS: memories 同 key 跨租户隔离，结构化列 target 正确提取");
    }

    // ===== 场景5：checkpoint_resumes 跨租户隔离 + 全局作用域(空 tenant/user) =====
    #[tokio::test]
    async fn test_scenario5_checkpoint_resume_isolation_and_global_scope() {
        let Some(store) = make_store().await else {
            return;
        };
        let thread_id = format!("thread-{}", uuid::Uuid::new_v4());
        let ns_a = "checkpoint_resume:tenant-a:user-1";
        let ns_b = "checkpoint_resume:tenant-b:user-2";
        let ns_global = "checkpoint_resume:global";

        store.put(ns_a, &thread_id, json!({"cp-a":{"ok":true}})).await.unwrap();
        store.put(ns_b, &thread_id, json!({"cp-b":{"ok":true}})).await.unwrap();
        store.put(ns_global, &thread_id, json!({"cp-global":{"ok":true}})).await.unwrap();

        let va = store.get(ns_a, &thread_id).await.unwrap().unwrap();
        let vb = store.get(ns_b, &thread_id).await.unwrap().unwrap();
        let vg = store.get(ns_global, &thread_id).await.unwrap().unwrap();
        assert!(va.get("cp-a").is_some());
        assert!(vb.get("cp-b").is_some());
        assert!(vg.get("cp-global").is_some());

        // 三者互不影响
        assert!(va.get("cp-b").is_none());
        assert!(vb.get("cp-a").is_none());
        assert!(vg.get("cp-a").is_none());

        // 全局作用域的 tenant_id/user_id 应为空字符串
        let (t, u): (String, String) = sqlx::query_as(
            "SELECT tenant_id, user_id FROM checkpoint_resumes WHERE key = $1 AND tenant_id = '' AND user_id = ''",
        )
        .bind(&thread_id)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(t, "");
        assert_eq!(u, "");

        store.delete(ns_a, &thread_id).await.unwrap();
        assert!(store.get(ns_b, &thread_id).await.unwrap().is_some());
        assert!(store.get(ns_global, &thread_id).await.unwrap().is_some());
        store.delete(ns_b, &thread_id).await.unwrap();
        store.delete(ns_global, &thread_id).await.unwrap();
        eprintln!("[场景5] PASS: checkpoint_resumes 跨租户隔离 + 全局作用域(空字符串)正常");
    }
}