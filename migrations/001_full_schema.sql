-- Loom 完整数据库 Schema
-- 与 crates/loom-infra/src/storage/postgres.rs 中的 KV_SCHEMA_SQL 保持一致
--
-- 设计原则：
-- - value 列存完整 JSON（get 时直接返回，无需重建，最安全）
-- - 常用查询/排序/过滤字段提取为结构化列（便于排查和 SQL 查询）
-- - 复杂嵌套数据保留在 value 中（避免过度设计，保留灵活性）
-- - 业务表（sessions/checkpoints/checkpoint_resumes/memories/delegation_tasks）
--   不再存储 namespace，仅用 tenant_id + user_id 做隔离，
--   主键为 (tenant_id, user_id, key)，tenant_id/user_id 为空字符串表示全局作用域。
-- - 通用表（kv_store）仍保留 namespace 列（兜底，存未路由的 namespace）。

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