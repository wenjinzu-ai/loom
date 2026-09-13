use loom_infra::PostgresJsonStore;
use loom_tools::cron::{CronStore, CronToolSet};
use loom_tools::memory::{MemoryStore, MemoryTarget};
use loom_tools::todo::{PostgresTodoBackend, TodoBackend};
use loom_tools::ToolSet;
use loom_core::MemoryScope;
use serde_json::json;
use std::sync::Arc;

async fn pg_pool() -> sqlx::PgPool {
    let opts = sqlx::postgres::PgConnectOptions::new()
        .host("127.0.0.1")
        .port(5432)
        .username("postgres")
        .password("postgres")
        .database("postgres");
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await
        .expect("connect to postgres")
}

#[tokio::test]
async fn memory_postgres_backend() {
    let kv = PostgresJsonStore::from_pool(pg_pool().await);
    kv.migrate().await.expect("migrate");
    let store = MemoryStore::new(Arc::new(kv));
    let scope = MemoryScope::default();
    let session_id = "test-session";

    store.clear(MemoryTarget::User, &scope, session_id).await.unwrap();
    store.clear(MemoryTarget::Memory, &scope, session_id).await.unwrap();

    store.add(MemoryTarget::User, "db_test_prefers_rust", &scope, session_id).await.unwrap();
    store.add(MemoryTarget::User, "db_test_likes_coffee", &scope, session_id).await.unwrap();
    let entries = store.list(MemoryTarget::User, &scope, session_id).await.unwrap();
    assert!(entries.contains(&"db_test_prefers_rust".to_string()));
    assert!(entries.contains(&"db_test_likes_coffee".to_string()));

    store.add(MemoryTarget::Memory, "db_test_working_on_cron", &scope, session_id).await.unwrap();
    let mem = store.list(MemoryTarget::Memory, &scope, session_id).await.unwrap();
    assert!(mem.contains(&"db_test_working_on_cron".to_string()));

    store.remove(MemoryTarget::User, "db_test_prefers_rust", &scope, session_id).await.unwrap();
    store.remove(MemoryTarget::User, "db_test_likes_coffee", &scope, session_id).await.unwrap();
    let after = store.list(MemoryTarget::User, &scope, session_id).await.unwrap();
    assert!(!after.contains(&"db_test_prefers_rust".to_string()));
}

#[tokio::test]
async fn memory_tool_with_postgres() {
    let kv = PostgresJsonStore::from_pool(pg_pool().await);
    kv.migrate().await.expect("migrate");
    let store = Arc::new(MemoryStore::new(Arc::new(kv)));
    let scope = MemoryScope::default();
    let session_id = "test-session";
    store.clear(MemoryTarget::User, &scope, session_id).await.unwrap();
    store.clear(MemoryTarget::Memory, &scope, session_id).await.unwrap();

    let toolset = loom_tools::memory::MemoryToolSet::with_store(store);

    let r = toolset
        .execute(
            "memory", json!({"action": "add", "target": "memory", "content": "hello world"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(r["success"], true);

    let r = toolset
        .execute(
            "memory", json!({"action": "add", "target": "memory", "content": "second entry"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(r["success"], true);
    assert!(r["memory"].as_str().unwrap().contains("hello world"));

    let r = toolset
        .execute(
            "memory", json!({"action": "remove", "target": "memory", "old_text": "hello world"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(r["success"], true);
    assert!(!r["memory"].as_str().unwrap().contains("hello world"));

    let r = toolset
        .execute("memory", json!({"action": "clear", "target": "memory"}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(r["success"], true);
    assert!(r["memory"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn cron_full_workflow() {
    let pool = pg_pool().await;
    let store = CronStore::from_pool(pool);
    store.migrate().await.expect("migrate");

    let toolset = CronToolSet::new(Arc::new(store));

    let created = toolset
        .execute(
            "cronjob_manage", json!({
                "action": "create",
                "name": "daily report",
                "schedule": "every 30 minutes",
                "prompt": "generate a daily status report",
                "description": "runs every 30 min",
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(created["success"], true);
    let job_id = created["job_id"].as_str().unwrap().to_string();
    println!("created job: {job_id}");

    let listed = toolset
        .execute("cronjob_manage", json!({"action": "list"}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(listed["success"], true);
    assert!(listed["count"].as_i64().unwrap() >= 1);

    let updated = toolset
        .execute(
            "cronjob_manage", json!({"action": "update", "job_id": job_id, "name": "daily report v2", "enabled": false}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(updated["success"], true);

    let triggered = toolset
        .execute(
            "cronjob_manage", json!({"action": "run_now", "job_id": job_id}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(triggered["success"], true);
    assert!(triggered["execution_id"].is_string());

    let execs = toolset
        .execute(
            "cronjob_manage", json!({"action": "executions", "job_id": job_id}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(execs["success"], true);
    assert!(!execs["executions"].as_array().unwrap().is_empty());

    let removed = toolset
        .execute(
            "cronjob_manage", json!({"action": "remove", "job_id": job_id}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(removed["success"], true);
}

#[tokio::test]
async fn todo_postgres_backend() {
    let pool = pg_pool().await;
    let backend = PostgresTodoBackend::from_pool(pool);
    backend.migrate().await.expect("migrate");
    backend.replace_all(vec![]).await.unwrap();

    let toolset = loom_tools::todo::TodoToolSet::with_backend(Arc::new(backend));

    let r = toolset
        .execute(
            "todo_list", json!({
                "todos": [
                    {"id": "1", "content": "setup db", "status": "completed"},
                    {"id": "2", "content": "write tests", "status": "in_progress"},
                    {"id": "3", "content": "deploy", "status": "pending"},
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(r["total"], 3);
    assert_eq!(r["completed"], 1);
    assert_eq!(r["in_progress"], 1);

    let r = toolset
        .execute(
            "todo_list", json!({
                "todos": [
                    {"id": "3", "content": "deploy to prod", "status": "in_progress"},
                ],
                "merge": true
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(r["total"], 3);
    let todos = r["todos"].as_array().unwrap();
    let item3 = todos.iter().find(|t| t["id"] == "3").unwrap();
    assert_eq!(item3["content"], "deploy to prod");
    // 只有一�?in_progress，item2 保持 in_progress，item3 被重置为 pending
    assert_eq!(item3["status"], "pending");
    assert_eq!(r["in_progress"], 1);

    let r = toolset
        .execute(
            "todo_list", json!({"todos": [{"id": "1", "content": "done", "status": "completed"}], "merge": true}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    // item2 仍为 in_progress，item1 �?completed
    assert_eq!(r["in_progress"], 1);
    assert_eq!(r["completed"], 1);
}