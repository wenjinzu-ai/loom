use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgresql://postgres:postgres@127.0.0.1:5432/loom")
        .await?;

    let tables = [
        "kv_store",
        "sessions",
        "checkpoints",
        "checkpoint_resumes",
        "memories",
        "delegation_tasks",
    ];

    println!("=== 表结构 ===");
    for t in tables {
        let cols: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT column_name, data_type FROM information_schema.columns
               WHERE table_schema='public' AND table_name=$1
               ORDER BY ordinal_position"#,
        )
        .bind(t)
        .fetch_all(&pool)
        .await?;

        if cols.is_empty() {
            println!("\n[{}] (不存在)", t);
        } else {
            println!("\n[{}]", t);
            for (name, ty) in cols {
                println!("  {} {}", name, ty);
            }
        }
    }

    println!("\n=== 数据行数 ===");
    for t in tables {
        let count: Option<i64> = sqlx::query_scalar(&format!("SELECT count(*) FROM {}", t))
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
        println!("{}: {:?} 行", t, count.unwrap_or(-1));
    }

    println!("\n=== sessions 结构化列数据 ===");
    let sessions: Vec<(String, String, String, Option<String>, Option<i32>, Option<i64>)> = sqlx::query_as(
        "SELECT tenant_id, user_id, key, title, message_count, updated_at FROM sessions ORDER BY updated_at DESC LIMIT 5",
    )
    .fetch_all(&pool)
    .await?;
    for (tenant_id, user_id, key, title, mc, ua) in sessions {
        println!("  tenant={}, user={}, key={}, title={:?}, messages={:?}, updated_at={:?}", tenant_id, user_id, key, title, mc, ua);
    }

    println!("\n=== checkpoints 结构化列数据 ===");
    let cps: Vec<(String, String, String, String, Option<String>, Option<i64>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT tenant_id, user_id, key, thread_id, checkpoint_id, step, source, created_at::text FROM checkpoints ORDER BY created_at DESC LIMIT 5",
    )
    .fetch_all(&pool)
    .await?;
    if cps.is_empty() {
        println!("  (无数据)");
    }
    for (tenant_id, user_id, key, tid, cid, step, src, ca) in cps {
        println!("  tenant={}, user={}, key={}, thread_id={:?}, cp_id={:?}, step={:?}, source={:?}, created_at={:?}",
            tenant_id, user_id, key, tid, cid, step, src, ca);
    }

    println!("\n=== delegation_tasks 结构化列数据 ===");
    let dtasks: Vec<(String, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT tenant_id, user_id, key, child_agent_id, status FROM delegation_tasks LIMIT 5",
    )
    .fetch_all(&pool)
    .await?;
    if dtasks.is_empty() {
        println!("  (无数据)");
    }
    for (tenant_id, user_id, key, caid, status) in dtasks {
        println!("  tenant={}, user={}, key={}, child_agent_id={:?}, status={:?}", tenant_id, user_id, key, caid, status);
    }

    Ok(())
}