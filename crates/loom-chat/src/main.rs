//! Loom Chat — Agent OS 的 HTTP/SSE 入口
//!
//! 替代原 CLI，提供：
//! - `POST /chat`  流式对话（SSE）
//! - `GET  /agents`  列出运行中 Agent
//! - `POST /agents`  启动 Agent
//! - `GET  /agents/:id`  查询状态
//! - `DELETE /agents/:id`  销毁 Agent
//! - `POST /agents/:id/messages`  向 Agent 发消息
//! - `GET  /capabilities`  列出已注册能力

mod chat;
mod config;
mod guest;
mod state;
mod waker;

use axum::{routing, Router};
use state::AppState;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 若以 --config <base64(AgentSpec)> 启动，则进入 Guest 模式（进程/容器隔离子进程）
    if guest::try_run_guest().await? {
        return Ok(());
    }

    // 加载 .env 文件（如果存在）
    if let Err(e) = dotenvy::dotenv() {
        eprintln!("no .env file found ({e}), using environment/config.toml");
    }

    // 先加载配置（此时 tracing 尚未初始化，配置加载阶段的日志用 eprintln）
    let config = config::AppConfig::load_default()?;

    // 初始化日志：优先级 RUST_LOG 环境变量 > config.toml 的 logging.filter > 默认值
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(config.logging.filter.clone())
    });
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    tracing::info!(
        "config loaded: server={}:{}, llm.model={}, log_filter={}",
        config.server.host,
        config.server.port,
        config.llm.model,
        config.logging.filter
    );

    let state = AppState::build(&config).await?;

    let app = Router::new()
        .route("/chat", routing::post(chat::chat_stream))
        .route(
            "/agents",
            routing::get(chat::list_agents).post(chat::launch_agent),
        )
        .route(
            "/agents/:id",
            routing::get(chat::get_agent).delete(chat::destroy_agent),
        )
        .route("/agents/:id/messages", routing::post(chat::send_message))
        .route("/capabilities", routing::get(chat::list_capabilities))
        .route("/sessions", routing::get(chat::list_sessions))
        .route(
            "/sessions/:id",
            routing::get(chat::get_session)
                .delete(chat::delete_session)
                .patch(chat::rename_session),
        )
        .route("/sessions/:id/resume", routing::post(chat::resume_session))
        .route(
            "/sessions/:id/resume-from-checkpoint",
            routing::post(chat::resume_from_checkpoint_session),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    let host: std::net::IpAddr = config.server.host.parse()?;
    let addr = SocketAddr::from((host, config.server.port));
    tracing::info!("loom-chat listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}