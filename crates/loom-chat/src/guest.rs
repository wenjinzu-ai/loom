//! Guest 模式：作为进程/容器隔离后端的子进程运行 Agent
//!
//! 当 `loom-chat` 以 `--config <base64(AgentSpec)>` 启动时，
//! 进入 Guest 模式：从 stdin 读取 HostCommand，向 stdout 写入 GuestEvent。
//!
//! ## 协议
//! - stdin：`{"type":"message","payload":{...}}` | `{"type":"stop"}`
//! - stdout：`{"type":"stream","payload":{...}}` | `{"type":"output","payload":{...}}`
//! - stderr：纯文本日志（由 Host 转发到 tracing）

use crate::config::AppConfig;
use crate::state::AppState;
use loom_agent::result::ProgressEvent;
use loom_core::{AgentOutput, AgentSpec};
use loom_isolation::protocol::{decode_spec, parse_config_arg, GuestEvent, HostCommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{mpsc, oneshot};

/// 尝试以 Guest 模式运行。
///
/// 若命令行参数中存在 `--config` 或环境变量 `LOOM_AGENT_CONFIG`，
/// 则进入 Guest 模式并运行 Agent，返回 `Ok(true)`。
/// 否则返回 `Ok(false)`，由调用方继续启动 HTTP 服务器。
pub async fn try_run_guest() -> anyhow::Result<bool> {
    let spec = load_spec()?;
    let Some(spec) = spec else {
        return Ok(false);
    };

    run_guest(spec).await?;
    Ok(true)
}

fn load_spec() -> anyhow::Result<Option<AgentSpec>> {
    if let Some(spec) = parse_config_arg() {
        return Ok(Some(spec));
    }
    if let Ok(val) = std::env::var("LOOM_AGENT_CONFIG") {
        return decode_spec(&val).map(Some).map_err(|e| anyhow::anyhow!(e));
    }
    Ok(None)
}

async fn run_guest(spec: AgentSpec) -> anyhow::Result<()> {
    eprintln!(
        "[guest] agent {} starting (isolation={:?})",
        spec.agent_id, spec.isolation
    );

    let config = AppConfig::load_default()?;
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(config.logging.filter.clone()));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let state = AppState::build(&config).await?;

    let agent_id = spec.agent_id;
    let goal = spec.goal.clone();

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ProgressEvent>();
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let mut stop_tx = Some(stop_tx);

    let agent_loop = if let Some(scope) = spec.scope.clone() {
        (*state.agent_loop).clone().with_scope(scope)
    } else {
        (*state.agent_loop).clone()
    }
    .with_progress_sender(progress_tx);

    let toolsets = spec.toolsets.clone();
    let mut run_handle = tokio::spawn(async move {
        tokio::select! {
            _ = &mut stop_rx => {
                tracing::info!("[guest] agent {} stopped by host", agent_id);
                AgentOutput::failure("stopped by host", 0, 0)
            }
            result = agent_loop.run_goal(&goal, &spec.context, agent_id, spec.parent_agent_id, spec.delegate_depth, &toolsets, &spec.parent_toolsets, None) => {
                match result {
                    Ok(r) => AgentOutput::success_with_meta(
                        r.final_response,
                        r.iterations,
                        r.tool_calls_made,
                        r.tool_names,
                        r.duration_ms,
                        r.tool_trace,
                    ),
                    Err(e) => {
                        tracing::error!("[guest] agent {} failed: {e}", agent_id);
                        AgentOutput::failure(e.to_string(), 0, 0)
                    }
                }
            }
        }
    });

    let stdin = tokio::io::stdin();
    let mut stdin_lines = AsyncBufReader::new(stdin).lines();
    let stdout = tokio::io::stdout();
    let mut stdout = tokio::io::BufWriter::new(stdout);

    loop {
        tokio::select! {
            line = stdin_lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<HostCommand>(&line) {
                            Ok(HostCommand::Stop) => {
                                tracing::info!("[guest] agent {} received stop", agent_id);
                                if let Some(tx) = stop_tx.take() {
                                    let _ = tx.send(());
                                }
                            }
                            Ok(HostCommand::Message { payload }) => {
                                tracing::debug!("[guest] agent {} received message: {:?}", agent_id, payload);
                            }
                            Err(e) => {
                                tracing::warn!("[guest] invalid host command: {e}");
                            }
                        }
                    }
                    Ok(None) => {
                        if let Some(tx) = stop_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("[guest] stdin read error: {e}");
                        if let Some(tx) = stop_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
            Some(progress) = progress_rx.recv() => {
                if let Ok(payload) = serde_json::to_value(&progress) {
                    let event = GuestEvent::Stream { payload };
                    if let Err(e) = write_event(&mut stdout, &event).await {
                        tracing::error!("[guest] failed to write stream event: {e}");
                        break;
                    }
                }
            }
            result = &mut run_handle => {
                let output = match result {
                    Ok(o) => o,
                    Err(e) => {
                        tracing::error!("[guest] agent {} task panicked: {e}", agent_id);
                        AgentOutput::failure(format!("task panic: {e}"), 0, 0)
                    }
                };
                let event = GuestEvent::Output { payload: output };
                if let Err(e) = write_event(&mut stdout, &event).await {
                    tracing::error!("[guest] failed to write output event: {e}");
                }
                break;
            }
        }
    }

    let _ = stdout.flush().await;
    tracing::info!("[guest] agent {} exiting", agent_id);
    // 强制退出，避免 tokio 运行时关闭等待（如 LLM 连接池 keepalive）导致进程延迟退出
    std::process::exit(0);
}

async fn write_event<W: AsyncWriteExt + Unpin>(
    w: &mut tokio::io::BufWriter<W>,
    event: &GuestEvent,
) -> anyhow::Result<()> {
    let line = serde_json::to_string(event)?;
    w.write_all(line.as_bytes()).await?;
    w.write_all(b"\n").await?;
    w.flush().await?;
    Ok(())
}