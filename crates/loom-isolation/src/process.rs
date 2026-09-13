//! 进程级隔离后端
//!
//! 通过 stdio JSON Line 协议与子进程通信，子进程为独立的 loom-agent 可执行文件。
//! 进程级隔离提供 OS 级别的地址空间隔离，但仍共享同一宿主机。
//!
//! ## 通信协议
//! 详见 [`super::protocol`]。
//!
//! ## 配置项（AgentSpec.config）
//! - `process_binary`: 子进程可执行文件路径，默认为当前可执行文件
//! - `process_args`: 额外命令行参数（字符串数组）

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{
    ActivitySummary, AgentOutput, AgentRuntime, AgentSpec, HealthStatus, IsolationBackend,
    IsolationLevel, Result,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex, oneshot};
use uuid::Uuid;

use super::protocol::{
    encode_spec, load_spec_from_env, parse_config_arg, GuestEvent, HostCommand,
};

pub struct ProcessBackend;

impl ProcessBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ProcessBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IsolationBackend for ProcessBackend {
    fn level(&self) -> IsolationLevel {
        IsolationLevel::Process
    }

    async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>> {
        let agent_id = spec.agent_id;
        tracing::info!("[process] spawning agent {}", agent_id);

        let binary = resolve_binary(&spec.config);
        let encoded_config = encode_spec(&spec)?;

        let mut cmd = tokio::process::Command::new(&binary);
        cmd.arg("--agent-config").arg(&encoded_config);
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        cmd.env("LOOM_AGENT_CONFIG", &encoded_config);

        if let Some(extra_args) = spec
            .config
            .get("process_args")
            .and_then(|v| v.as_array())
        {
            for arg in extra_args {
                if let Some(s) = arg.as_str() {
                    cmd.arg(s);
                }
            }
        }

        let mut child = cmd.spawn().map_err(|e| {
            loom_core::LoomError::Other(format!(
                "failed to spawn process agent '{}': {e}",
                binary.display()
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open child stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open child stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open child stderr".into()))?;

        let (stream_tx, stream_rx) = mpsc::channel::<Result<serde_json::Value>>(256);
        let (output_tx, output_rx) = oneshot::channel::<Result<AgentOutput>>();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let child_arc = Arc::new(Mutex::new(Some(child)));

        // 活动状态：stdout 收到任何一行即刷新 last_activity_ts，
        // 供父端心跳陈旧检测判断子进程是否还在产出
        let activity = Arc::new(std::sync::Mutex::new(ActivitySummary {
            iterations: 0,
            api_call_count: 0,
            current_tool: None,
            last_activity_ts: Some(chrono::Utc::now()),
        }));
        let activity_for_stdout = activity.clone();

        // stdout 解析任务
        let stdout_handle = tokio::spawn(async move {
            let mut output_tx = Some(output_tx);
            let mut reader = BufReader::new(stdout).lines();
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        // 收到任何 stdout 输出即视为活动
                        if let Ok(mut a) = activity_for_stdout.lock() {
                            a.last_activity_ts = Some(chrono::Utc::now());
                            a.iterations = a.iterations.saturating_add(1);
                        }
                        match serde_json::from_str::<GuestEvent>(&line) {
                            Ok(GuestEvent::Stream { payload }) => {
                                let _ = stream_tx.send(Ok(payload)).await;
                            }
                            Ok(GuestEvent::Output { payload }) => {
                                if let Some(tx) = output_tx.take() {
                                    let _ = tx.send(Ok(payload));
                                }
                                break;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "[process] agent {} invalid stdout line: {e}",
                                    agent_id
                                );
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!("[process] agent {} stdout read error: {e}", agent_id);
                        break;
                    }
                }
            }
            // stdout 关闭但未收到 output，说明进程异常退出或被 kill
            if let Some(tx) = output_tx.take() {
                let _ = tx.send(Err(loom_core::LoomError::Other(
                    "process agent exited without output".into(),
                )));
            }
        });

        // stderr 转发任务
        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                tracing::debug!("[process-stderr] agent {}: {}", agent_id, line);
            }
        });

        // 停止监听任务：收到停止信号后，先给 Guest 优雅退出的宽限期，
        // 超时仍未退出则强制 kill
        let child_for_stop = child_arc.clone();
        let stop_handle = tokio::spawn(async move {
            let _ = stop_rx.await;
            tracing::info!("[process] agent {} stop requested", agent_id);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let mut guard = child_for_stop.lock().await;
            if let Some(mut child) = guard.take() {
                tracing::warn!(
                    "[process] agent {} did not exit within grace period, force killing",
                    agent_id
                );
                let _ = child.kill().await;
            }
        });

        Ok(Box::new(ProcessRuntime {
            agent_id,
            stdin: Mutex::new(Some(stdin)),
            child: child_arc,
            stream_rx: Mutex::new(Some(stream_rx)),
            output_rx: Mutex::new(Some(output_rx)),
            stop_tx: Mutex::new(Some(stop_tx)),
            _handles: Mutex::new(vec![stdout_handle, stderr_handle, stop_handle]),
            activity,
        }))
    }
}

/// 解析子进程可执行文件路径，默认为当前可执行文件
fn resolve_binary(config: &serde_json::Value) -> std::path::PathBuf {
    if let Some(path) = config.get("process_binary").and_then(|v| v.as_str()) {
        return std::path::PathBuf::from(path);
    }
    match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => std::path::PathBuf::from("loom-chat"),
    }
}

pub struct ProcessRuntime {
    #[allow(dead_code)]
    agent_id: Uuid,
    stdin: Mutex<Option<tokio::process::ChildStdin>>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    stream_rx: Mutex<Option<mpsc::Receiver<Result<serde_json::Value>>>>,
    output_rx: Mutex<Option<oneshot::Receiver<Result<AgentOutput>>>>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    _handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    activity: Arc<std::sync::Mutex<ActivitySummary>>,
}

#[async_trait]
impl AgentRuntime for ProcessRuntime {
    async fn send(&self, msg: loom_core::AgentMessage) -> Result<()> {
        let cmd = HostCommand::Message { payload: msg };
        let line = serde_json::to_string(&cmd)?;
        let mut guard = self.stdin.lock().await;
        if let Some(stdin) = guard.as_mut() {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
        }
        Ok(())
    }

    async fn send_stream(
        &self,
        _msg: loom_core::AgentMessage,
    ) -> Result<BoxStream<'static, Result<serde_json::Value>>> {
        let rx = {
            let mut guard = self.stream_rx.lock().await;
            guard.take().ok_or(loom_core::LoomError::Other(
                "stream already consumed".into(),
            ))?
        };
        Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|v| (v, rx))
        })))
    }

    async fn stop(&self) -> Result<()> {
        // 先尝试优雅停止（发送 stop 指令）
        {
            let cmd = HostCommand::Stop;
            if let Ok(line) = serde_json::to_string(&cmd) {
                let mut guard = self.stdin.lock().await;
                if let Some(stdin) = guard.as_mut() {
                    let _ = stdin.write_all(line.as_bytes()).await;
                    let _ = stdin.write_all(b"\n").await;
                }
            }
        }
        // 再触发强制停止
        let mut guard = self.stop_tx.lock().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        let alive = {
            let mut guard = self.child.lock().await;
            if let Some(child) = guard.as_mut() {
                match child.try_wait() {
                    Ok(None) => true,
                    Ok(Some(_)) => {
                        *guard = None;
                        false
                    }
                    Err(_) => false,
                }
            } else {
                false
            }
        };
        HealthStatus {
            alive,
            last_heartbeat: Some(chrono::Utc::now()),
            diagnostic: None,
        }
    }

    async fn activity_summary(&self) -> Result<ActivitySummary> {
        let mut summary = self
            .activity
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| loom_core::LoomError::Other("activity state lock poisoned".into()))?;
        // 跨进程后端无法精确感知子进程执行的工具，但若进程存活，
        // 视为可能正在执行工具，使用更长的陈旧阈值（STALE_CYCLES_IN_TOOL），
        // 避免长耗时工具被误判为卡死。
        let alive = {
            let guard = self.child.lock().await;
            guard.is_some()
        };
        if alive && summary.current_tool.is_none() {
            summary.current_tool = Some("process".into());
        }
        Ok(summary)
    }

    async fn wait(&self) -> Result<AgentOutput> {
        let rx = {
            let mut guard = self.output_rx.lock().await;
            guard.take().ok_or(loom_core::LoomError::Other(
                "result already consumed".into(),
            ))?
        };
        rx.await.map_err(|_| {
            loom_core::LoomError::Other("process agent task dropped without result".into())
        })?
    }
}

/// 子进程入口辅助：从命令行或环境变量加载 AgentSpec
///
/// 在子进程 main 中调用，返回 AgentSpec 则以子进程模式运行。
pub fn child_entrypoint() -> Option<AgentSpec> {
    parse_config_arg().or_else(load_spec_from_env)
}