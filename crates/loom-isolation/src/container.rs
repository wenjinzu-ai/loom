//! 容器级隔离后端
//!
//! 通过 Docker CLI 在独立容器中运行 Agent。
//! 容器级隔离提供文件系统、网络、进程命名空间隔离，安全性仅次于 Wasm。
//!
//! ## 通信协议
//! 与进程后端相同，详见 [`super::protocol`]。
//! 容器通过 stdin/stdout 与 Host 交换 JSON Line。
//!
//! ## Windows + WSL2 支持
//! Docker Desktop for Windows 将守护进程运行在 WSL2 中，`docker.exe` CLI
//! 可在 Windows 原生调用。卷挂载路径自动处理：
//! - Windows 路径 `C:\path\to\dir` → 容器内 `/host/path/to/dir`
//! - 已为 WSL 风格的路径 `/mnt/c/...` 保持不变
//!
//! ## 配置项（AgentSpec.config）
//! - `container_image`: Docker 镜像名（默认 `loom-agent:latest`）
//! - `container_network`: 网络模式（如 `bridge`、`none`、`host`）
//! - `container_volumes`: 卷挂载数组，格式 `"host_path:container_path[:ro]"`
//! - `container_env`: 额外环境变量（JSON 对象）

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

use super::protocol::{encode_spec, GuestEvent, HostCommand};

pub struct ContainerBackend;

impl ContainerBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ContainerBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IsolationBackend for ContainerBackend {
    fn level(&self) -> IsolationLevel {
        IsolationLevel::Container
    }

    async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>> {
        let agent_id = spec.agent_id;
        let container_name = format!("loom-agent-{}", agent_id);
        tracing::info!("[container] spawning agent {} as {}", agent_id, container_name);

        let image = spec
            .config
            .get("container_image")
            .and_then(|v| v.as_str())
            .unwrap_or("loom-agent:latest");

        let encoded_config = encode_spec(&spec)?;

        let mut cmd = tokio::process::Command::new(docker_cli());
        cmd.arg("run");
        cmd.arg("-i");
        cmd.arg("--rm");
        cmd.args(["--name", &container_name]);
        cmd.args(["-e", &format!("LOOM_AGENT_CONFIG={}", encoded_config)]);

        if let Some(network) = spec
            .config
            .get("container_network")
            .and_then(|v| v.as_str())
        {
            cmd.args(["--network", network]);
        } else {
            cmd.args(["--network", "none"]);
        }

        if let Some(env_map) = spec
            .config
            .get("container_env")
            .and_then(|v| v.as_object())
        {
            for (k, v) in env_map {
                if let Some(val) = v.as_str() {
                    cmd.args(["-e", &format!("{}={}", k, val)]);
                }
            }
        }

        if let Some(volumes) = spec
            .config
            .get("container_volumes")
            .and_then(|v| v.as_array())
        {
            for vol in volumes {
                if let Some(s) = vol.as_str() {
                    cmd.args(["-v", &convert_volume_mount(s)]);
                }
            }
        }

        cmd.arg(image);
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| {
            loom_core::LoomError::Other(format!(
                "failed to run docker container '{}': {e} (is Docker installed and running?)",
                image
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open container stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open container stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| loom_core::LoomError::Other("failed to open container stderr".into()))?;

        let (stream_tx, stream_rx) = mpsc::channel::<Result<serde_json::Value>>(256);
        let (output_tx, output_rx) = oneshot::channel::<Result<AgentOutput>>();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let child_arc = Arc::new(Mutex::new(Some(child)));
        let container_name_arc = Arc::new(container_name);

        // 活动状态：stdout 收到任何一行即刷新 last_activity_ts
        let activity = Arc::new(std::sync::Mutex::new(ActivitySummary {
            iterations: 0,
            api_call_count: 0,
            current_tool: None,
            last_activity_ts: Some(chrono::Utc::now()),
        }));
        let activity_for_stdout = activity.clone();

        // stdout 解析任务
        let child_for_stdout = child_arc.clone();
        let cname_for_stdout = container_name_arc.clone();
        let stdout_handle = tokio::spawn(async move {
            let mut output_tx = Some(output_tx);
            let mut reader = BufReader::new(stdout).lines();
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
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
                                    "[container] {} invalid stdout line: {e}",
                                    cname_for_stdout
                                );
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!("[container] {} stdout read error: {e}", cname_for_stdout);
                        break;
                    }
                }
            }
            let exit_status = {
                let mut guard = child_for_stdout.lock().await;
                if let Some(child) = guard.as_mut() {
                    child.wait().await.ok()
                } else {
                    None
                }
            };
            if let Some(tx) = output_tx.take() {
                let _ = tx.send(Err(loom_core::LoomError::Other(format!(
                    "container exited without output, status: {:?}",
                    exit_status
                ))));
            }
        });

        // stderr 转发任务
        let cname_for_stderr = container_name_arc.clone();
        let stderr_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                tracing::debug!("[container-stderr] {}: {}", cname_for_stderr, line);
            }
        });

        // 停止任务：先 docker stop，再清理
        let cname_for_stop = container_name_arc.clone();
        let child_for_stop = child_arc.clone();
        let stop_handle = tokio::spawn(async move {
            let _ = stop_rx.await;
            tracing::info!("[container] stop requested for {}", cname_for_stop);
            // 优雅停止容器
            let _ = tokio::process::Command::new(docker_cli())
                .args(["stop", "-t", "5", &cname_for_stop])
                .output()
                .await;
            // 清理（--rm 会自动处理，但保险起见）
            let _ = tokio::process::Command::new(docker_cli())
                .args(["rm", "-f", &cname_for_stop])
                .output()
                .await;
            // 确保 docker run 进程也退出
            let mut guard = child_for_stop.lock().await;
            if let Some(mut child) = guard.take() {
                let _ = child.kill().await;
            }
        });

        Ok(Box::new(ContainerRuntime {
            agent_id,
            container_name: container_name_arc,
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

/// 返回 Docker CLI 命令名
///
/// 在 Windows 上为 `docker.exe`，其他平台为 `docker`。
fn docker_cli() -> &'static str {
    if cfg!(windows) {
        "docker.exe"
    } else {
        "docker"
    }
}

/// 转换卷挂载路径，适配 Windows + WSL2 Docker
///
/// Docker Desktop for Windows 可识别 Windows 风格路径，但为兼容性，
/// 将 `C:\path` 转换为 `/host/path` 形式（Docker Desktop 的 WSL 绑定挂载约定）。
/// 若路径已是 POSIX 风格（以 `/` 开头）则保持不变。
///
/// 输入格式：`host_path:container_path[:ro]`
fn convert_volume_mount(spec: &str) -> String {
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    if parts.len() < 2 {
        return spec.to_string();
    }
    let host_path = convert_host_path(parts[0]);
    let container_path = parts[1];
    let opts = parts.get(2).map(|o| format!(":{}", o)).unwrap_or_default();
    format!("{}:{}:{}", host_path, container_path, opts)
        .trim_end_matches(':')
        .to_string()
}

/// 将 Windows 主机路径转换为 Docker Desktop 可识别的形式
fn convert_host_path(path: &str) -> String {
    // 已是 POSIX 路径（/mnt/c/... 或 /host/...），保持不变
    if path.starts_with('/') {
        return path.to_string();
    }
    // Windows 路径 C:\path\to\dir → /host/path/to/dir
    if let Some(rest) = path.strip_prefix(r"\\?\") {
        return convert_windows_path(rest);
    }
    if path.len() >= 2 && path.as_bytes()[1] == b':' {
        return convert_windows_path(path);
    }
    path.replace('\\', "/")
}

fn convert_windows_path(path: &str) -> String {
    // 去掉盘符冒号，反斜杠转正斜杠，统一为 /host/... 形式
    let without_drive = if path.len() >= 2 && path.as_bytes()[1] == b':' {
        &path[2..]
    } else {
        path
    };
    let normalized = without_drive.replace('\\', "/");
    let trimmed = normalized.trim_start_matches('/');
    format!("/host/{}", trimmed)
}

pub struct ContainerRuntime {
    #[allow(dead_code)]
    agent_id: Uuid,
    container_name: Arc<String>,
    stdin: Mutex<Option<tokio::process::ChildStdin>>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    stream_rx: Mutex<Option<mpsc::Receiver<Result<serde_json::Value>>>>,
    output_rx: Mutex<Option<oneshot::Receiver<Result<AgentOutput>>>>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    _handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    activity: Arc<std::sync::Mutex<ActivitySummary>>,
}

#[async_trait]
impl AgentRuntime for ContainerRuntime {
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
        // 先尝试发送 stop 指令让容器内 Agent 优雅退出
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
        // 触发容器停止流程
        let mut guard = self.stop_tx.lock().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        let running = check_container_running(&self.container_name).await;
        let child_alive = self
            .child
            .try_lock()
            .map(|g| g.is_some())
            .unwrap_or(false);
        HealthStatus {
            alive: running && child_alive,
            last_heartbeat: Some(chrono::Utc::now()),
            diagnostic: if !running {
                Some(format!("container {} not running", self.container_name))
            } else {
                None
            },
        }
    }

    async fn activity_summary(&self) -> Result<ActivitySummary> {
        let mut summary = self
            .activity
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| loom_core::LoomError::Other("activity state lock poisoned".into()))?;
        // 跨容器后端无法精确感知子容器执行的工具，但若容器存活，
        // 视为可能正在执行工具，使用更长的陈旧阈值，避免长耗时工具被误判为卡死。
        let running = check_container_running(&self.container_name).await;
        if running && summary.current_tool.is_none() {
            summary.current_tool = Some("container".into());
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
            loom_core::LoomError::Other("container task dropped without result".into())
        })?
    }
}

/// 检查容器是否正在运行
async fn check_container_running(name: &str) -> bool {
    let output = match tokio::process::Command::new(docker_cli())
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
        .await
    {
        Ok(o) => o,
        Err(_) => return false,
    };
    if !output.status.success() {
        return false;
    }
    String::from_utf8_lossy(&output.stdout).trim() == "true"
}