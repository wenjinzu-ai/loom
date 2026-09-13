//! 终端 / 代码执行工具集
//!
//! - terminal: 执行 shell 命令，支持后台、超时、工作目录
//! - process_manage: 管理后台进程（list/poll/log/wait/kill/write/submit/close）
//! - execute_code: 执行 Python 代码（通过系统 python 解释器）
//! - run_shell: 简单 shell 执行（保留兼容）

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};

/// 后台进程的共享输出缓冲区
#[derive(Default)]
struct OutputBuffer {
    stdout: String,
    stderr: String,
    poll_offset_stdout: usize,
    poll_offset_stderr: usize,
}

impl OutputBuffer {
    fn append_stdout(&mut self, data: &[u8]) {
        self.stdout.push_str(&String::from_utf8_lossy(data));
    }
    fn append_stderr(&mut self, data: &[u8]) {
        self.stderr.push_str(&String::from_utf8_lossy(data));
    }
    fn poll_new(&mut self) -> (String, String) {
        let new_stdout = self.stdout[self.poll_offset_stdout..].to_string();
        let new_stderr = self.stderr[self.poll_offset_stderr..].to_string();
        self.poll_offset_stdout = self.stdout.len();
        self.poll_offset_stderr = self.stderr.len();
        (new_stdout, new_stderr)
    }
}

#[allow(dead_code)]
struct BgProcess {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    command: String,
    workdir: Option<String>,
    started_at: std::time::Instant,
    output: Arc<Mutex<OutputBuffer>>,
    finished: Arc<Mutex<Option<i32>>>,
}

struct TerminalState {
    bg_processes: HashMap<String, BgProcess>,
    next_id: u64,
}

impl TerminalState {
    fn new() -> Self {
        Self {
            bg_processes: HashMap::new(),
            next_id: 1,
        }
    }
}

pub struct CodeToolSet {
    state: Arc<Mutex<TerminalState>>,
}

impl Default for CodeToolSet {
    fn default() -> Self {
        Self::new()
    }
}

impl CodeToolSet {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TerminalState::new())),
        }
    }
}

#[async_trait]
impl ToolSet for CodeToolSet {
    fn name(&self) -> &str {
        "terminal"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec {
                name: "terminal".into(),
                description: "Execute a shell command. Returns stdout, stderr, and exit code. For long-running processes, set background=true to get an ID immediately and poll for results. Commands run in the working directory.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "The shell command to execute"},
                        "timeout": {"type": "integer", "description": "Timeout in seconds (default: 120, max: 600). 0 = no timeout.", "default": 120, "minimum": 0, "maximum": 600},
                        "background": {"type": "boolean", "description": "If true, runs the command in the background and returns immediately with an id. Use process_manage to poll/kill it.", "default": false},
                        "workdir": {"type": "string", "description": "Working directory for the command (default: current working directory)"},
                        "pty": {"type": "boolean", "description": "Run in a pseudo-terminal (best-effort; falls back to normal execution if unsupported)", "default": false}
                    },
                    "required": ["command"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["terminal".into()],
            },
            ToolSpec {
                name: "process_manage".into(),
                description: "Poll, wait on, or kill background terminal processes (from terminal(background=true)). poll: status + new output. log: full output, paged. wait: block until exit or timeout (partial output on timeout). write vs submit: submit appends Enter — use it to answer prompts; write sends raw bytes, no newline. close: EOF stdin. kill: terminate.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["list", "poll", "log", "wait", "kill", "write", "submit", "close"]
                        },
                        "session_id": {
                            "type": "string",
                            "description": "From terminal background output; the process id (e.g. 'bg-1'). Required except for 'list'."
                        },
                        "data": {
                            "type": "string",
                            "description": "Stdin text for write/submit."
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Max seconds for 'wait'.",
                            "minimum": 1
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Log line offset (default: last 200)."
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max log lines.",
                            "minimum": 1
                        }
                    },
                    "required": ["action"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["terminal".into()],
            },
            ToolSpec {
                name: "execute_code".into(),
                description: "Execute Python code and return its stdout. The code runs in a fresh Python interpreter; print() your result to stdout. Variables do not persist between calls.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Python source code to execute"},
                        "timeout": {"type": "integer", "description": "Timeout in seconds (default: 30, max: 300)", "default": 30, "minimum": 1, "maximum": 300},
                        "reset": {"type": "boolean", "description": "Ignored (stateless execution). Kept for API compatibility.", "default": false}
                    },
                    "required": ["code"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["code".into()],
            },
            ToolSpec {
                name: "run_shell".into(),
                description: "执行 Shell 命令，返回 stdout/stderr/exit_code".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"command": {"type": "string"}},
                    "required": ["command"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["code".into()],
            },
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "terminal" => self.terminal(&args).await,
            "process_manage" => self.process_manage(&args).await,
            "execute_code" => self.execute_code(&args).await,
            "run_shell" => self.run_shell(&args).await,
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

impl CodeToolSet {
    async fn terminal(&self, args: &Value) -> Result<Value> {
        let command = args["command"].as_str().unwrap_or("");
        if command.is_empty() {
            return Err(loom_core::LoomError::Other(
                "terminal: 'command' is required".into(),
            ));
        }

        // 如果 command 是后台进程 id，则轮询其状态（兼容旧用法）
        if command.starts_with("bg-") {
            return self
                .process_manage(&json!({
                    "action": "poll",
                    "session_id": command
                }))
                .await;
        }

        let timeout = args["timeout"].as_u64().unwrap_or(120).min(600);
        let background = args["background"].as_bool().unwrap_or(false);
        let workdir = args["workdir"].as_str().map(|s| s.to_string());
        let _pty = args["pty"].as_bool().unwrap_or(false);

        if background {
            return self.spawn_background(command, workdir);
        }

        self.run_foreground(command, workdir, timeout).await
    }

    async fn process_manage(&self, args: &Value) -> Result<Value> {
        let action = args["action"].as_str().unwrap_or("");

        if action == "list" {
            return self.list_processes();
        }

        let session_id = args["session_id"].as_str().unwrap_or("").to_string();
        if session_id.is_empty() {
            return Err(loom_core::LoomError::Other(format!(
                "process_manage: session_id is required for '{action}'"
            )));
        }

        match action {
            "poll" => self.poll_process(&session_id).await,
            "log" => self.log_process(&session_id, args),
            "wait" => self.wait_process(&session_id, args).await,
            "kill" => self.kill_process(&session_id).await,
            "write" => self.write_stdin(&session_id, args, false).await,
            "submit" => self.write_stdin(&session_id, args, true).await,
            "close" => self.close_stdin(&session_id).await,
            _ => Err(loom_core::LoomError::Other(format!(
                "process_manage: unknown action '{action}'. Use: list, poll, log, wait, kill, write, submit, close"
            ))),
        }
    }

    fn list_processes(&self) -> Result<Value> {
        let state = self.state.lock();
        let processes: Vec<Value> = state
            .bg_processes
            .iter()
            .map(|(id, proc)| {
                let finished = proc.finished.lock();
                let status = if finished.is_some() {
                    "completed"
                } else {
                    "running"
                };
                json!({
                    "id": id,
                    "command": proc.command,
                    "status": status,
                    "started_at_secs": proc.started_at.elapsed().as_secs(),
                })
            })
            .collect();
        Ok(json!({
            "processes": processes,
            "count": processes.len(),
        }))
    }

    async fn poll_process(&self, id: &str) -> Result<Value> {
        let (output_snapshot, finished, command) = {
            let mut state = self.state.lock();
            let proc = match state.bg_processes.get_mut(id) {
                Some(p) => p,
                None => {
                    return Ok(json!({
                        "id": id,
                        "status": "not_found",
                        "error": "No background process with this id.",
                    }));
                }
            };

            // 检查进程是否完成
            if proc.finished.lock().is_none() {
                if let Some(child) = proc.child.as_mut() {
                    if let Ok(Some(status)) = child.try_wait() {
                        *proc.finished.lock() = Some(status.code().unwrap_or(-1));
                        proc.child = None;
                    }
                }
            }

            let finished = *proc.finished.lock();
            let (new_stdout, new_stderr) = proc.output.lock().poll_new();
            let command = proc.command.clone();

            // 如果完成且已收割，移除进程
            if finished.is_some() && proc.child.is_none() {
                state.bg_processes.remove(id);
            }

            ((new_stdout, new_stderr), finished, command)
        };

        let (new_stdout, new_stderr) = output_snapshot;
        match finished {
            Some(code) => Ok(json!({
                "id": id,
                "status": "completed",
                "command": command,
                "stdout": new_stdout,
                "stderr": new_stderr,
                "exit_code": code,
            })),
            None => Ok(json!({
                "id": id,
                "status": "running",
                "command": command,
                "stdout": new_stdout,
                "stderr": new_stderr,
                "message": "Process still running.",
            })),
        }
    }

    fn log_process(&self, id: &str, args: &Value) -> Result<Value> {
        let state = self.state.lock();
        let proc = match state.bg_processes.get(id) {
            Some(p) => p,
            None => {
                return Ok(json!({
                    "id": id,
                    "status": "not_found",
                    "error": "No background process with this id.",
                }));
            }
        };

        let output = proc.output.lock();
        let combined = format!("{}{}", output.stdout, output.stderr);
        let lines: Vec<&str> = combined.lines().collect();

        let limit = args["limit"].as_u64().unwrap_or(200) as usize;
        let total = lines.len();
        let offset = match args["offset"].as_u64() {
            Some(o) => o as usize,
            None => total.saturating_sub(limit),
        };

        let end = (offset + limit).min(total);
        let selected: Vec<&str> = if offset < total {
            lines[offset..end].to_vec()
        } else {
            Vec::new()
        };

        let finished = proc.finished.lock().is_some();
        Ok(json!({
            "id": id,
            "status": if finished { "completed" } else { "running" },
            "lines": selected,
            "offset": offset,
            "limit": limit,
            "total_lines": total,
        }))
    }

    async fn wait_process(&self, id: &str, args: &Value) -> Result<Value> {
        let timeout = args["timeout"].as_u64().unwrap_or(60).min(600);

        // 先快速检查是否已完成
        let already_finished = {
            let state = self.state.lock();
            state
                .bg_processes
                .get(id)
                .map(|proc| proc.finished.lock().is_some())
        };
        if already_finished == Some(true) {
            return self.poll_process(id).await;
        }
        if already_finished.is_none() {
            return Ok(json!({
                "id": id,
                "status": "not_found",
                "error": "No background process with this id.",
            }));
        }

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);
        loop {
            {
                let mut state = self.state.lock();
                if let Some(proc) = state.bg_processes.get_mut(id) {
                    if proc.finished.lock().is_none() {
                        if let Some(child) = proc.child.as_mut() {
                            if let Ok(Some(status)) = child.try_wait() {
                                *proc.finished.lock() = Some(status.code().unwrap_or(-1));
                                proc.child = None;
                            }
                        }
                    }
                    if proc.finished.lock().is_some() {
                        state.bg_processes.remove(id);
                        break;
                    }
                } else {
                    break;
                }
            }

            if tokio::time::Instant::now() >= deadline {
                // 超时，返回当前输出
                let (new_stdout, new_stderr) = {
                    let state = self.state.lock();
                    match state.bg_processes.get(id) {
                        Some(proc) => proc.output.lock().poll_new(),
                        None => (String::new(), String::new()),
                    }
                };
                return Ok(json!({
                    "id": id,
                    "status": "running",
                    "stdout": new_stdout,
                    "stderr": new_stderr,
                    "timed_out": true,
                    "message": format!("Wait timed out after {timeout}s. Process still running."),
                }));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        self.poll_process(id).await
    }

    async fn kill_process(&self, id: &str) -> Result<Value> {
        let (child_handle, finished) = {
            let mut state = self.state.lock();
            let proc = match state.bg_processes.get_mut(id) {
                Some(p) => p,
                None => {
                    return Ok(json!({
                        "id": id,
                        "status": "not_found",
                        "error": "No background process with this id.",
                    }));
                }
            };
            let finished_val = *proc.finished.lock();
            (proc.child.take(), finished_val)
        };

        if let Some(mut child) = child_handle {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }

        let output = {
            let state = self.state.lock();
            match state.bg_processes.get(id) {
                Some(proc) => {
                    *proc.finished.lock() = Some(-1);
                    proc.output.lock().poll_new()
                }
                None => (String::new(), String::new()),
            }
        };

        let mut state = self.state.lock();
        state.bg_processes.remove(id);

        Ok(json!({
            "id": id,
            "status": "killed",
            "stdout": output.0,
            "stderr": output.1,
            "exit_code": -1,
            "already_finished": finished.is_some(),
        }))
    }

    async fn write_stdin(&self, id: &str, args: &Value, append_newline: bool) -> Result<Value> {
        let data = args["data"].as_str().unwrap_or("").to_string();

        let mut stdin = {
            let mut state = self.state.lock();
            let proc = match state.bg_processes.get_mut(id) {
                Some(p) => p,
                None => {
                    return Ok(json!({
                        "id": id,
                        "status": "not_found",
                        "error": "No background process with this id.",
                    }));
                }
            };
            match proc.stdin.take() {
                Some(s) => s,
                None => {
                    return Ok(json!({
                        "id": id,
                        "status": "error",
                        "error": "Process has no stdin pipe (stdin may already be closed).",
                    }));
                }
            }
        };

        let write_data = if append_newline {
            format!("{data}\n")
        } else {
            data.clone()
        };

        let result = stdin.write_all(write_data.as_bytes()).await;
        let _ = stdin.flush().await;

        // 把 stdin 还回去（如果还能用）
        {
            let mut state = self.state.lock();
            if let Some(proc) = state.bg_processes.get_mut(id) {
                proc.stdin = Some(stdin);
            }
        }

        match result {
            Ok(_) => Ok(json!({
                "id": id,
                "status": "ok",
                "written": write_data.len(),
            })),
            Err(e) => Ok(json!({
                "id": id,
                "status": "error",
                "error": format!("Failed to write to stdin: {e}"),
            })),
        }
    }

    async fn close_stdin(&self, id: &str) -> Result<Value> {
        let stdin = {
            let mut state = self.state.lock();
            let proc = match state.bg_processes.get_mut(id) {
                Some(p) => p,
                None => {
                    return Ok(json!({
                        "id": id,
                        "status": "not_found",
                        "error": "No background process with this id.",
                    }));
                }
            };
            proc.stdin.take()
        };

        match stdin {
            Some(_) => Ok(json!({
                "id": id,
                "status": "ok",
                "message": "stdin closed (EOF sent).",
            })),
            None => Ok(json!({
                "id": id,
                "status": "error",
                "error": "stdin was already closed.",
            })),
        }
    }

    fn spawn_background(&self, command: &str, workdir: Option<String>) -> Result<Value> {
        let mut cmd = build_shell_command(command);
        if let Some(ref wd) = workdir {
            cmd.current_dir(wd);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| loom_core::LoomError::Other(format!("terminal spawn failed: {e}")))?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();

        let output = Arc::new(Mutex::new(OutputBuffer::default()));
        let finished = Arc::new(Mutex::new(None));

        // 启动 stdout 捕获任务
        if let Some(mut out) = stdout {
            let output_clone = output.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match out.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => output_clone.lock().append_stdout(&buf[..n]),
                        Err(_) => break,
                    }
                }
            });
        }

        // 启动 stderr 捕获任务
        if let Some(mut err) = stderr {
            let output_clone = output.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match err.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => output_clone.lock().append_stderr(&buf[..n]),
                        Err(_) => break,
                    }
                }
            });
        }

        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id += 1;
        let bg_id = format!("bg-{id}");
        state.bg_processes.insert(
            bg_id.clone(),
            BgProcess {
                child: Some(child),
                stdin,
                command: command.to_string(),
                workdir,
                started_at: std::time::Instant::now(),
                output,
                finished,
            },
        );

        Ok(json!({
            "id": bg_id,
            "command": command,
            "background": true,
            "status": "running",
            "message": "Command started in background. Use process_manage with action='poll' and session_id to get output."
        }))
    }

    async fn run_foreground(
        &self,
        command: &str,
        workdir: Option<String>,
        timeout_secs: u64,
    ) -> Result<Value> {
        let mut cmd = build_shell_command(command);
        if let Some(ref wd) = workdir {
            cmd.current_dir(wd);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .map_err(|e| loom_core::LoomError::Other(format!("terminal spawn failed: {e}")))?;

        let output = if timeout_secs > 0 {
            let timeout = std::time::Duration::from_secs(timeout_secs);
            match tokio::time::timeout(timeout, child.wait_with_output()).await {
                Ok(result) => result.map_err(|e| {
                    loom_core::LoomError::Other(format!("terminal wait failed: {e}"))
                })?,
                Err(_) => {
                    return Ok(json!({
                        "stdout": "",
                        "stderr": "",
                        "exit_code": -1,
                        "timed_out": true,
                        "error": format!("Command timed out after {timeout_secs}s"),
                    }));
                }
            }
        } else {
            child
                .wait_with_output()
                .await
                .map_err(|e| loom_core::LoomError::Other(format!("terminal wait failed: {e}")))?
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        Ok(json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
        }))
    }

    async fn execute_code(&self, args: &Value) -> Result<Value> {
        let code = args["code"].as_str().unwrap_or("");
        if code.is_empty() {
            return Err(loom_core::LoomError::Other(
                "execute_code: 'code' is required".into(),
            ));
        }
        let timeout = args["timeout"].as_u64().unwrap_or(30).min(300);

        let python = find_python();
        if python.is_none() {
            return Err(loom_core::LoomError::Other(
                "execute_code: Python interpreter not found. Install Python and ensure it's on PATH.".into(),
            ));
        }
        let python = python.unwrap();

        let mut cmd = Command::new(&python);
        cmd.arg("-c").arg(code);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .map_err(|e| loom_core::LoomError::Other(format!("execute_code spawn failed: {e}")))?;

        let timeout_dur = std::time::Duration::from_secs(timeout);
        let output = match tokio::time::timeout(timeout_dur, child.wait_with_output()).await {
            Ok(result) => result.map_err(|e| {
                loom_core::LoomError::Other(format!("execute_code wait failed: {e}"))
            })?,
            Err(_) => {
                return Ok(json!({
                    "stdout": "",
                    "stderr": "",
                    "exit_code": -1,
                    "timed_out": true,
                    "error": format!("Python code timed out after {timeout}s"),
                }));
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);

        Ok(json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
        }))
    }

    async fn run_shell(&self, args: &Value) -> Result<Value> {
        let command = args["command"].as_str().unwrap_or("");
        if command.is_empty() {
            return Err(loom_core::LoomError::Other(
                "run_shell: command is required".into(),
            ));
        }
        let output = if cfg!(target_os = "windows") {
            Command::new("cmd").args(["/C", command]).output().await
        } else {
            Command::new("sh").args(["-c", command]).output().await
        };
        match output {
            Ok(out) => {
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                let stderr = String::from_utf8_lossy(&out.stderr).to_string();
                let exit_code = out.status.code().unwrap_or(-1);
                Ok(json!({
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code
                }))
            }
            Err(e) => Err(loom_core::LoomError::Other(format!(
                "run_shell failed: {e}"
            ))),
        }
    }
}

fn build_shell_command(command: &str) -> Command {
    if cfg!(target_os = "windows") {
        let mut cmd = Command::new("cmd");
        cmd.args(["/C", command]);
        cmd
    } else {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", command]);
        cmd
    }
}

fn find_python() -> Option<String> {
    for candidate in &["python3", "python"] {
        if which(candidate).is_some() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn which(name: &str) -> Option<String> {
    let path_var = std::env::var("PATH").ok()?;
    let separator = if cfg!(target_os = "windows") {
        ";"
    } else {
        ":"
    };
    for dir in path_var.split(separator) {
        let exe = if cfg!(target_os = "windows") {
            format!("{dir}\\{name}.exe")
        } else {
            format!("{dir}/{name}")
        };
        if std::path::Path::new(&exe).exists() {
            return Some(exe);
        }
    }
    None
}