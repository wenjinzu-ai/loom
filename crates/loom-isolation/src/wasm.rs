//! Wasm 沙箱隔离后端
//!
//! 使用 wasmtime 运行 Wasm 字节码，提供强隔离沙箱。
//! Wasm 是字节码级别的隔离，Guest 无法直接访问 Host 内存，安全性最高。
//!
//! ## Guest → Host 协议
//!
//! Guest 模块需导出：
//! - `memory`：线性内存
//! - `alloc(len: i32) -> i32`：分配内存，返回指针
//! - `run_agent(goal_ptr, goal_len, ctx_ptr, ctx_len) -> i32`：入口函数，返回 0 表示成功
//!
//! Host 向 Guest 提供的导入函数（`env` 模块）：
//! - `log(level: i32, ptr: i32, len: i32)`：日志输出（0=debug,1=info,2=warn,3=error）
//! - `call_tool(name_ptr, name_len, args_ptr, args_len) -> i64`：调用工具，返回值高32位=结果长度，低32位=状态码(0成功)
//! - `read_tool_result(buf_ptr, buf_len) -> i32`：读取上次工具调用结果，返回实际写入字节数
//! - `set_output(ptr, len)`：设置 Agent 最终输出文本

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{
    ActivitySummary, AgentOutput, AgentRuntime, AgentSpec, HealthStatus, IsolationBackend,
    IsolationLevel, Result,
};
use std::sync::Arc;
use uuid::Uuid;
use wasmtime::{Caller, Engine, Instance, Linker, Module, Store, Trap, TypedFunc};

/// Host 共享状态，在 Wasm 导入函数间传递
struct HostState {
    #[allow(dead_code)]
    engine: Engine,
    memory: Option<wasmtime::Memory>,
    /// 上次工具调用的结果，供 read_tool_result 读取
    last_tool_result: Option<Vec<u8>>,
    /// 最终输出文本
    output: String,
    /// 工具调用计数
    tool_calls: usize,
    /// 调用过的工具名
    tool_names: Vec<String>,
    /// 迭代轮数（简单计数）
    iterations: usize,
}

impl HostState {
    /// 从 Wasm 线性内存读取字符串
    fn read_str(&self, caller: &Caller<'_, Self>, ptr: i32, len: i32) -> Option<String> {
        let mem = self.memory?;
        let data = mem.data(caller);
        let start = ptr as usize;
        let end = start + len as usize;
        if end > data.len() {
            return None;
        }
        String::from_utf8(data[start..end].to_vec()).ok()
    }
}

/// Wasm 隔离后端
pub struct WasmBackend {
    engine: Engine,
}

impl WasmBackend {
    pub fn new() -> Self {
        let mut config = wasmtime::Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("failed to create wasmtime engine");
        Self { engine }
    }
}

impl Default for WasmBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IsolationBackend for WasmBackend {
    fn level(&self) -> IsolationLevel {
        IsolationLevel::Wasm
    }

    async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>> {
        let agent_id = spec.agent_id;
        tracing::info!("[wasm] spawning agent {}", agent_id);

        let wasm_bytes = extract_wasm_bytes(&spec.config)?;
        tracing::debug!(
            "[wasm] module loaded: bytes={}, agent_id={}",
            wasm_bytes.len(),
            agent_id
        );

        let engine = self.engine.clone();
        let goal = spec.goal.clone();
        let context = spec.context.clone();

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<AgentOutput>>();

        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let stop_engine = engine_clone.clone();
            tokio::select! {
                _ = stop_rx => {
                    tracing::info!("[wasm] agent {} stop requested, interrupting", agent_id);
                    stop_engine.increment_epoch();
                }
            }
        });

        let engine_for_run = engine.clone();
        let activity = Arc::new(std::sync::Mutex::new(ActivitySummary {
            iterations: 0,
            api_call_count: 0,
            current_tool: None,
            last_activity_ts: Some(chrono::Utc::now()),
        }));
        let activity_for_run = activity.clone();
        let run_handle = tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                if let Ok(mut a) = activity_for_run.lock() {
                    a.last_activity_ts = Some(chrono::Utc::now());
                }
                run_wasm_agent(engine_for_run, wasm_bytes, goal, context)
            })
            .await
            .unwrap_or_else(|e| Err(loom_core::LoomError::Other(format!("wasm task join error: {e}"))));
            let _ = result_tx.send(result);
        });

        Ok(Box::new(WasmRuntime {
            agent_id,
            stop_tx: parking_lot::Mutex::new(Some(stop_tx)),
            result_rx: parking_lot::Mutex::new(Some(result_rx)),
            _handles: parking_lot::Mutex::new(vec![handle, run_handle]),
            activity,
        }))
    }
}

/// 从 AgentSpec.config 中提取 Wasm 字节码
///
/// 支持两种方式：
/// - `wasm_path`: 文件路径
/// - `wasm_base64`: base64 编码的内联字节码
fn extract_wasm_bytes(config: &serde_json::Value) -> Result<Vec<u8>> {
    if let Some(path) = config.get("wasm_path").and_then(|v| v.as_str()) {
        let bytes = std::fs::read(path).map_err(|e| {
            loom_core::LoomError::Other(format!("failed to read wasm file {path}: {e}"))
        })?;
        return Ok(bytes);
    }

    if let Some(b64) = config.get("wasm_base64").and_then(|v| v.as_str()) {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| loom_core::LoomError::Other(format!("invalid wasm_base64: {e}")))?;
        return Ok(bytes);
    }

    Err(loom_core::LoomError::Other(
        "wasm backend requires either 'wasm_path' or 'wasm_base64' in config".into(),
    ))
}

/// 执行 Wasm Agent
fn run_wasm_agent(
    engine: Engine,
    wasm_bytes: Vec<u8>,
    goal: String,
    context: String,
) -> Result<AgentOutput> {
    let start = std::time::Instant::now();

    let module = Module::new(&engine, &wasm_bytes)
        .map_err(|e| loom_core::LoomError::Other(format!("wasm compile error: {e}")))?;

    let mut store: Store<HostState> = Store::new(
        &engine,
        HostState {
            engine: engine.clone(),
            memory: None,
            last_tool_result: None,
            output: String::new(),
            tool_calls: 0,
            tool_names: Vec::new(),
            iterations: 0,
        },
    );
    store.set_epoch_deadline(1);

    let mut linker: Linker<HostState> = Linker::new(&engine);

    // env.log(level, ptr, len)
    linker
        .func_wrap(
            "env",
            "log",
            |caller: Caller<'_, HostState>, level: i32, ptr: i32, len: i32| {
                let msg = caller
                    .data()
                    .read_str(&caller, ptr, len)
                    .unwrap_or_default();
                match level {
                    0 => tracing::debug!("[wasm] {}", msg),
                    1 => tracing::info!("[wasm] {}", msg),
                    2 => tracing::warn!("[wasm] {}", msg),
                    _ => tracing::error!("[wasm] {}", msg),
                }
            },
        )
        .map_err(|e| loom_core::LoomError::Other(format!("link log failed: {e}")))?;

    // env.call_tool(name_ptr, name_len, args_ptr, args_len) -> i64
    // 返回值: 高32位 = 结果长度, 低32位 = 状态码 (0=成功, 非0=错误)
    linker
        .func_wrap(
            "env",
            "call_tool",
            |mut caller: Caller<'_, HostState>,
             name_ptr: i32,
             name_len: i32,
             args_ptr: i32,
             args_len: i32|
             -> i64 {
                let name = caller
                    .data()
                    .read_str(&caller, name_ptr, name_len)
                    .unwrap_or_default();
                let args = caller
                    .data()
                    .read_str(&caller, args_ptr, args_len)
                    .unwrap_or_default();

                caller.data_mut().tool_calls += 1;
                caller.data_mut().tool_names.push(name.clone());
                caller.data_mut().iterations += 1;

                tracing::debug!(
                    "[wasm] call_tool: name={}, args={}",
                    name,
                    if args.len() > 200 {
                        format!("{}...", &args[..200])
                    } else {
                        args.clone()
                    }
                );

                let result = dispatch_wasm_tool(&name, &args);
                let status: i32 = if result.is_ok() { 0 } else { 1 };
                let result_bytes = result.unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
                let result_len = result_bytes.len() as i32;

                caller.data_mut().last_tool_result = Some(result_bytes.into_bytes());

                ((result_len as i64) << 32) | (status as i64 & 0xFFFF_FFFF)
            },
        )
        .map_err(|e| loom_core::LoomError::Other(format!("link call_tool failed: {e}")))?;

    // env.read_tool_result(buf_ptr, buf_len) -> i32
    // 将上次工具调用结果写入 buf，返回实际写入字节数
    linker
        .func_wrap(
            "env",
            "read_tool_result",
            |mut caller: Caller<'_, HostState>, buf_ptr: i32, buf_len: i32| -> i32 {
                let result = match caller.data().last_tool_result.clone() {
                    Some(r) => r,
                    None => return 0,
                };
                let mem = match caller.data().memory {
                    Some(m) => m,
                    None => return -1,
                };
                let write_len = std::cmp::min(result.len(), buf_len as usize);
                let data = mem.data_mut(&mut caller);
                let start = buf_ptr as usize;
                let end = start + write_len;
                if end > data.len() {
                    return -1;
                }
                data[start..end].copy_from_slice(&result[..write_len]);
                write_len as i32
            },
        )
        .map_err(|e| loom_core::LoomError::Other(format!("link read_tool_result failed: {e}")))?;

    // env.set_output(ptr, len)
    linker
        .func_wrap(
            "env",
            "set_output",
            |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| {
                let output = caller
                    .data()
                    .read_str(&caller, ptr, len)
                    .unwrap_or_default();
                caller.data_mut().output = output;
            },
        )
        .map_err(|e| loom_core::LoomError::Other(format!("link set_output failed: {e}")))?;

    let instance: Instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| loom_core::LoomError::Other(format!("wasm instantiate error: {e}")))?;

    // 获取 memory 导出
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| loom_core::LoomError::Other("wasm module must export 'memory'".into()))?;
    store.data_mut().memory = Some(memory);

    // 获取 alloc 导出函数
    let alloc: TypedFunc<i32, i32> = instance
        .get_typed_func(&mut store, "alloc")
        .map_err(|e| loom_core::LoomError::Other(format!("wasm module must export 'alloc': {e}")))?;

    // 获取 run_agent 导出函数
    let run_agent: TypedFunc<(i32, i32, i32, i32), i32> = instance
        .get_typed_func(&mut store, "run_agent")
        .map_err(|e| {
            loom_core::LoomError::Other(format!("wasm module must export 'run_agent': {e}"))
        })?;

    // 将 goal 和 context 写入 Wasm 内存
    let goal_bytes = goal.into_bytes();
    let goal_ptr = alloc
        .call(&mut store, goal_bytes.len() as i32)
        .map_err(|e| loom_core::LoomError::Other(format!("alloc goal failed: {e}")))?;
    memory
        .write(&mut store, goal_ptr as usize, &goal_bytes)
        .map_err(|e| loom_core::LoomError::Other(format!("write goal failed: {e}")))?;

    let ctx_bytes = context.into_bytes();
    let ctx_ptr = alloc
        .call(&mut store, ctx_bytes.len() as i32)
        .map_err(|e| loom_core::LoomError::Other(format!("alloc context failed: {e}")))?;
    memory
        .write(&mut store, ctx_ptr as usize, &ctx_bytes)
        .map_err(|e| loom_core::LoomError::Other(format!("write context failed: {e}")))?;

    tracing::debug!("[wasm] calling run_agent");
    let ret = run_agent
        .call(
            &mut store,
            (goal_ptr, goal_bytes.len() as i32, ctx_ptr, ctx_bytes.len() as i32),
        )
        .map_err(|e| {
            if let Some(trap) = e.downcast_ref::<Trap>() {
                if trap.to_string().contains("epoch deadline") {
                    return loom_core::LoomError::Other("wasm agent interrupted by stop".into());
                }
            }
            loom_core::LoomError::Other(format!("wasm run_agent error: {e}"))
        })?;

    let state = store.into_data();
    let duration_ms = start.elapsed().as_millis() as u64;

    if ret != 0 {
        tracing::warn!("[wasm] run_agent returned non-zero: {}", ret);
        return Ok(AgentOutput::failure(
            format!("wasm run_agent returned code {ret}"),
            state.iterations,
            state.tool_calls,
        ));
    }

    tracing::info!(
        "[wasm] agent finished: iterations={}, tool_calls={}, output_len={}, duration_ms={}",
        state.iterations,
        state.tool_calls,
        state.output.len(),
        duration_ms
    );

    Ok(AgentOutput::success_with_meta(
        state.output,
        state.iterations,
        state.tool_calls,
        state.tool_names,
        duration_ms,
        None,
    ))
}

/// Wasm 工具调用分发（当前为骨架，返回 mock 结果）
///
/// 实际项目中应通过 CapabilityRegistry 查找并执行工具，
/// 此处保持与 Coroutine 后端一致的工具集接口。
fn dispatch_wasm_tool(name: &str, args: &str) -> std::result::Result<String, String> {
    match name {
        "echo" => Ok(format!("{{\"echoed\":{}}}", args)),
        "noop" => Ok("{}".to_string()),
        _ => {
            tracing::warn!("[wasm] unknown tool called: {}", name);
            Err(format!("unknown tool: {name}"))
        }
    }
}

pub struct WasmRuntime {
    agent_id: Uuid,
    stop_tx: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    result_rx: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<Result<AgentOutput>>>>,
    _handles: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    activity: Arc<std::sync::Mutex<ActivitySummary>>,
}

#[async_trait]
impl AgentRuntime for WasmRuntime {
    async fn send(&self, _msg: loom_core::AgentMessage) -> Result<()> {
        tracing::warn!(
            "[wasm] send_message not supported for wasm agent {}, ignoring",
            self.agent_id
        );
        Ok(())
    }

    async fn send_stream(
        &self,
        _msg: loom_core::AgentMessage,
    ) -> Result<BoxStream<'static, Result<serde_json::Value>>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.stop_tx.lock();
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
            tracing::info!("[wasm] stop signal sent for agent {}", self.agent_id);
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        let alive = self.result_rx.lock().is_some();
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
        // Wasm 后端无法精确感知执行的工具，但若 result_rx 仍存在（未完成），
        // 视为可能正在执行工具，使用更长的陈旧阈值，避免长耗时工具被误判为卡死。
        let running = self.result_rx.lock().is_some();
        if running && summary.current_tool.is_none() {
            summary.current_tool = Some("wasm".into());
        }
        Ok(summary)
    }

    async fn wait(&self) -> Result<AgentOutput> {
        let rx = {
            let mut guard = self.result_rx.lock();
            guard.take().ok_or(loom_core::LoomError::Other(
                "result already consumed".into(),
            ))?
        };
        rx.await
            .map_err(|_| loom_core::LoomError::Other("wasm agent task dropped without result".into()))?
    }
}