//! Loom Isolation — 分级隔离后端
//!
//! 支持协程、进程、容器、WASM 四种隔离级别。
//! 协程级默认，按需升级到更强隔离。
//!
//! 各后端独立实现 `loom_core::IsolationBackend` trait，
//! 由 `IsolationManager` 根据 `IsolationLevel` 分发。

pub mod container;
pub mod coroutine;
pub mod process;
pub mod protocol;
pub mod wasm;

pub use container::{ContainerBackend, ContainerRuntime};
pub use coroutine::{CoroutineBackend, CoroutineRuntime};
pub use process::{ProcessBackend, ProcessRuntime};
pub use wasm::{WasmBackend, WasmRuntime};

use loom_core::{AgentRunner, AgentRuntime, AgentSpec, IsolationBackend, IsolationLevel, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 隔离后端管理器：根据隔离级别选择对应后端
pub struct IsolationManager {
    backends: HashMap<IsolationLevel, Arc<dyn IsolationBackend>>,
    runner_slot: Arc<RwLock<Option<Arc<dyn AgentRunner>>>>,
}

impl IsolationManager {
    pub fn new() -> Self {
        let runner_slot: Arc<RwLock<Option<Arc<dyn AgentRunner>>>> = Arc::new(RwLock::new(None));
        let mut backends: HashMap<IsolationLevel, Arc<dyn IsolationBackend>> = HashMap::new();
        backends.insert(
            IsolationLevel::Coroutine,
            Arc::new(coroutine::CoroutineBackend::new(runner_slot.clone())),
        );
        backends.insert(
            IsolationLevel::Process,
            Arc::new(process::ProcessBackend::new()),
        );
        backends.insert(
            IsolationLevel::Container,
            Arc::new(container::ContainerBackend::new()),
        );
        backends.insert(IsolationLevel::Wasm, Arc::new(wasm::WasmBackend::new()));
        Self {
            backends,
            runner_slot,
        }
    }

    /// 延迟注入 AgentRunner，coroutine 级别后端会用它运行真正的 Agent 循环
    pub fn set_runner(&self, runner: Arc<dyn AgentRunner>) {
        *self.runner_slot.write() = Some(runner);
    }

    pub async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>> {
        let backend = self.backends.get(&spec.isolation).ok_or_else(|| {
            loom_core::LoomError::IsolationNotSupported(format!("{:?}", spec.isolation))
        })?;
        backend.spawn(spec).await
    }
}

impl Default for IsolationManager {
    fn default() -> Self {
        Self::new()
    }
}