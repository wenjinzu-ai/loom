use crate::config::AgentLoopConfig;
use loom_core::{AgentLifecycleManager, JsonKeyValueStore};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use registry::BackgroundTaskRegistry;

mod background;
mod execute;
mod loops;
mod persistence;
mod registry;
mod schema;
mod spawn_args;
mod subagent;
mod types;

pub use schema::{
    append_output_contract, build_retry_message, coerce_output_schema, extract_json_candidate,
    validate_output, MAX_SCHEMA_RETRIES,
};
pub use types::{
    ActivitySnapshot, BackgroundTask, BackgroundTaskStatus, DeliveryState, SessionWaker,
    MAX_DELIVERY_ATTEMPTS, PERSISTENCE_NAMESPACE,
};

/// 委派管理器：封装 spawn_agent 工具的全部委派逻辑
#[derive(Clone)]
pub struct DelegationManager {
    lifecycle: Arc<dyn AgentLifecycleManager>,
    config: AgentLoopConfig,
    child_semaphore: Arc<Semaphore>,
    /// 后台任务注册表（批次管理 + 结果回注 + 实时转录）
    registry: BackgroundTaskRegistry,
    /// 持久化存储（可选）：支持 PostgreSQL / 内存，用于进程重启后恢复未完成任务
    /// 使用共享锁，因为陈旧检测守护任务在 new 中启动，早于 with_persistence 设置
    persistence: Arc<Mutex<Option<Arc<dyn JsonKeyValueStore>>>>,
    /// 会话唤醒器（推模式投递回调）
    session_waker: Arc<Mutex<Option<Arc<dyn types::SessionWaker>>>>,
    /// 当前进程实例 ID（UUID，崩溃恢复时判断任务归属）
    instance_id: String,
    /// 关闭信号：通知后台守护循环（stale_monitor / completion_watcher）优雅退出
    shutdown_token: CancellationToken,
}

impl DelegationManager {
    pub fn new(lifecycle: Arc<dyn AgentLifecycleManager>, config: AgentLoopConfig) -> Self {
        let child_semaphore = Arc::new(Semaphore::new(config.max_concurrent_children));
        let registry = BackgroundTaskRegistry::new();
        let persistence = Arc::new(Mutex::new(None));
        let session_waker = Arc::new(Mutex::new(None));
        let shutdown_token = CancellationToken::new();

        // P1: 启动后台陈旧检测守护任务
        let monitor_lifecycle = lifecycle.clone();
        let monitor_registry = registry.clone();
        let monitor_persistence = persistence.clone();
        let monitor_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            loops::stale_monitor_loop(
                monitor_lifecycle,
                monitor_registry,
                monitor_persistence,
                monitor_shutdown,
            )
            .await;
        });

        // P2: 启动 completion watcher（推模式投递）
        let watcher_registry = registry.clone();
        let watcher_waker = session_waker.clone();
        let watcher_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            loops::completion_watcher_loop(watcher_registry, watcher_waker, watcher_shutdown)
                .await;
        });

        Self {
            lifecycle,
            config,
            child_semaphore,
            registry,
            persistence,
            session_waker,
            instance_id: Uuid::new_v4().to_string(),
            shutdown_token,
        }
    }

    /// 设置持久化存储（启用崩溃恢复）
    pub fn with_persistence(&self, kv: Arc<dyn JsonKeyValueStore>) {
        *self.persistence.lock().unwrap() = Some(kv);
    }

    /// 获取持久化存储引用
    fn persistence(&self) -> Option<Arc<dyn JsonKeyValueStore>> {
        self.persistence.lock().unwrap().clone()
    }

    /// 设置会话唤醒器（推模式投递）
    pub fn with_session_waker(&self, waker: Arc<dyn SessionWaker>) {
        *self.session_waker.lock().unwrap() = Some(waker);
    }

    /// 优雅关闭：通知所有后台守护循环退出
    ///
    /// 调用后 stale_monitor 和 completion_watcher 将在下一个 tick 退出。
    /// 已注册的后台任务不受影响（仍可通过 list/claim 查询）。
    pub fn shutdown(&self) {
        tracing::info!("[delegation] shutting down background loops");
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests;