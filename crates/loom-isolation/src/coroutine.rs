//! 协程级隔离后端
//!
//! 使用 tokio task 在同进程内运行 Agent。
//! 启动开销 μs 级，通信纳秒级，适用于可信 Agent。
//! 若注入了 AgentRunner，则运行真正的 LLM 对话循环；否则跑空消息循环。

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{
    ActivitySummary, AgentMessage, AgentOutput, AgentRunner, AgentRuntime, AgentSpec, HealthStatus,
    IsolationBackend, IsolationLevel, Result,
};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct CoroutineBackend {
    runner_slot: Arc<RwLock<Option<Arc<dyn AgentRunner>>>>,
}

impl CoroutineBackend {
    pub fn new(runner_slot: Arc<RwLock<Option<Arc<dyn AgentRunner>>>>) -> Self {
        Self { runner_slot }
    }
}

#[async_trait]
impl IsolationBackend for CoroutineBackend {
    fn level(&self) -> IsolationLevel {
        IsolationLevel::Coroutine
    }

    async fn spawn(&self, spec: AgentSpec) -> Result<Box<dyn AgentRuntime>> {
        let (tx, rx) = mpsc::channel(256);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (result_tx, result_rx) = oneshot::channel::<Result<AgentOutput>>();

        let agent_id = spec.agent_id;
        let goal = spec.goal.clone();
        let runner = self.runner_slot.read().clone();

        // 创建共享活动状态，注入 spec 供子循环更新，runtime 侧持有副本用于采样
        let activity = Arc::new(std::sync::Mutex::new(ActivitySummary {
            iterations: 0,
            api_call_count: 0,
            current_tool: None,
            last_activity_ts: Some(chrono::Utc::now()),
        }));
        let mut spec = spec;
        spec.activity_state = Some(activity.clone());

        tokio::spawn(async move {
            tracing::info!("coroutine agent {} started, goal: {}", agent_id, goal);
            let result = run_agent_loop(spec, rx, stop_rx, runner).await;
            let _ = result_tx.send(result);
            tracing::info!("coroutine agent {} stopped", agent_id);
        });

        Ok(Box::new(CoroutineRuntime {
            agent_id,
            tx,
            stop_tx: Mutex::new(Some(stop_tx)),
            result_rx: Mutex::new(Some(result_rx)),
            activity,
        }))
    }
}

async fn run_agent_loop(
    spec: AgentSpec,
    mut rx: mpsc::Receiver<AgentMessage>,
    mut stop_rx: oneshot::Receiver<()>,
    runner: Option<Arc<dyn AgentRunner>>,
) -> Result<AgentOutput> {
    if let Some(runner) = runner {
        return runner.run(&spec).await;
    }

    loop {
        tokio::select! {
            _ = &mut stop_rx => break,
            msg = rx.recv() => {
                match msg {
                    Some(_msg) => {
                        tracing::trace!("agent {} received message", spec.agent_id);
                    }
                    None => break,
                }
            }
        }
    }

    Ok(AgentOutput::success(String::new(), 0, 0, Vec::new()))
}

pub struct CoroutineRuntime {
    #[allow(dead_code)]
    agent_id: Uuid,
    tx: mpsc::Sender<AgentMessage>,
    stop_tx: Mutex<Option<oneshot::Sender<()>>>,
    result_rx: Mutex<Option<oneshot::Receiver<Result<AgentOutput>>>>,
    /// 子循环更新、父端采样的共享活动状态
    activity: Arc<std::sync::Mutex<ActivitySummary>>,
}

#[async_trait]
impl AgentRuntime for CoroutineRuntime {
    async fn send(&self, msg: AgentMessage) -> Result<()> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| loom_core::LoomError::Other("agent channel closed".into()))
    }

    async fn send_stream(
        &self,
        _msg: AgentMessage,
    ) -> Result<BoxStream<'static, Result<serde_json::Value>>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.stop_tx.lock();
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        let last_heartbeat = self
            .activity
            .lock()
            .ok()
            .and_then(|a| a.last_activity_ts)
            .or_else(|| Some(chrono::Utc::now()));
        HealthStatus {
            alive: true,
            last_heartbeat,
            diagnostic: None,
        }
    }

    async fn activity_summary(&self) -> Result<ActivitySummary> {
        self.activity
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| loom_core::LoomError::Other("activity state lock poisoned".into()))
    }

    async fn wait(&self) -> Result<AgentOutput> {
        let rx = {
            let mut guard = self.result_rx.lock();
            guard.take().ok_or(loom_core::LoomError::Other(
                "result already consumed".into(),
            ))?
        };
        rx.await
            .map_err(|_| loom_core::LoomError::Other("agent task dropped without result".into()))?
    }
}