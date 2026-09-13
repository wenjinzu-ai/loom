//! Agent 生命周期管理
//!
//! 管理 Agent 的创建、启停、暂停、恢复、销毁，维护状态机。
//! 持有每个 Agent 的运行时句柄（AgentRuntime），stop/destroy 会真正终止运行时。

use async_trait::async_trait;
use loom_core::{
    isolation::select_isolation, ActivitySummary, AgentHandle, AgentLaunchRequest,
    AgentLifecycleManager, AgentRunner, AgentRuntime, AgentState, IsolationLevel, Result,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use loom_isolation::IsolationManager;

struct AgentEntry {
    handle: AgentHandle,
    runtime: Arc<dyn AgentRuntime>,
}

pub struct LifecycleManager {
    agents: RwLock<HashMap<Uuid, AgentEntry>>,
    isolation: IsolationManager,
}

impl LifecycleManager {
    pub fn new() -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
            isolation: IsolationManager::new(),
        }
    }

    /// 设置 AgentRunner，使 launch 的 Agent 能运行真正的 LLM 对话循环。
    /// 支持延迟注入以打破循环依赖。
    pub fn set_runner(&self, runner: Arc<dyn AgentRunner>) {
        self.isolation.set_runner(runner);
    }

    fn transition(&self, id: &Uuid, next: AgentState) -> Result<()> {
        let mut agents = self.agents.write();
        let entry = agents
            .get_mut(id)
            .ok_or(loom_core::LoomError::AgentNotFound(*id))?;
        if !entry.handle.state.can_transition_to(next) {
            return Err(loom_core::LoomError::InvalidStateTransition {
                from: entry.handle.state.to_string(),
                to: next.to_string(),
            });
        }
        entry.handle.state = next;
        Ok(())
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentLifecycleManager for LifecycleManager {
    async fn launch(&self, req: AgentLaunchRequest) -> Result<AgentHandle> {
        let agent_id = Uuid::new_v4();
        tracing::debug!(
            "[lifecycle] launch: agent_id={}, isolation={:?}, parent={:?}, depth={}",
            agent_id,
            req.isolation,
            req.parent_agent_id,
            req.delegate_depth
        );

        let isolation = if req.isolation == IsolationLevel::Coroutine && req.config.is_object() {
            let requires_fs = req
                .config
                .get("requires_fs")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let requires_network = req
                .config
                .get("requires_network")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let untrusted = req
                .config
                .get("untrusted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            select_isolation(requires_fs, requires_network, untrusted)
        } else {
            req.isolation
        };

        let spec = loom_core::AgentSpec {
            agent_id,
            capability_id: req.capability_id,
            goal: req.goal.clone(),
            context: req.context,
            toolsets: req.toolsets,
            isolation,
            timeout: req.timeout,
            config: req.config,
            parent_agent_id: req.parent_agent_id,
            delegate_depth: req.delegate_depth,
            scope: req.scope,
            parent_toolsets: req.parent_toolsets,
            session_id: req.session_id,
            // 活动状态由隔离后端在 spawn 时创建并注入到 spec 中，
            // 这里先置 None，后端内部会覆盖为 Some(shared_state)。
            activity_state: None,
        };

        let runtime = self.isolation.spawn(spec).await?;
        let runtime: Arc<dyn AgentRuntime> = Arc::from(runtime);

        let handle = AgentHandle {
            agent_id,
            state: AgentState::Created,
            isolation,
            parent_agent_id: req.parent_agent_id,
            created_at: chrono::Utc::now(),
            goal: req.goal,
        };

        self.agents.write().insert(
            agent_id,
            AgentEntry {
                handle: handle.clone(),
                runtime,
            },
        );

        // 通过状态机校验 Created → Running（即时启动路径）
        self.transition(&agent_id, AgentState::Running)?;

        tracing::info!("launched agent {} with isolation {:?}", agent_id, isolation);

        // 返回 Running 状态的 handle
        let mut handle = handle;
        handle.state = AgentState::Running;
        Ok(handle)
    }

    async fn stop(&self, id: &Uuid) -> Result<()> {
        let runtime = {
            let agents = self.agents.read();
            agents
                .get(id)
                .ok_or(loom_core::LoomError::AgentNotFound(*id))?
                .runtime
                .clone()
        };

        self.transition(id, AgentState::Stopping)?;
        if let Err(e) = runtime.stop().await {
            tracing::warn!("stopping agent {} runtime failed: {}", id, e);
        }
        self.transition(id, AgentState::Stopped)?;
        tracing::info!("stopped agent {}", id);
        Ok(())
    }

    async fn pause(&self, id: &Uuid) -> Result<()> {
        // TODO: 当前 AgentRuntime trait 未提供 pause 能力，仅更新状态标记。
        // runtime 实际仍在运行，待 runtime 层支持后需调用 runtime.pause()。
        tracing::warn!(
            "pause agent {}: state updated to Paused, but runtime is still running (not yet supported)",
            id
        );
        self.transition(id, AgentState::Paused)?;
        Ok(())
    }

    async fn resume(&self, id: &Uuid) -> Result<()> {
        // TODO: 同 pause，runtime 层暂不支持恢复，仅更新状态。
        tracing::warn!("resume agent {}: state updated to Running", id);
        self.transition(id, AgentState::Running)?;
        Ok(())
    }

    async fn destroy(&self, id: &Uuid) -> Result<()> {
        let handle = self.get_status(id).await;
        if let Ok(ref h) = handle {
            if matches!(h.state, AgentState::Running | AgentState::Paused) {
                let _ = self.stop(id).await;
            }
        }

        let runtime = {
            let agents = self.agents.read();
            agents
                .get(id)
                .ok_or(loom_core::LoomError::AgentNotFound(*id))?
                .runtime
                .clone()
        };
        let _ = runtime.stop().await;

        if let Err(e) = self.transition(id, AgentState::Destroyed) {
            // 可能已处于终态，记录但不阻塞销毁流程
            tracing::debug!("destroy agent {} transition skipped: {}", id, e);
        }
        self.agents.write().remove(id);
        tracing::info!("destroyed agent {}", id);
        Ok(())
    }

    async fn get_status(&self, id: &Uuid) -> Result<AgentHandle> {
        self.agents
            .read()
            .get(id)
            .map(|e| e.handle.clone())
            .ok_or(loom_core::LoomError::AgentNotFound(*id))
    }

    async fn list(&self) -> Result<Vec<AgentHandle>> {
        Ok(self
            .agents
            .read()
            .values()
            .map(|e| e.handle.clone())
            .collect())
    }

    async fn send_message(&self, id: &Uuid, msg: loom_core::AgentMessage) -> Result<()> {
        let runtime = {
            let agents = self.agents.read();
            agents
                .get(id)
                .ok_or(loom_core::LoomError::AgentNotFound(*id))?
                .runtime
                .clone()
        };
        runtime.send(msg).await
    }

    async fn get_activity_summary(&self, id: &Uuid) -> Result<ActivitySummary> {
        let runtime = {
            let agents = self.agents.read();
            agents
                .get(id)
                .ok_or(loom_core::LoomError::AgentNotFound(*id))?
                .runtime
                .clone()
        };
        runtime.activity_summary().await
    }

    async fn wait_for_result(&self, id: &Uuid) -> Result<loom_core::AgentOutput> {
        let runtime = {
            let agents = self.agents.read();
            agents
                .get(id)
                .ok_or(loom_core::LoomError::AgentNotFound(*id))?
                .runtime
                .clone()
        };
        let result = runtime.wait().await;

        // 无论成功还是失败，都更新状态并清理 entry（避免内存泄漏）
        let mut agents = self.agents.write();
        if let Some(entry) = agents.get_mut(id) {
            match &result {
                Ok(output) => {
                    entry.handle.state = loom_core::AgentState::Stopped;
                    tracing::info!(
                        "agent {} completed, iterations={}, tool_calls={}, success={}",
                        id,
                        output.iterations,
                        output.tool_calls_made,
                        output.success
                    );
                }
                Err(e) => {
                    entry.handle.state = loom_core::AgentState::Failed;
                    tracing::error!("agent {} failed: {}", id, e);
                }
            }
        }
        // Agent 已结束，从内存中移除 entry 释放资源
        agents.remove(id);

        result
    }
}