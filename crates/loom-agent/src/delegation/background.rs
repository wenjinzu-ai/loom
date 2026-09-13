use loom_core::{
    AgentLaunchRequest, IsolationLevel, MemoryScope, Result,
};
use serde_json::{json, Value};
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::DelegationManager;
use super::spawn_args::SpawnTask;
use super::types::{
    BackgroundTask, BackgroundTaskStatus, DeliveryState, PERSISTENCE_NAMESPACE,
};
use super::schema::append_output_contract;
use crate::result::SubagentResult;

impl DelegationManager {
    /// 异步委派：立即返回 handle，不等待结果
    ///
    /// P0-2 异步交付能力检测：
    /// 若 `session_id` 为 None（一次性/无状态运行，如 cron、无会话 HTTP），
    /// 后台结果无法跨轮次回注给父 Agent，回退为同步执行并附带说明。
    ///
    /// P1 批次管理：
    /// 同一 spawn_background 调用的多个任务共享一个 `delegation_id`，
    /// 注册到 BackgroundTaskRegistry，支持批量查询与状态聚合。
    ///
    /// P0-1 后台结果回注：
    /// 子 Agent 完成后将结果存入 Registry：
    /// - 父 Agent 可在当前轮次通过 action=list 拉取
    /// - chat_stream 在下一轮对话开始时注入会话历史
    pub(crate) async fn spawn_background(
        &self,
        tasks: Vec<SpawnTask>,
        toolsets: Vec<String>,
        parent_toolsets: Vec<String>,
        isolation: IsolationLevel,
        parent_agent_id: Uuid,
        child_depth: u32,
        scope: Option<MemoryScope>,
        session_id: Option<String>,
        cancel_token: Option<CancellationToken>,
    ) -> Result<Value> {
        // P0-2: 异步交付能力检测
        // 无会话 ID 时无法回注后台结果，回退为同步执行
        if session_id.is_none() {
            tracing::info!(
                "[delegation] background requested but no session_id (async delivery unsupported), \
                 falling back to sync execution"
            );
            let sync_result = self
                .spawn_sync(
                    tasks,
                    toolsets,
                    parent_toolsets,
                    isolation,
                    parent_agent_id,
                    child_depth,
                    scope,
                    session_id,
                    cancel_token,
                )
                .await?;
            // 在同步结果中附加回退说明
            let mut map = sync_result.as_object().cloned().unwrap_or_default();
            map.insert(
                "fallback_note".to_string(),
                Value::String(
                    "background execution fell back to sync: no session_id to receive async results"
                        .to_string(),
                ),
            );
            map.insert("mode".to_string(), Value::String("sync".to_string()));
            return Ok(Value::Object(map));
        }

        // P1: 整个批次共享一个 delegation_id
        let delegation_id = Uuid::new_v4();
        let dispatched_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut handles = Vec::with_capacity(tasks.len());

        for task in tasks {
            let permit = self.child_semaphore.clone().acquire_owned().await;
            let lifecycle = self.lifecycle.clone();
            let toolsets = toolsets.clone();
            let parent_toolsets = parent_toolsets.clone();
            let goal = task.goal.clone();
            let scope = scope.clone();
            let session_id = session_id.clone();
            let registry = self.registry.clone();
            // 若有 schema，追加 OUTPUT CONTRACT（后台模式不验证结果，但提示子 Agent 遵循）
            let ctx = match &task.output_schema {
                Some(schema) => append_output_contract(&task.context, schema),
                None => task.context,
            };
            // 多租户隔离：从 scope 提取租户/用户信息（scope 随后被 move 到 launch_req）
            let (scope_tenant_id, scope_user_id) = match &scope {
                Some(s) => (s.tenant_id.clone(), s.user_id.clone()),
                None => (None, None),
            };
            let launch_req = AgentLaunchRequest {
                capability_id: None,
                goal: goal.clone(),
                context: ctx,
                toolsets,
                isolation,
                timeout: None,
                parent_agent_id: Some(parent_agent_id),
                config: Value::Null,
                delegate_depth: child_depth,
                scope,
                parent_toolsets,
                session_id: session_id.clone(),
            };

            match lifecycle.launch(launch_req).await {
                Ok(handle) => {
                    let agent_id = handle.agent_id;
                    tracing::info!(
                        "background spawned child agent {} (delegation={}, depth={}, session={:?})",
                        agent_id,
                        delegation_id,
                        child_depth,
                        session_id
                    );
                    // P1: 注册到批次注册表
                    let bg_task = BackgroundTask {
                        delegation_id,
                        child_agent_id: agent_id,
                        goal: goal.clone(),
                        status: BackgroundTaskStatus::Running,
                        parent_agent_id,
                        session_id: session_id.clone(),
                        // 会话路由：记录原始会话 ID，结果始终回到发起委派的会话
                        origin_session_id: session_id.clone(),
                        dispatched_at,
                        completed_at: None,
                        result: None,
                        live_transcript: vec![],
                        delivery_state: DeliveryState::Pending,
                        delivery_attempts: 0,
                        delivery_claim: None,
                        delivery_claimed_at: None,
                        // 记录派发该任务的进程实例 ID，崩溃恢复时判断归属
                        owner_instance_id: Some(self.instance_id.clone()),
                        // 多租户隔离：从 scope 提取租户/用户信息
                        tenant_id: scope_tenant_id,
                        user_id: scope_user_id,
                    };
                    registry.register(bg_task.clone());
                    // 持久化：后台任务注册后立即写入存储
                    self.persist_task(&bg_task);
                    handles.push(json!({
                        "delegation_id": delegation_id.to_string(),
                        "agent_id": agent_id.to_string(),
                        "goal": goal,
                        "status": "running"
                    }));
                    let registry_clone = registry.clone();
                    let persistence_clone = self.persistence();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let result = lifecycle.wait_for_result(&agent_id).await;
                        tracing::info!(
                            "background agent {} finished: success={}, iterations={}",
                            agent_id,
                            result.as_ref().map(|r| r.success).unwrap_or(false),
                            result.as_ref().map(|r| r.iterations).unwrap_or(0)
                        );
                        // P0-1/P1: 将完成结果存入 Registry
                        let sub_result = match result {
                            Ok(o) => {
                                if o.success {
                                    SubagentResult::from_success_with_meta(
                                        o.summary,
                                        o.iterations,
                                        o.tool_calls_made,
                                        &o.tool_names,
                                        o.duration_ms,
                                        o.tool_trace,
                                    )
                                } else {
                                    SubagentResult::from_error(
                                        &o.error_message.unwrap_or_else(|| {
                                            format!("background agent {} failed", agent_id)
                                        }),
                                        o.iterations,
                                        o.tool_calls_made,
                                    )
                                }
                            }
                            Err(e) => SubagentResult::from_error(&e.to_string(), 0, 0),
                        };

                        // Finalizing 中间态：先标记并持久化，防止完成事件广播后、
                        // 最终状态写入前进程崩溃导致结果丢失
                        if let Some(finalizing_task) = registry_clone.begin_complete(&agent_id) {
                            if let Some(ref kv) = persistence_clone {
                                let key = finalizing_task.child_agent_id.to_string();
                                if let Ok(v) = serde_json::to_value(&finalizing_task) {
                                    if let Err(e) = kv.put(PERSISTENCE_NAMESPACE, &key, v).await {
                                        tracing::error!(
                                            "[persistence] failed to persist finalizing state for task {}: {}",
                                            key,
                                            e
                                        );
                                    }
                                }
                            }
                            // 写入最终结果，置 delivery_state=Pending，广播完成通知
                            if let Some(updated_task) =
                                registry_clone.finish_complete(&agent_id, sub_result)
                            {
                                // 持久化：最终完成状态写入存储
                                if let Some(ref kv) = persistence_clone {
                                    let key = updated_task.child_agent_id.to_string();
                                    if let Ok(v) = serde_json::to_value(&updated_task) {
                                        if let Err(e) = kv.put(PERSISTENCE_NAMESPACE, &key, v).await {
                                            tracing::error!(
                                                "[persistence] failed to persist final state for task {}: {}",
                                                key,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    handles.push(json!({
                        "goal": goal,
                        "error": e.to_string()
                    }));
                }
            }
        }

        Ok(json!({
            "delegation_id": delegation_id.to_string(),
            "handles": handles,
            "mode": "background",
            "note": "sub-agents running in background; results will be injected on next chat turn or via action=list"
        }))
    }
}