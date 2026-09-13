use futures::future::join_all;
use loom_core::{
    ActivitySummary, AgentLaunchRequest, AgentLifecycleManager, IsolationLevel, MemoryScope, Result,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::DelegationManager;
use super::spawn_args::{SpawnArgs, SpawnTask, SubagentJob};
use super::types::HEARTBEAT_INTERVAL_SECS;
use super::types::STALE_CYCLES_IDLE;
use super::types::STALE_CYCLES_IN_TOOL;
use super::schema::{
    append_output_contract, build_retry_message, validate_output, MAX_SCHEMA_RETRIES,
};
use crate::result::SubagentResult;

/// 父中断传播的清理 guard：drop 时停止所有跟踪的子 Agent
///
/// 当 sync future 被取消（客户端断开、父 Agent 出错等）时，
/// 该 guard 的 Drop 会触发，best-effort 停止所有仍在运行的子 Agent。
pub(crate) struct CleanupGuard {
    pub lifecycle: Arc<dyn AgentLifecycleManager>,
    pub child_ids: Arc<std::sync::Mutex<Vec<Uuid>>>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let ids = self.child_ids.lock().unwrap();
        if ids.is_empty() {
            return;
        }
        tracing::warn!(
            "[delegation] sync future dropped, stopping {} child agents",
            ids.len()
        );
        let lifecycle = self.lifecycle.clone();
        let ids = ids.clone();
        // tokio::spawn 在 runtime 仍运行时正常调度任务；若 runtime 已关闭，
        // 子 Agent 将随进程退出而终止，此处无法异步停止。
        let _handle = tokio::spawn(async move {
            for id in ids {
                if let Err(e) = lifecycle.stop(&id).await {
                    tracing::warn!(
                        "[delegation] failed to stop child agent {} during cleanup: {}",
                        id,
                        e
                    );
                }
            }
        });
    }
}

impl DelegationManager {
    /// spawn_agent 工具入口
    ///
    /// 支持：
    /// - 单 goal 委派
    /// - tasks 批量委派（并行）
    /// - background 异步模式（仅顶层）
    /// - action=list/stop 控制操作
    ///
    /// `scope` 从父 Agent 的 ToolContext 传入，
    /// 确保子 Agent 继承父 Agent 的租户/用户隔离。
    /// 注意：子 Agent 不加载父会话记忆（子 Agent skip_memory=True），
    /// 父 Agent 应通过 `context` 字段显式传递子 Agent 所需信息。
    pub async fn execute_spawn_agent(
        &self,
        args: &Value,
        current_agent_id: Uuid,
        parent_agent_id: Option<Uuid>,
        delegate_depth: u32,
        parent_toolsets: Vec<String>,
        scope: Option<MemoryScope>,
        session_id: Option<String>,
        cancel_token: Option<CancellationToken>,
    ) -> Result<Value> {
        let is_top_level = parent_agent_id.is_none();
        tracing::debug!(
            "[delegation] execute_spawn_agent: current_agent_id={}, parent={:?}, depth={}, is_top_level={}",
            current_agent_id,
            parent_agent_id,
            delegate_depth,
            is_top_level
        );

        // 1. 解析参数（JSON → 强类型结构体，与业务逻辑分离）
        let parsed = match SpawnArgs::from_value(args, is_top_level) {
            Ok(p) => p,
            Err(err_json) => return Ok(err_json),
        };

        tracing::debug!(
            "[delegation] parsed: action={}, tasks={}, background={}, depth_limit={}",
            parsed.action,
            parsed.tasks.len(),
            parsed.background,
            self.config.max_delegation_depth
        );

        // 2. 控制类 action（list/stop/steer）不需要走委派流程
        match parsed.action.as_str() {
            "list" => return self.list_sub_agents(current_agent_id).await,
            "stop" => return self.stop_sub_agent(args, current_agent_id).await,
            "steer" => return self.steer_sub_agent(args, current_agent_id).await,
            _ => {}
        }

        // 3. 委派深度校验
        let child_depth = delegate_depth + 1;
        if child_depth > self.config.max_delegation_depth {
            tracing::debug!(
                "[delegation] depth limit reached: child_depth={}, max={}",
                child_depth,
                self.config.max_delegation_depth
            );
            return Ok(json!({
                "error": format!(
                    "delegation depth limit reached (depth={}, max={})",
                    child_depth, self.config.max_delegation_depth
                )
            }));
        }

        // 4. 执行委派（后台 / 同步）
        if parsed.background {
            return self
                .spawn_background(
                    parsed.tasks,
                    parsed.toolsets,
                    parent_toolsets,
                    parsed.isolation,
                    current_agent_id,
                    child_depth,
                    scope,
                    session_id,
                    cancel_token,
                )
                .await;
        }

        self.spawn_sync(
            parsed.tasks,
            parsed.toolsets,
            parent_toolsets,
            parsed.isolation,
            current_agent_id,
            child_depth,
            scope,
            session_id,
            cancel_token,
        )
        .await
    }

    /// 同步委派：等待所有子 Agent 完成并返回结果
    ///
    /// 若任务携带 `output_schema`，则：
    /// - 向子 Agent context 追加 OUTPUT CONTRACT 块
    /// - 验证子 Agent 最终响应是否符合 schema
    /// - 验证失败时进行单次有界重试（MAX_SCHEMA_RETRIES）
    pub(super) async fn spawn_sync(
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
        // 父中断传播：跟踪所有已启动的子 Agent，future 被 drop 时统一停止
        // （参考 child agent cancellation：父中断 → 所有子 Agent 停止）
        let child_ids: Arc<std::sync::Mutex<Vec<Uuid>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let cleanup_lifecycle = self.lifecycle.clone();
        let cleanup_ids = child_ids.clone();
        let _cleanup_guard = CleanupGuard {
            lifecycle: cleanup_lifecycle,
            child_ids: cleanup_ids,
        };

        let delegate_summary_max_chars = self.config.delegate_summary_max_chars;
        let mut futures = Vec::with_capacity(tasks.len());

        for task in tasks {
            let lifecycle = self.lifecycle.clone();
            let toolsets = toolsets.clone();
            let parent_toolsets = parent_toolsets.clone();
            let scope = scope.clone();
            let session_id = session_id.clone();
            let permit = self.child_semaphore.clone().acquire_owned().await;
            let child_ids = child_ids.clone();
            let cancel_token = cancel_token.clone();
            let fut = async move {
                let _permit = permit;
                let result = Self::run_single_subagent(
                    lifecycle,
                    task,
                    toolsets,
                    parent_toolsets,
                    isolation,
                    parent_agent_id,
                    child_depth,
                    scope,
                    session_id,
                    cancel_token,
                    child_ids,
                    delegate_summary_max_chars,
                )
                .await;
                result
            };
            futures.push(fut);
        }

        let results = join_all(futures).await;

        tracing::info!("batch delegation completed: {} tasks", results.len());

        Ok(json!({
            "results": results,
            "mode": "sync"
        }))
    }

    /// 执行单个子 Agent 任务（含 schema 验证 + 单次重试）
    ///
    /// 返回值为 JSON 对象（SubagentResult 的 to_json + goal）。
    async fn run_single_subagent(
        lifecycle: Arc<dyn AgentLifecycleManager>,
        task: SpawnTask,
        toolsets: Vec<String>,
        parent_toolsets: Vec<String>,
        isolation: IsolationLevel,
        parent_agent_id: Uuid,
        child_depth: u32,
        scope: Option<MemoryScope>,
        session_id: Option<String>,
        cancel_token: Option<CancellationToken>,
        child_ids: Arc<std::sync::Mutex<Vec<Uuid>>>,
        delegate_summary_max_chars: usize,
    ) -> Value {
        let goal = task.goal.clone();

        // 首次执行的 context：若有 schema，追加 OUTPUT CONTRACT
        let mut context = task.context.clone();
        if let Some(ref schema) = task.output_schema {
            context = append_output_contract(&context, schema);
        }

        let job = SubagentJob {
            goal: goal.clone(),
            context,
            toolsets,
            parent_toolsets,
            isolation,
            parent_agent_id,
            child_depth,
            output_schema: task.output_schema,
            scope,
            session_id,
        };

        let sub_result =
            Self::validate_with_retry(lifecycle, job, cancel_token, child_ids).await;

        // 应用委派摘要预算：截断过长的子 Agent 结果，防止撑爆父上下文
        let sub_result = sub_result.with_summary_budget(delegate_summary_max_chars);

        let mut result = sub_result.to_json();
        result["goal"] = Value::String(goal);
        result
    }

    /// 启动单个子 Agent 并等待其完成，返回 SubagentResult
    ///
    /// 心跳陈旧检测（参考 delegate_tool_child_run._Heartbeat）：
    /// - 定期采样子 Agent 的 ActivitySummary
    /// - 若 iterations/current_tool/last_activity_ts 任一推进 → 重置陈旧计数
    /// - 否则陈旧计数 +1；空闲态达到 STALE_CYCLES_IDLE、工具中达到 STALE_CYCLES_IN_TOOL 则判定卡死
    /// - 判定卡死后主动停止子 Agent，返回超时错误结果（区分于正常长任务）
    async fn launch_subagent_and_wait(
        lifecycle: &Arc<dyn AgentLifecycleManager>,
        job: &SubagentJob,
        attempt: usize,
        cancel_token: Option<CancellationToken>,
        child_ids: Arc<std::sync::Mutex<Vec<Uuid>>>,
    ) -> SubagentResult {
        let launch_req = AgentLaunchRequest {
            capability_id: None,
            goal: job.goal.clone(),
            context: job.context.clone(),
            toolsets: job.toolsets.clone(),
            isolation: job.isolation,
            timeout: None,
            parent_agent_id: Some(job.parent_agent_id),
            config: Value::Null,
            delegate_depth: job.child_depth,
            scope: job.scope.clone(),
            parent_toolsets: job.parent_toolsets.clone(),
            session_id: job.session_id.clone(),
        };

        let handle = match lifecycle.launch(launch_req).await {
            Ok(h) => h,
            Err(e) => return SubagentResult::from_error(&e.to_string(), 0, 0),
        };

        let agent_id = handle.agent_id;
        tracing::info!(
            "spawned child agent {} (depth={}, attempt={})",
            agent_id,
            job.child_depth,
            attempt
        );

        // 父中断传播：注册子 Agent ID，供 CleanupGuard 在 future drop 时统一停止
        child_ids.lock().unwrap().push(agent_id);

        // 心跳陈旧检测：与 wait_for_result 并发执行
        let lifecycle_hb = lifecycle.clone();
        let mut stale_count: u32 = 0;
        let mut last_seen_iter: u64 = 0;
        let mut last_seen_tool: Option<String> = None;
        let mut last_seen_ts: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut ticker = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let wait_fut = lifecycle.wait_for_result(&agent_id);
        tokio::pin!(wait_fut);

        let result = loop {
            tokio::select! {
                // 子 Agent 正常完成
                output = &mut wait_fut => {
                    break match output {
                        Ok(o) => SubagentResult::from_success_with_meta(
                            o.summary,
                            o.iterations,
                            o.tool_calls_made,
                            &o.tool_names,
                            o.duration_ms,
                            o.tool_trace,
                        ),
                        Err(e) => SubagentResult::from_error(&e.to_string(), 0, 0),
                    };
                }
                // 父中断传播：父 Agent 被取消时，主动停止子 Agent 并返回取消结果
                _ = async {
                    match &cancel_token {
                        Some(t) => t.cancelled().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    tracing::warn!(
                        "[delegation] parent cancelled, stopping child agent {}",
                        agent_id
                    );
                    let _ = lifecycle.stop(&agent_id).await;
                    let mut r = SubagentResult::from_error(
                        &format!("sub-agent {} cancelled by parent", agent_id),
                        0,
                        0,
                    );
                    r.error_classification = crate::result::ErrorClassification::Cancelled;
                    break r;
                }
                // 心跳采样
                _ = ticker.tick() => {
                    let summary = match lifecycle_hb.get_activity_summary(&agent_id).await {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let ActivitySummary {
                        iterations,
                        current_tool,
                        last_activity_ts,
                        ..
                    } = summary;

                    let advanced = iterations > last_seen_iter
                        || current_tool != last_seen_tool
                        || (last_activity_ts.is_some()
                            && (last_seen_ts.is_none() || last_activity_ts > last_seen_ts));

                    if advanced {
                        stale_count = 0;
                        last_seen_iter = iterations;
                        last_seen_tool = current_tool.clone();
                        last_seen_ts = last_activity_ts;
                    } else {
                        stale_count += 1;
                        let threshold = if current_tool.is_some() {
                            STALE_CYCLES_IN_TOOL
                        } else {
                            STALE_CYCLES_IDLE
                        };
                        if stale_count >= threshold {
                            let elapsed_secs = stale_count as u64 * HEARTBEAT_INTERVAL_SECS;
                            tracing::warn!(
                                "[heartbeat] child agent {} appears stale: no progress for {} cycles ({}s), \
                                 current_tool={:?}. Stopping stuck agent.",
                                agent_id,
                                stale_count,
                                elapsed_secs,
                                current_tool
                            );
                            let _ = lifecycle_hb.stop(&agent_id).await;
                            break SubagentResult::from_error(
                                &format!(
                                    "sub-agent {} timed out (stuck for {}s, no progress, tool={:?})",
                                    agent_id, elapsed_secs, current_tool
                                ),
                                0,
                                0,
                            );
                        }
                    }
                }
            }
        };

        result
    }

    /// 带 schema 验证的重试循环
    ///
    /// 若无 output_schema，执行一次后直接返回；否则验证输出，
    /// 失败时最多重试 MAX_SCHEMA_RETRIES 次，每次将验证错误追加到 context。
    async fn validate_with_retry(
        lifecycle: Arc<dyn AgentLifecycleManager>,
        mut job: SubagentJob,
        cancel_token: Option<CancellationToken>,
        child_ids: Arc<std::sync::Mutex<Vec<Uuid>>>,
    ) -> SubagentResult {
        let mut last_result: Option<SubagentResult> = None;

        for attempt in 0..=MAX_SCHEMA_RETRIES {
            let sub_result = Self::launch_subagent_and_wait(
                &lifecycle,
                &job,
                attempt,
                cancel_token.clone(),
                child_ids.clone(),
            )
            .await;

            // 父中断：直接返回取消结果，不再重试
            if !sub_result.success
                && sub_result.error_classification == crate::result::ErrorClassification::Cancelled
            {
                return sub_result;
            }

            // 无 schema → 直接返回
            let Some(ref schema) = job.output_schema else {
                return sub_result;
            };

            // 验证输出
            let (valid, errors) = validate_output(&sub_result.summary, schema);
            if valid {
                tracing::debug!(
                    "sub-agent output passed schema validation (attempt={})",
                    attempt
                );
                return sub_result;
            }

            // 验证失败：若还有重试机会，构建重试消息并重新执行
            if attempt < MAX_SCHEMA_RETRIES {
                tracing::warn!(
                    "sub-agent output failed schema validation, retrying (attempt={}): {:?}",
                    attempt,
                    errors
                );
                let retry_msg = build_retry_message(&errors);
                job.context = format!("{}\n\n{}", job.context.trim_end(), retry_msg);
                last_result = Some(sub_result);
                continue;
            }

            // 重试耗尽：标记失败并返回最后一次结果
            tracing::error!(
                "sub-agent output failed schema validation after {} retries: {:?}",
                MAX_SCHEMA_RETRIES,
                errors
            );
            let mut failed = sub_result;
            failed.success = false;
            failed.error_classification = crate::result::ErrorClassification::DelegationError;
            failed.error_message = Some(format!(
                "output did not match schema after {} retries: {}",
                MAX_SCHEMA_RETRIES,
                errors.join("; ")
            ));
            return failed;
        }

        last_result.unwrap_or_else(|| SubagentResult::from_error("sub-agent produced no result", 0, 0))
    }
}