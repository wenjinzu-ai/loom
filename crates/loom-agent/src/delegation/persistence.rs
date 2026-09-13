use std::time::SystemTime;
use uuid::Uuid;

use super::DelegationManager;
use super::types::{
    BackgroundTask, BackgroundTaskStatus, DeliveryState, CLAIM_TTL_SECS, PERSISTENCE_NAMESPACE,
};
use crate::result::SubagentResult;

impl DelegationManager {
    /// 持久化单个后台任务（best-effort，失败仅日志告警，不影响主流程）
    pub(crate) fn persist_task(&self, task: &BackgroundTask) {
        let Some(kv) = self.persistence() else {
            return;
        };
        let key = task.child_agent_id.to_string();
        let value = match serde_json::to_value(task) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "[persistence] failed to serialize task {}: {}",
                    task.child_agent_id,
                    e
                );
                return;
            }
        };
        tokio::spawn(async move {
            if let Err(e) = kv.put(PERSISTENCE_NAMESPACE, &key, value).await {
                tracing::warn!("[persistence] failed to persist task {}: {}", key, e);
            }
        });
    }

    /// 从持久化存储中删除已完成的任务（drain 后清理）
    pub(crate) fn delete_persisted(&self, child_agent_id: &Uuid) {
        let Some(kv) = self.persistence() else {
            return;
        };
        let key = child_agent_id.to_string();
        tokio::spawn(async move {
            let _ = kv.delete(PERSISTENCE_NAMESPACE, &key).await;
        });
    }

    /// 从持久化存储恢复未完成的后台任务（进程重启后调用）
    ///
    /// 参考 `recover_abandoned_delegations()`：
    /// - 加载所有持久化记录
    /// - 状态为 Running/Finalizing 且归属其他进程实例的任务标记为 Unknown（结果未知）
    /// - 状态为 Completed/Failed/Stale 的任务保留原结果，等待父 Agent 取回
    /// - Claimed 状态且 claim 超时（> CLAIM_TTL_SECS）的任务重置为 Pending 允许重试
    ///
    /// 返回恢复的任务数量。
    pub async fn recover_persisted(&self) -> usize {
        let Some(kv) = self.persistence() else {
            return 0;
        };
        let entries = match kv.list(PERSISTENCE_NAMESPACE).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("[persistence] failed to list persisted tasks: {}", e);
                return 0;
            }
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let current_instance = &self.instance_id;

        let mut recovered = 0;
        for (key, value) in entries {
            match serde_json::from_value::<BackgroundTask>(value) {
                Ok(mut task) => {
                    // Owner 检测：仅当任务归属其他进程实例时，Running/Finalizing 才视为已遗弃
                    // 若归属当前实例（不应出现在恢复中，防御性处理），保持原状态
                    let is_abandoned = task
                        .owner_instance_id
                        .as_deref()
                        .map(|owner| owner != current_instance)
                        .unwrap_or(true); // 无 owner 信息的旧记录按已遗弃处理

                    if is_abandoned
                        && matches!(
                            task.status,
                            BackgroundTaskStatus::Running | BackgroundTaskStatus::Finalizing
                        )
                    {
                        tracing::warn!(
                            "[persistence] recovering abandoned task {} (goal={}, owner={:?}), status -> unknown",
                            task.child_agent_id,
                            task.goal,
                            task.owner_instance_id
                        );
                        task.status = BackgroundTaskStatus::Unknown;
                        if task.result.is_none() {
                            task.result = Some(SubagentResult::from_error(
                                "process restarted before task completion; result unknown",
                                0,
                                0,
                            ));
                        }
                    }

                    // Claim TTL：已认领但超过 CLAIM_TTL_SECS 未确认的，重置为 Pending 允许重试
                    // （可能是认领进程崩溃，claim 令牌失效）
                    if task.delivery_state == DeliveryState::Claimed {
                        let claim_age = task
                            .delivery_claimed_at
                            .map(|t| now - t)
                            .unwrap_or(f64::INFINITY);
                        if claim_age > CLAIM_TTL_SECS {
                            tracing::warn!(
                                "[persistence] task {} claim expired ({:.0}s > {}s), resetting to Pending",
                                task.child_agent_id,
                                claim_age,
                                CLAIM_TTL_SECS
                            );
                            task.delivery_state = DeliveryState::Pending;
                            task.delivery_claim = None;
                            task.delivery_claimed_at = None;
                        }
                    }
                    // 重新注册到内存注册表
                    self.registry.register(task);
                    recovered += 1;
                }
                Err(e) => {
                    tracing::warn!("[persistence] failed to deserialize task {}: {}", key, e);
                }
            }
        }
        if recovered > 0 {
            tracing::info!(
                "[persistence] recovered {} background task(s) from store",
                recovered
            );
        }
        recovered
    }

    /// 取出指定会话的所有已完成后台子 Agent 结果（用于跨轮次回注）
    pub fn drain_background_results_by_session(
        &self,
        session_id: &str,
    ) -> Vec<(Uuid, String, SubagentResult)> {
        let drained = self.registry.drain_by_session(session_id);
        // 持久化清理：取出后从存储中删除
        for t in &drained {
            self.delete_persisted(&t.child_agent_id);
        }
        drained
            .into_iter()
            .filter_map(|t| t.result.map(|r| (t.child_agent_id, t.goal, r)))
            .collect()
    }

    /// exactly-once 投递：认领指定会话的所有 Pending 已完成任务
    ///
    /// 将 delivery_state 从 Pending 改为 Claimed（递增 attempts），写入 claim_id。
    /// 调用方在成功注入历史后必须调用 `complete_background_delivery` 确认；
    /// 失败则调用 `release_background_delivery` 回退重试。
    pub fn claim_completed_tasks(&self, session_id: &str) -> Vec<BackgroundTask> {
        self.registry.claim_completed(session_id, &self.instance_id)
    }

    /// exactly-once 投递：确认投递成功，删除任务及其持久化记录
    ///
    /// `claim_ids` 与 `child_agent_ids` 一一对应；claim 不匹配的任务不会被删除。
    /// 仅对实际从 Registry 删除的任务清理持久化记录，避免 claim 不匹配任务的
    /// 持久化被误删导致崩溃后结果丢失。
    pub fn complete_background_delivery(
        &self,
        child_agent_ids: &[Uuid],
        claim_ids: &[Option<String>],
    ) {
        let removed = self.registry.complete_delivery(child_agent_ids, claim_ids);
        for id in &removed {
            self.delete_persisted(id);
        }
    }

    /// exactly-once 投递：释放认领，回退到 Pending 以便重试
    ///
    /// `claim_ids` 与 `child_agent_ids` 一一对应；claim 不匹配的任务不会被释放。
    /// 超过 MAX_DELIVERY_ATTEMPTS 的任务会被标记为 Dropped 并从持久化中删除。
    /// 返回被标记为 Dropped 的任务 ID 列表。
    pub fn release_background_delivery(
        &self,
        child_agent_ids: &[Uuid],
        claim_ids: &[Option<String>],
    ) -> Vec<Uuid> {
        let dropped = self.registry.release_claim(child_agent_ids, claim_ids);
        for id in &dropped {
            self.delete_persisted(id);
        }
        dropped
    }

    /// 列出指定父 Agent 的所有后台任务（含运行中 + 已完成，含 live transcript）
    ///
    /// 用于 `action=list` 控制操作，返回完整任务快照。
    pub fn list_background_tasks(&self, parent_agent_id: &Uuid) -> Vec<BackgroundTask> {
        self.registry.list_by_parent(parent_agent_id)
    }

    /// 检查指定父 Agent 是否有待取回的后台结果
    pub fn has_background_results(&self, parent_agent_id: &Uuid) -> bool {
        self.registry.has_for_parent(parent_agent_id)
    }
}