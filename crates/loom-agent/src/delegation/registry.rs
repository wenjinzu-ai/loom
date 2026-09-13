use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

use super::types::{
    ActivitySnapshot, BackgroundTask, BackgroundTaskStatus, DeliveryState,
    CLAIM_TTL_SECS, MAX_DELIVERY_ATTEMPTS,
};
use crate::result::SubagentResult;

/// 后台任务注册表（线程安全）
///
/// 统一管理所有后台委派任务：
/// - 按 delegation_id 分组（批次管理）
/// - 按 parent_agent_id 索引（父 Agent 轮询）
/// - 按 session_id 索引（跨轮次回注）
/// - 支持实时转录快照（P2）
/// - 支持成本聚合（P2）
/// - 支持 completion 广播（推模式投递）
/// - 支持 exactly-once 投递状态机
#[derive(Clone)]
pub(crate) struct BackgroundTaskRegistry {
    inner: Arc<std::sync::Mutex<RegistryInner>>,
    /// 任务完成广播：通知 completion watcher 有新结果待投递
    completion_tx: tokio::sync::broadcast::Sender<CompletionNotification>,
}

/// 完成通知（推模式投递）
#[derive(Clone, Debug)]
pub(crate) struct CompletionNotification {
    pub session_id: String,
    pub child_agent_id: Uuid,
}

struct RegistryInner {
    /// child_agent_id → 任务记录
    tasks: HashMap<Uuid, BackgroundTask>,
    /// delegation_id → child_agent_id 列表（批次管理）
    by_delegation: HashMap<Uuid, Vec<Uuid>>,
}

impl BackgroundTaskRegistry {
    pub(crate) fn new() -> Self {
        let (completion_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            inner: Arc::new(std::sync::Mutex::new(RegistryInner {
                tasks: HashMap::new(),
                by_delegation: HashMap::new(),
            })),
            completion_tx,
        }
    }

    /// 订阅任务完成通知（推模式投递用）
    pub(crate) fn subscribe_completions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<CompletionNotification> {
        self.completion_tx.subscribe()
    }

    /// 注册一个新的后台任务
    pub(crate) fn register(&self, task: BackgroundTask) {
        let delegation_id = task.delegation_id;
        let child_id = task.child_agent_id;
        let mut inner = self.inner.lock().unwrap();
        inner
            .by_delegation
            .entry(delegation_id)
            .or_default()
            .push(child_id);
        inner.tasks.insert(child_id, task);
    }

    /// 开始完成流程：将状态置为 Finalizing 并返回快照（用于持久化）
    ///
    /// 参考 finalizing 状态：在写入最终结果前先标记 Finalizing 并持久化。
    /// 若进程在 Finalizing 持久化后、最终状态写入前崩溃，恢复时将其视为 Unknown。
    /// 调用方应持久化返回的 Finalizing 快照后，再调用 `finish_complete`。
    pub(crate) fn begin_complete(&self, child_agent_id: &Uuid) -> Option<BackgroundTask> {
        let mut inner = self.inner.lock().unwrap();
        let task = inner.tasks.get_mut(child_agent_id)?;
        // 仅 Running 状态可进入 Finalizing（重复调用或已完成的任务忽略）
        if task.status != BackgroundTaskStatus::Running {
            return None;
        }
        task.status = BackgroundTaskStatus::Finalizing;
        Some(task.clone())
    }

    /// 完成流程：写入最终结果，置 delivery_state=Pending，广播完成通知
    ///
    /// 必须在 `begin_complete` 之后调用。返回最终状态的任务快照（用于持久化）。
    pub(crate) fn finish_complete(
        &self,
        child_agent_id: &Uuid,
        result: SubagentResult,
    ) -> Option<BackgroundTask> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut inner = self.inner.lock().unwrap();
        let task = inner.tasks.get_mut(child_agent_id)?;
        // 仅 Finalizing 状态可完成（防止重复完成）
        if task.status != BackgroundTaskStatus::Finalizing {
            return None;
        }
        task.status = if result.success {
            BackgroundTaskStatus::Completed
        } else {
            BackgroundTaskStatus::Failed
        };
        task.completed_at = Some(now);
        task.result = Some(result);
        task.delivery_state = DeliveryState::Pending;
        let updated = task.clone();
        drop(inner);

        // 推模式：广播完成通知给 completion watcher
        if let Some(ref sid) = updated.origin_session_id.as_ref().or(updated.session_id.as_ref()) {
            let _ = self.completion_tx.send(CompletionNotification {
                session_id: sid.to_string(),
                child_agent_id: *child_agent_id,
            });
        }
        Some(updated)
    }

    /// 标记任务为卡死状态，返回更新后的任务（用于持久化）
    ///
    /// 同步清除残留的 delivery_claim，避免与新的 Pending 状态不一致；
    /// 返回更新后的任务供调用方持久化（与 begin_complete/finish_complete 持久化语义一致）。
    ///
    /// 仅 Running 状态的任务可被标记为 Stale，其他状态忽略。
    pub(crate) fn mark_stale(
        &self,
        child_agent_id: &Uuid,
        error_msg: &str,
    ) -> Option<BackgroundTask> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut inner = self.inner.lock().unwrap();
        let task = inner.tasks.get_mut(child_agent_id)?;
        // 仅 Running 状态可标记为 Stale（防止重复标记或标记已完成的任务）
        if task.status != BackgroundTaskStatus::Running {
            tracing::debug!(
                "[registry] mark_stale ignored for {}: current status={:?}, expected Running",
                child_agent_id,
                task.status
            );
            return None;
        }
        task.status = BackgroundTaskStatus::Stale;
        task.completed_at = Some(now);
        task.result = Some(SubagentResult::from_error(error_msg, 0, 0));
        task.delivery_state = DeliveryState::Pending;
        // 清除可能残留的认领令牌，与 Pending 状态保持一致
        task.delivery_claim = None;
        task.delivery_claimed_at = None;
        let updated = task.clone();
        drop(inner);

        // 推模式：卡死也算完成，通知 watcher 投递错误结果
        if let Some(ref sid) = updated.origin_session_id.as_ref().or(updated.session_id.as_ref()) {
            let _ = self.completion_tx.send(CompletionNotification {
                session_id: sid.to_string(),
                child_agent_id: *child_agent_id,
            });
        }
        Some(updated)
    }

    /// 追加活动快照（P2 实时转录）
    pub(crate) fn push_activity_snapshot(&self, child_agent_id: &Uuid, summary: &loom_core::ActivitySummary) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(task) = inner.tasks.get_mut(child_agent_id) {
            task.live_transcript.push(ActivitySnapshot::from_summary(summary));
            // 保留最近 50 条快照，避免无限增长
            if task.live_transcript.len() > 50 {
                let drain_count = task.live_transcript.len() - 50;
                task.live_transcript.drain(0..drain_count);
            }
        }
    }

    /// 取出某个父 Agent 的所有已完成后台结果（取出后清除）
    ///
    /// 仅取出 delivery_state=Pending 的任务，避免与 push 模式的 claim 竞争导致重复投递。
    pub(crate) fn drain_by_parent(&self, parent_agent_id: &Uuid) -> Vec<BackgroundTask> {
        let mut inner = self.inner.lock().unwrap();
        let completed_ids: Vec<Uuid> = inner
            .tasks
            .iter()
            .filter(|(_, t)| {
                t.parent_agent_id == *parent_agent_id
                    && t.delivery_state == DeliveryState::Pending
                    && matches!(
                        t.status,
                        BackgroundTaskStatus::Completed
                            | BackgroundTaskStatus::Failed
                            | BackgroundTaskStatus::Stale
                    )
            })
            .map(|(id, _)| *id)
            .collect();
        let mut results = Vec::with_capacity(completed_ids.len());
        for id in &completed_ids {
            if let Some(task) = inner.tasks.remove(id) {
                // 从 by_delegation 中移除
                if let Some(children) = inner.by_delegation.get_mut(&task.delegation_id) {
                    children.retain(|c| c != id);
                    if children.is_empty() {
                        inner.by_delegation.remove(&task.delegation_id);
                    }
                }
                results.push(task);
            }
        }
        results
    }

    /// 取出某个会话的所有已完成后台结果（取出后清除，用于跨轮次回注）
    ///
    /// 会话路由：优先按 origin_session_id 匹配，其次按 session_id。
    /// 仅取出 delivery_state=Pending 的任务，避免与 push 模式的 claim 竞争导致重复投递。
    pub(crate) fn drain_by_session(&self, session_id: &str) -> Vec<BackgroundTask> {
        let mut inner = self.inner.lock().unwrap();
        let completed_ids: Vec<Uuid> = inner
            .tasks
            .iter()
            .filter(|(_, t)| {
                let matches_origin = t.origin_session_id.as_deref() == Some(session_id);
                let matches_session =
                    t.origin_session_id.is_none() && t.session_id.as_deref() == Some(session_id);
                (matches_origin || matches_session)
                    && t.delivery_state == DeliveryState::Pending
                    && matches!(
                        t.status,
                        BackgroundTaskStatus::Completed
                            | BackgroundTaskStatus::Failed
                            | BackgroundTaskStatus::Stale
                            | BackgroundTaskStatus::Unknown
                    )
            })
            .map(|(id, _)| *id)
            .collect();
        let mut results = Vec::with_capacity(completed_ids.len());
        for id in &completed_ids {
            if let Some(task) = inner.tasks.remove(id) {
                if let Some(children) = inner.by_delegation.get_mut(&task.delegation_id) {
                    children.retain(|c| c != id);
                    if children.is_empty() {
                        inner.by_delegation.remove(&task.delegation_id);
                    }
                }
                results.push(task);
            }
        }
        results
    }

    /// 认领指定会话的所有 Pending 已完成任务（exactly-once 投递）
    ///
    /// 将 delivery_state 从 Pending 改为 Claimed，写入 claim_id + claimed_at，返回任务列表。
    /// 调用方在成功注入后应调用 `complete_delivery` 确认；失败则调用 `release_claim` 重试。
    ///
    /// `consumer_id` 用于生成跨进程互斥的 claim 令牌（`consumer:pid:uuid`，参考）。
    /// 若已有 claim 但超过 CLAIM_TTL_SECS（认领进程可能崩溃），允许重新认领。
    pub(crate) fn claim_completed(
        &self,
        session_id: &str,
        consumer_id: &str,
    ) -> Vec<BackgroundTask> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let pid = std::process::id();
        let mut inner = self.inner.lock().unwrap();
        let ids: Vec<Uuid> = inner
            .tasks
            .iter()
            .filter(|(_, t)| {
                let matches_origin = t.origin_session_id.as_deref() == Some(session_id);
                let matches_session =
                    t.origin_session_id.is_none() && t.session_id.as_deref() == Some(session_id);
                let session_match = matches_origin || matches_session;
                let is_terminal = matches!(
                    t.status,
                    BackgroundTaskStatus::Completed
                        | BackgroundTaskStatus::Failed
                        | BackgroundTaskStatus::Stale
                        | BackgroundTaskStatus::Unknown
                );
                // 可认领条件：Pending，或 Claimed 但已超时（认领进程崩溃）
                let claimable = t.delivery_state == DeliveryState::Pending
                    || (t.delivery_state == DeliveryState::Claimed
                        && t.delivery_claimed_at
                            .map(|t| now - t > CLAIM_TTL_SECS)
                            .unwrap_or(true));
                session_match && is_terminal && claimable
            })
            .map(|(id, _)| *id)
            .collect();
        let mut results = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(task) = inner.tasks.get_mut(id) {
                let claim_id = format!("{}:{}:{}", consumer_id, pid, Uuid::new_v4());
                task.delivery_state = DeliveryState::Claimed;
                task.delivery_attempts += 1;
                task.delivery_claim = Some(claim_id);
                task.delivery_claimed_at = Some(now);
                results.push(task.clone());
            }
        }
        results
    }

    /// 确认投递成功，删除任务（exactly-once 投递）
    ///
    /// `claim_ids` 与 `child_agent_ids` 一一对应；仅当任务的 delivery_claim 匹配时才删除，
    /// 防止非认领者误删（参考 complete_completion_delivery 的 claim 校验）。
    ///
    /// 返回实际被删除的任务 ID 列表，调用方据此清理持久化记录，
    /// 避免 claim 不匹配的任务持久化被误删导致崩溃后丢失。
    pub(crate) fn complete_delivery(
        &self,
        child_agent_ids: &[Uuid],
        claim_ids: &[Option<String>],
    ) -> Vec<Uuid> {
        let mut inner = self.inner.lock().unwrap();
        let mut removed = Vec::new();
        for (id, claim_id) in child_agent_ids.iter().zip(claim_ids.iter()) {
            // 校验 claim：若提供了 claim_id，则必须匹配；无 claim_id 的旧任务直接删除
            if let Some(claim) = claim_id {
                if let Some(task) = inner.tasks.get(id) {
                    if task.delivery_claim.as_deref() != Some(claim.as_str()) {
                        tracing::warn!(
                            "[delivery] complete_delivery claim mismatch for {}: expected {:?}, got {:?}",
                            id, task.delivery_claim, claim_id
                        );
                        continue;
                    }
                }
            }
            if let Some(task) = inner.tasks.remove(id) {
                if let Some(children) = inner.by_delegation.get_mut(&task.delegation_id) {
                    children.retain(|c| c != id);
                    if children.is_empty() {
                        inner.by_delegation.remove(&task.delegation_id);
                    }
                }
                removed.push(*id);
            }
        }
        removed
    }

    /// 释放认领，回退到 Pending 以便重试（exactly-once 投递）
    ///
    /// 清除 delivery_claim / delivery_claimed_at。
    /// `claim_ids` 与 `child_agent_ids` 一一对应；仅当 claim 匹配时才释放。
    /// 若投递尝试次数超过 MAX_DELIVERY_ATTEMPTS，标记为 Dropped 并不再重试。
    pub(crate) fn release_claim(
        &self,
        child_agent_ids: &[Uuid],
        claim_ids: &[Option<String>],
    ) -> Vec<Uuid> {
        let mut inner = self.inner.lock().unwrap();
        let mut dropped = Vec::new();
        for (id, claim_id) in child_agent_ids.iter().zip(claim_ids.iter()) {
            if let Some(task) = inner.tasks.get_mut(id) {
                // 校验 claim
                if let Some(claim) = claim_id {
                    if task.delivery_claim.as_deref() != Some(claim.as_str()) {
                        continue;
                    }
                }
                // 清除认领令牌
                task.delivery_claim = None;
                task.delivery_claimed_at = None;
                if task.delivery_attempts >= MAX_DELIVERY_ATTEMPTS {
                    task.delivery_state = DeliveryState::Dropped;
                    dropped.push(*id);
                } else {
                    task.delivery_state = DeliveryState::Pending;
                }
            }
        }
        dropped
    }

    /// 获取某个父 Agent 的所有任务（包括运行中，不清除）
    pub(crate) fn list_by_parent(&self, parent_agent_id: &Uuid) -> Vec<BackgroundTask> {
        let inner = self.inner.lock().unwrap();
        inner
            .tasks
            .values()
            .filter(|t| t.parent_agent_id == *parent_agent_id)
            .cloned()
            .collect()
    }

    /// 获取所有运行中的任务（用于陈旧检测扫描）
    pub(crate) fn list_running(&self) -> Vec<BackgroundTask> {
        let inner = self.inner.lock().unwrap();
        inner
            .tasks
            .values()
            .filter(|t| t.status == BackgroundTaskStatus::Running)
            .cloned()
            .collect()
    }

    /// 检查某个父 Agent 是否有待取回的后台结果
    ///
    /// 仅当任务处于终态且 delivery_state == Pending 时返回 true，
    /// 与 drain_by_parent 的过滤条件保持一致，避免对已被 claim 的任务误报。
    pub(crate) fn has_for_parent(&self, parent_agent_id: &Uuid) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.tasks.values().any(|t| {
            t.parent_agent_id == *parent_agent_id
                && t.delivery_state == DeliveryState::Pending
                && matches!(
                    t.status,
                    BackgroundTaskStatus::Completed
                        | BackgroundTaskStatus::Failed
                        | BackgroundTaskStatus::Stale
                )
        })
    }
}