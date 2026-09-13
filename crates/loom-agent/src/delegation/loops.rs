use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::registry::BackgroundTaskRegistry;
use super::types::{SessionWaker, HEARTBEAT_INTERVAL_SECS, STALE_CYCLES_IDLE, STALE_CYCLES_IN_TOOL};
use loom_core::ActivitySummary;
use loom_core::AgentLifecycleManager;
use loom_core::JsonKeyValueStore;

/// P2: 推模式完成投递守护循环
///
/// 参考 `_completion_watcher_loop()`：
/// - 监听后台任务完成广播
/// - 对同一会话的多个完成在 500ms 窗口内批量合并（批量合并投递）
/// - 通过 SessionWaker 唤醒父会话处理结果（唤醒机制）
///
/// 投递语义：仅唤醒，不直接注入历史。注入由被唤醒的会话在自身上下文中完成，
/// 确保结果进入正确的会话历史（会话路由 + 线程安全）。
///
/// `shutdown`：当 CancellationToken 被取消时，循环优雅退出。
pub(super) async fn completion_watcher_loop(
    registry: BackgroundTaskRegistry,
    session_waker: Arc<std::sync::Mutex<Option<Arc<dyn SessionWaker>>>>,
    shutdown: CancellationToken,
) {
    let mut rx = registry.subscribe_completions();
    // session_id → 合并窗口中的任务集合
    let mut pending: HashMap<String, std::collections::HashSet<Uuid>> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // 收到完成通知：合并到对应会话的待投递集合
            note = rx.recv() => {
                match note {
                    Ok(n) => {
                        pending
                            .entry(n.session_id)
                            .or_default()
                            .insert(n.child_agent_id);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
            // 500ms 窗口到期：批量唤醒所有有待投递结果的会话
            _ = ticker.tick() => {
                if pending.is_empty() {
                    continue;
                }
                let sessions: Vec<String> = pending.keys().cloned().collect();
                let waker = session_waker.lock().unwrap().clone();
                for sid in sessions {
                    if let Some(ref w) = waker {
                        tracing::debug!(
                            "[completion-watcher] waking session {} ({} pending results)",
                            sid,
                            pending.get(&sid).map(|s| s.len()).unwrap_or(0)
                        );
                        w.wake(&sid);
                    }
                }
                pending.clear();
            }
            // 关闭信号：优雅退出
            _ = shutdown.cancelled() => {
                tracing::info!("[completion-watcher] shutting down");
                break;
            }
        }
    }
}

/// 后台陈旧检测守护循环
///
/// 定期扫描运行中的后台子 Agent，检测卡死并自动终止。
/// 与 sync 委派的心跳检测不同，此处针对后台任务（无需等待结果）。
///
/// `shutdown`：当 CancellationToken 被取消时，循环优雅退出。
pub(super) async fn stale_monitor_loop(
    lifecycle: Arc<dyn AgentLifecycleManager>,
    registry: BackgroundTaskRegistry,
    persistence: Arc<std::sync::Mutex<Option<Arc<dyn JsonKeyValueStore>>>>,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // 每个子 Agent 的陈旧计数与上次活动状态
    let mut stale_counts: HashMap<Uuid, u32> = HashMap::new();
    let mut last_iter: HashMap<Uuid, u64> = HashMap::new();
    let mut last_tool: HashMap<Uuid, Option<String>> = HashMap::new();
    let mut last_ts: HashMap<Uuid, Option<chrono::DateTime<chrono::Utc>>> = HashMap::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.cancelled() => {
                tracing::info!("[stale-monitor] shutting down");
                break;
            }
        }

        let running = registry.list_running();
        if running.is_empty() {
            continue;
        }

        for task in running {
            let agent_id = task.child_agent_id;
            let summary = match lifecycle.get_activity_summary(&agent_id).await {
                Ok(s) => s,
                Err(_) => continue,
            };

            // P2: 存入实时转录快照
            registry.push_activity_snapshot(&agent_id, &summary);

            let ActivitySummary {
                iterations,
                current_tool,
                last_activity_ts,
                ..
            } = &summary;

            // 判断是否有进展
            let prev_iter = last_iter.get(&agent_id).copied().unwrap_or(0);
            let prev_tool = last_tool.get(&agent_id).cloned().flatten();
            let prev_ts = last_ts.get(&agent_id).copied().flatten();

            let advanced = *iterations > prev_iter
                || current_tool.as_deref() != prev_tool.as_deref()
                || (last_activity_ts.is_some()
                    && (prev_ts.is_none() || last_activity_ts > &prev_ts));

            if advanced {
                stale_counts.insert(agent_id, 0);
                last_iter.insert(agent_id, *iterations);
                last_tool.insert(agent_id, current_tool.clone());
                last_ts.insert(agent_id, *last_activity_ts);
            } else {
                let count = stale_counts.entry(agent_id).or_insert(0);
                *count += 1;
                let threshold = if current_tool.is_some() {
                    STALE_CYCLES_IN_TOOL
                } else {
                    STALE_CYCLES_IDLE
                };
                if *count >= threshold {
                    let elapsed_secs = *count as u64 * HEARTBEAT_INTERVAL_SECS;
                    tracing::warn!(
                        "[stale-monitor] background agent {} appears stale: no progress for {} cycles ({}s), \
                         current_tool={:?}. Stopping.",
                        agent_id,
                        *count,
                        elapsed_secs,
                        current_tool
                    );
                    let _ = lifecycle.stop(&agent_id).await;
                    let stale_task = registry.mark_stale(
                        &agent_id,
                        &format!(
                            "background agent {} timed out (stuck for {}s, no progress, tool={:?})",
                            agent_id, elapsed_secs, current_tool
                        ),
                    );
                    // 持久化：卡死状态写入存储
                    if let Some(task) = stale_task {
                        if let Some(kv) = persistence.lock().unwrap().clone() {
                            let key = task.child_agent_id.to_string();
                            if let Ok(value) = serde_json::to_value(&task) {
                                tokio::spawn(async move {
                                    if let Err(e) = kv
                                        .put(super::types::PERSISTENCE_NAMESPACE, &key, value)
                                        .await
                                    {
                                        tracing::warn!(
                                            "[persistence] failed to persist stale task {}: {}",
                                            key,
                                            e
                                        );
                                    }
                                });
                            }
                        }
                    }
                    // 清理该 Agent 的跟踪状态
                    stale_counts.remove(&agent_id);
                    last_iter.remove(&agent_id);
                    last_tool.remove(&agent_id);
                    last_ts.remove(&agent_id);
                }
            }
        }
    }
}