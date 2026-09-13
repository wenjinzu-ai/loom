use std::collections::HashMap;

use loom_core::{Result};
use serde_json::{json, Value};
use uuid::Uuid;

use super::DelegationManager;
use super::types::BackgroundTaskStatus;

impl DelegationManager {
    /// 列出指定父 Agent 下的所有子 Agent
    pub(super) async fn list_sub_agents(&self, parent_agent_id: Uuid) -> Result<Value> {
        let all = self.lifecycle.list().await.unwrap_or_default();
        let children: Vec<Value> = all
            .into_iter()
            .filter(|h| h.parent_agent_id == Some(parent_agent_id))
            .map(|h| {
                json!({
                    "agent_id": h.agent_id.to_string(),
                    "goal": h.goal,
                    "state": h.state.to_string(),
                    "created_at": h.created_at.to_rfc3339()
                })
            })
            .collect();

        // P1: 从 Registry 获取所有后台任务（运行中 + 已完成）
        let all_tasks = self.registry.list_by_parent(&parent_agent_id);

        // P2: 成本聚合（所有已完成任务的 duration/iterations/tool_calls 求和）
        let mut total_duration_ms: u64 = 0;
        let mut total_iterations: usize = 0;
        let mut total_tool_calls: usize = 0;
        for task in &all_tasks {
            if let Some(ref result) = task.result {
                total_duration_ms += result.duration_ms;
                total_iterations += result.usage.iterations;
                total_tool_calls += result.usage.tool_calls;
            }
        }
        let cost_aggregation = json!({
            "total_duration_ms": total_duration_ms,
            "total_iterations": total_iterations,
            "total_tool_calls": total_tool_calls,
            "task_count": all_tasks.len()
        });

        // 运行中的任务（含 P2 实时转录快照）
        let running: Vec<Value> = all_tasks
            .iter()
            .filter(|t| t.status == BackgroundTaskStatus::Running)
            .map(|t| {
                let transcript: Vec<Value> = t
                    .live_transcript
                    .iter()
                    .map(|s| {
                        json!({
                            "timestamp": s.timestamp,
                            "iterations": s.iterations,
                            "api_call_count": s.api_call_count,
                            "current_tool": s.current_tool,
                            "last_activity_ts": s.last_activity_ts
                        })
                    })
                    .collect();
                json!({
                    "delegation_id": t.delegation_id.to_string(),
                    "agent_id": t.child_agent_id.to_string(),
                    "goal": t.goal,
                    "status": "running",
                    "dispatched_at": t.dispatched_at,
                    "live_transcript": transcript
                })
            })
            .collect();

        // 已完成的后台结果（取出后从 Registry 清除，避免重复返回）
        let completed = self.registry.drain_by_parent(&parent_agent_id);
        // 持久化清理：取出后从存储中删除
        for t in &completed {
            self.delete_persisted(&t.child_agent_id);
        }
        let completed_results: Vec<Value> = completed
            .into_iter()
            .map(|t| {
                let mut obj = t
                    .result
                    .map(|r| r.to_json())
                    .unwrap_or_else(|| json!({"success": false, "summary": "no result"}));
                obj["delegation_id"] = Value::String(t.delegation_id.to_string());
                obj["agent_id"] = Value::String(t.child_agent_id.to_string());
                obj["goal"] = Value::String(t.goal);
                obj["status"] = Value::String(match t.status {
                    BackgroundTaskStatus::Completed => "completed".to_string(),
                    BackgroundTaskStatus::Failed => "failed".to_string(),
                    BackgroundTaskStatus::Stale => "stale".to_string(),
                    _ => "unknown".to_string(),
                });
                if let Some(completed_at) = t.completed_at {
                    obj["completed_at"] = Value::from(completed_at);
                }
                obj
            })
            .collect();

        // P1: 批次视图（按 delegation_id 分组，显示每个批次的整体状态）
        let mut batch_map: HashMap<String, Vec<&super::types::BackgroundTask>> = HashMap::new();
        for task in &all_tasks {
            batch_map
                .entry(task.delegation_id.to_string())
                .or_default()
                .push(task);
        }
        let batches: Vec<Value> = batch_map
            .into_iter()
            .map(|(delegation_id, tasks)| {
                let total = tasks.len();
                let completed_count = tasks
                    .iter()
                    .filter(|t| {
                        matches!(
                            t.status,
                            BackgroundTaskStatus::Completed
                                | BackgroundTaskStatus::Failed
                                | BackgroundTaskStatus::Stale
                        )
                    })
                    .count();
                let failed_count = tasks
                    .iter()
                    .filter(|t| {
                        matches!(
                            t.status,
                            BackgroundTaskStatus::Failed | BackgroundTaskStatus::Stale
                        )
                    })
                    .count();
                let batch_status = if completed_count == total {
                    if failed_count > 0 {
                        "partial_failed"
                    } else {
                        "all_completed"
                    }
                } else if completed_count > 0 {
                    "partial_completed"
                } else {
                    "running"
                };
                json!({
                    "delegation_id": delegation_id,
                    "total_tasks": total,
                    "completed_tasks": completed_count,
                    "failed_tasks": failed_count,
                    "status": batch_status
                })
            })
            .collect();

        Ok(json!({
            "children": children,
            "running": running,
            "completed": completed_results,
            "batches": batches,
            "cost_aggregation": cost_aggregation
        }))
    }

    /// 停止指定子 Agent
    ///
    /// 参考：父子权限校验 —— 只能停止父 Agent 为自己的子 Agent
    pub(super) async fn stop_sub_agent(&self, args: &Value, current_agent_id: Uuid) -> Result<Value> {
        let subagent_id = args["subagent_id"].as_str().ok_or_else(|| {
            loom_core::LoomError::Other("subagent_id is required for action=stop".into())
        })?;

        let id = Uuid::parse_str(subagent_id).map_err(|_| {
            loom_core::LoomError::Other(format!("invalid subagent_id: {subagent_id}"))
        })?;

        // 所有权校验：子 Agent 的 parent 必须是当前 Agent
        let is_owned = self
            .lifecycle
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|h| h.agent_id == id && h.parent_agent_id == Some(current_agent_id));

        if !is_owned {
            return Ok(json!({
                "error": format!("agent {subagent_id} is not a child of the current agent")
            }));
        }

        self.lifecycle.stop(&id).await?;
        Ok(json!({ "stopped": subagent_id }))
    }

    /// 向子 Agent 追加 steering 消息（参考 steer_agent）
    ///
    /// 仅作用于当前 Agent 直接启动的子 Agent（parent_agent_id 校验）。
    pub(super) async fn steer_sub_agent(&self, args: &Value, current_agent_id: Uuid) -> Result<Value> {
        let subagent_id = args["subagent_id"].as_str().ok_or_else(|| {
            loom_core::LoomError::Other("subagent_id is required for action=steer".into())
        })?;

        let id = Uuid::parse_str(subagent_id).map_err(|_| {
            loom_core::LoomError::Other(format!("invalid subagent_id: {subagent_id}"))
        })?;

        let message = args["message"].as_str().ok_or_else(|| {
            loom_core::LoomError::Other("message is required for action=steer".into())
        })?;

        // 所有权校验
        let is_owned = self
            .lifecycle
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|h| h.agent_id == id && h.parent_agent_id == Some(current_agent_id));

        if !is_owned {
            return Ok(json!({
                "error": format!("agent {subagent_id} is not a child of the current agent")
            }));
        }

        let msg = loom_core::AgentMessage::new(
            current_agent_id,
            id,
            loom_core::MessageContent::Text(format!("[steer from parent] {message}")),
        );

        match self.lifecycle.send_message(&id, msg).await {
            Ok(_) => Ok(json!({
                "status": "message_sent",
                "subagent_id": subagent_id
            })),
            Err(e) => Ok(json!({
                "error": format!("steer failed: {e}")
            })),
        }
    }
}