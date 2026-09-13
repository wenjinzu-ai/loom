use super::*;
use crate::config::AgentLoopConfig;
use crate::result::SubagentResult;
use loom_core::{ActivitySummary, Result};
use loom_infra::InMemoryJsonStore;
use std::time::Duration;

fn make_manager() -> DelegationManager {
    let lifecycle = Arc::new(MockLifecycle::default());
    DelegationManager::new(lifecycle, AgentLoopConfig::default())
}

#[tokio::test]
async fn recover_persisted_without_store_returns_zero() {
    let mgr = make_manager();
    assert_eq!(mgr.recover_persisted().await, 0);
}

#[tokio::test]
async fn recover_persisted_restores_completed_and_marks_running_unknown() {
    let mgr = make_manager();
    let kv: Arc<dyn JsonKeyValueStore> = Arc::new(InMemoryJsonStore::new());
    mgr.with_persistence(kv.clone());

    let parent = Uuid::new_v4();
    let session = "sess-1".to_string();

    // 模拟两个任务：一个已完成，一个仍在 Running
    let completed = BackgroundTask {
        delegation_id: Uuid::new_v4(),
        child_agent_id: Uuid::new_v4(),
        goal: "done task".into(),
        status: BackgroundTaskStatus::Completed,
        parent_agent_id: parent,
        session_id: Some(session.clone()),
        origin_session_id: None,
        dispatched_at: 0.0,
        completed_at: Some(1.0),
        result: Some(SubagentResult::from_success("ok".into(), 3, 2, &[])),
        live_transcript: vec![],
        delivery_state: DeliveryState::Pending,
        delivery_attempts: 0,
        delivery_claim: None,
        delivery_claimed_at: None,
        owner_instance_id: Some("other-instance".into()),
        tenant_id: None,
        user_id: None,
    };
    let running = BackgroundTask {
        delegation_id: Uuid::new_v4(),
        child_agent_id: Uuid::new_v4(),
        goal: "in-flight task".into(),
        status: BackgroundTaskStatus::Running,
        parent_agent_id: parent,
        session_id: Some(session.clone()),
        origin_session_id: None,
        dispatched_at: 0.0,
        completed_at: None,
        result: None,
        live_transcript: vec![],
        delivery_state: DeliveryState::Pending,
        delivery_attempts: 0,
        delivery_claim: None,
        delivery_claimed_at: None,
        owner_instance_id: Some("other-instance".into()),
        tenant_id: None,
        user_id: None,
    };

    // 直接写入 KV 存储（模拟持久化）
    kv.put(
        PERSISTENCE_NAMESPACE,
        &completed.child_agent_id.to_string(),
        serde_json::to_value(&completed).unwrap(),
    )
    .await
    .unwrap();
    kv.put(
        PERSISTENCE_NAMESPACE,
        &running.child_agent_id.to_string(),
        serde_json::to_value(&running).unwrap(),
    )
    .await
    .unwrap();

    // 恢复
    let recovered = mgr.recover_persisted().await;
    assert_eq!(recovered, 2);

    // 已完成的任务保持 Completed，可被 drain 取出
    let results = mgr.drain_background_results_by_session(&session);
    assert_eq!(results.len(), 2);
    // Running 的任务恢复后变为 Unknown，并带有 error 结果
    let unknown = results
        .iter()
        .find(|(_, goal, _)| goal == "in-flight task")
        .unwrap();
    assert!(!unknown.2.success);
    let err_msg = unknown.2.error_message.as_deref().unwrap_or_default();
    assert!(err_msg.contains("process restarted"));
}

#[tokio::test]
async fn persist_task_writes_to_store() {
    let mgr = make_manager();
    let kv: Arc<dyn JsonKeyValueStore> = Arc::new(InMemoryJsonStore::new());
    mgr.with_persistence(kv.clone());

    let task = BackgroundTask {
        delegation_id: Uuid::new_v4(),
        child_agent_id: Uuid::new_v4(),
        goal: "test".into(),
        status: BackgroundTaskStatus::Running,
        parent_agent_id: Uuid::new_v4(),
        session_id: None,
        origin_session_id: None,
        dispatched_at: 0.0,
        completed_at: None,
        result: None,
        live_transcript: vec![],
        delivery_state: DeliveryState::Pending,
        delivery_attempts: 0,
        delivery_claim: None,
        delivery_claimed_at: None,
        owner_instance_id: None,
        tenant_id: None,
        user_id: None,
    };
    mgr.persist_task(&task);

    // 等待异步持久化完成
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stored = kv
        .get(PERSISTENCE_NAMESPACE, &task.child_agent_id.to_string())
        .await
        .unwrap();
    assert!(stored.is_some());
    let stored_task: BackgroundTask = serde_json::from_value(stored.unwrap()).unwrap();
    assert_eq!(stored_task.goal, "test");
    assert_eq!(stored_task.status, BackgroundTaskStatus::Running);
}

/// 最小化的 MockLifecycle，仅用于构造 DelegationManager（不触发实际启动）
#[derive(Default)]
struct MockLifecycle;

#[async_trait::async_trait]
impl AgentLifecycleManager for MockLifecycle {
    async fn launch(&self, _req: loom_core::AgentLaunchRequest) -> Result<loom_core::AgentHandle> {
        Ok(loom_core::AgentHandle {
            agent_id: Uuid::new_v4(),
            state: loom_core::AgentState::Running,
            isolation: loom_core::IsolationLevel::Coroutine,
            parent_agent_id: None,
            created_at: chrono::Utc::now(),
            goal: "mock".into(),
        })
    }
    async fn stop(&self, _id: &Uuid) -> Result<()> {
        Ok(())
    }
    async fn pause(&self, _id: &Uuid) -> Result<()> {
        Ok(())
    }
    async fn resume(&self, _id: &Uuid) -> Result<()> {
        Ok(())
    }
    async fn destroy(&self, _id: &Uuid) -> Result<()> {
        Ok(())
    }
    async fn get_status(&self, _id: &Uuid) -> Result<loom_core::AgentHandle> {
        todo!()
    }
    async fn list(&self) -> Result<Vec<loom_core::AgentHandle>> {
        Ok(vec![])
    }
    async fn send_message(&self, _id: &Uuid, _msg: loom_core::AgentMessage) -> Result<()> {
        Ok(())
    }
    async fn get_activity_summary(&self, _id: &Uuid) -> Result<ActivitySummary> {
        Ok(ActivitySummary::default())
    }
    async fn wait_for_result(&self, _id: &Uuid) -> Result<loom_core::AgentOutput> {
        Ok(loom_core::AgentOutput {
            success: true,
            summary: "ok".into(),
            error_message: None,
            error_kind: loom_core::AgentErrorKind::None,
            iterations: 1,
            tool_calls_made: 0,
            tool_names: vec![],
            duration_ms: 10,
            tool_trace: None,
        })
    }
}