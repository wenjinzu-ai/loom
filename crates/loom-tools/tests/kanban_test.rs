use loom_tools::kanban::KanbanStore;
use loom_tools::ToolSet;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn kanban_full_workflow() {
    let store = KanbanStore::new("127.0.0.1", 5432, "postgres", "postgres", "postgres")
        .await
        .expect("connect to postgres");
    store.migrate().await.expect("migrate");

    let toolset = loom_tools::kanban::KanbanToolSet::new(Arc::new(store));

    // create
    let created = toolset
        .execute(
            "kanban_create", json!({
                "title": "test task",
                "assignee": "worker-a",
                "body": "do something",
                "priority": 5,
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(created["success"], true);
    let task_id = created["task_id"].as_str().unwrap().to_string();
    println!("created task: {task_id}");

    // show
    let shown = toolset
        .execute("kanban_show", json!({"task_id": task_id}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(shown["success"], true);
    assert_eq!(shown["task"]["title"], "test task");
    assert_eq!(shown["task"]["assignee"], "worker-a");
    assert_eq!(shown["task"]["status"], "running");

    // list
    let listed = toolset
        .execute("kanban_list", json!({"assignee": "worker-a"}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(listed["success"], true);
    assert!(listed["count"].as_i64().unwrap() >= 1);

    // comment
    let commented = toolset
        .execute(
            "kanban_comment", json!({"task_id": task_id, "body": "hello"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(commented["success"], true);

    // block
    let blocked = toolset
        .execute(
            "kanban_block", json!({"task_id": task_id, "reason": "waiting"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(blocked["success"], true);

    // unblock
    let unblocked = toolset
        .execute("kanban_unblock", json!({"task_id": task_id}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(unblocked["success"], true);

    // complete
    let completed = toolset
        .execute(
            "kanban_complete", json!({"task_id": task_id, "summary": "done", "result": "ok"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(completed["success"], true);

    // show again - should be done
    let shown = toolset
        .execute("kanban_show", json!({"task_id": task_id}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(shown["task"]["status"], "done");

    // link: create a child task and link
    let child = toolset
        .execute(
            "kanban_create", json!({"title": "child", "assignee": "worker-b"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    let child_id = child["task_id"].as_str().unwrap().to_string();
    let linked = toolset
        .execute(
            "kanban_link", json!({"parent_id": task_id, "child_id": child_id}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(linked["success"], true);

    // self-link should fail
    let self_link = toolset
        .execute(
            "kanban_link", json!({"parent_id": child_id, "child_id": child_id}), &loom_core::ToolContext::default(),
        )
        .await;
    assert!(self_link.is_err());

    println!("kanban full workflow PASSED");
}
