use loom_tools::clarify::ClarifyToolSet;
use loom_tools::memory::MemoryToolSet;
use loom_tools::spec::ToolSet;
use loom_tools::todo::TodoToolSet;
use serde_json::json;

#[tokio::test]
async fn test_memory_add_and_read() {
    let set = MemoryToolSet::new();
    let result = set
        .execute(
            "memory", json!({"action": "add", "target": "memory", "content": "User likes Rust"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["success"], true);

    let result = set
        .execute(
            "memory", json!({"action": "add", "target": "memory", "content": "Project is Loom"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["memory"], "User likes Rust\nProject is Loom");
}

#[tokio::test]
async fn test_memory_replace() {
    let set = MemoryToolSet::new();
    set.execute("memory", json!({"action": "add", "content": "fact one"}), &loom_core::ToolContext::default())
        .await
        .unwrap();

    let result = set
        .execute(
            "memory", json!({"action": "replace", "old_text": "fact one", "content": "fact updated"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["memory"], "fact updated");
}

#[tokio::test]
async fn test_memory_remove() {
    let set = MemoryToolSet::new();
    set.execute("memory", json!({"action": "add", "content": "keep"}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    set.execute("memory", json!({"action": "add", "content": "remove me"}), &loom_core::ToolContext::default())
        .await
        .unwrap();

    let result = set
        .execute(
            "memory", json!({"action": "remove", "old_text": "remove me"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["memory"], "keep");
}

#[tokio::test]
async fn test_memory_clear() {
    let set = MemoryToolSet::new();
    set.execute("memory", json!({"action": "add", "content": "something"}), &loom_core::ToolContext::default())
        .await
        .unwrap();

    let result = set
        .execute("memory", json!({"action": "clear"}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(result["memory"], "");
}

#[tokio::test]
async fn test_memory_batch_operations() {
    let set = MemoryToolSet::new();
    let result = set
        .execute(
            "memory", json!({
                "operations": [
                    {"action": "add", "content": "batch item 1"},
                    {"action": "add", "content": "batch item 2"},
                    {"action": "add", "target": "user", "content": "user profile"}
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["operations"], 3);
    assert!(result["memory"].as_str().unwrap().contains("batch item 1"));
    assert!(result["memory"].as_str().unwrap().contains("batch item 2"));
    assert!(result["user"].as_str().unwrap().contains("user profile"));
}

#[tokio::test]
async fn test_memory_replace_not_found() {
    let set = MemoryToolSet::new();
    let result = set
        .execute(
            "memory", json!({"action": "replace", "old_text": "nonexistent", "content": "x"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["success"], false);
}

#[tokio::test]
async fn test_todo_replace_mode() {
    let set = TodoToolSet::new();
    let result = set
        .execute(
            "todo_list", json!({
                "todos": [
                    {"id": "1", "content": "Step 1", "status": "in_progress"},
                    {"id": "2", "content": "Step 2", "status": "pending"}
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["total"], 2);
    assert_eq!(result["in_progress"], 1);
    let todos = result["todos"].as_array().unwrap();
    assert_eq!(todos[0]["content"], "Step 1");
    assert_eq!(todos[0]["status"], "in_progress");
}

#[tokio::test]
async fn test_todo_merge_mode() {
    let set = TodoToolSet::new();
    set.execute(
        "todo_list", json!({
            "todos": [
                {"id": "1", "content": "Step 1", "status": "in_progress"},
                {"id": "2", "content": "Step 2", "status": "pending"}
            ]
        }), &loom_core::ToolContext::default(),
    )
    .await
    .unwrap();

    let result = set
        .execute(
            "todo_list", json!({
                "merge": true,
                "todos": [
                    {"id": "1", "content": "Step 1", "status": "completed"},
                    {"id": "3", "content": "Step 3", "status": "pending"}
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["total"], 3);
    assert_eq!(result["completed"], 1);
}

#[tokio::test]
async fn test_todo_only_one_in_progress() {
    let set = TodoToolSet::new();
    let result = set
        .execute(
            "todo_list", json!({
                "todos": [
                    {"id": "1", "content": "Step 1", "status": "in_progress"},
                    {"id": "2", "content": "Step 2", "status": "in_progress"}
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["in_progress"], 1);
}

#[tokio::test]
async fn test_todo_read_empty() {
    let set = TodoToolSet::new();
    let result = set.execute("todo_list", json!({}), &loom_core::ToolContext::default()).await.unwrap();
    assert_eq!(result["total"], 0);
    assert!(result["todos"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_todo_with_parent() {
    let set = TodoToolSet::new();
    let result = set
        .execute(
            "todo_list", json!({
                "todos": [
                    {"id": "1", "content": "Parent", "status": "in_progress"},
                    {"id": "1a", "content": "Subtask", "status": "pending", "parent": "1"}
                ]
            }), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    let todos = result["todos"].as_array().unwrap();
    assert_eq!(todos[1]["parent"], "1");
}

#[tokio::test]
async fn test_show_message() {
    let set = ClarifyToolSet;
    let result = set
        .execute(
            "show_message", json!({"message": "Hello world", "level": "info"}), &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["displayed"], true);
}

#[tokio::test]
async fn test_show_message_empty_fails() {
    let set = ClarifyToolSet;
    let result = set.execute("show_message", json!({"message": ""}), &loom_core::ToolContext::default()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_ask_user_requires_questions() {
    let set = ClarifyToolSet;
    let result = set.execute("ask_user", json!({"questions": []}), &loom_core::ToolContext::default()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_ask_user_returns_pending() {
    let set = ClarifyToolSet;
    let result = set
        .execute("ask_user", json!({"questions": ["What is your name?"]}), &loom_core::ToolContext::default())
        .await
        .unwrap();
    assert_eq!(result["success"], true);
    assert_eq!(result["pending_user_input"], true);
}
