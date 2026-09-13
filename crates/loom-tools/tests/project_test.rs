use loom_tools::project::ProjectToolSet;
use loom_tools::spec::ToolSet;
use serde_json::json;
use tempfile::TempDir;

fn run(args: serde_json::Value, db_path: &std::path::Path) -> serde_json::Value {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let set = ProjectToolSet::with_db_path(db_path);
    rt.block_on(async move { set.execute("desktop_project", args, &loom_core::ToolContext::default()).await.unwrap() })
}

#[test]
fn test_list_empty() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");
    let result = run(json!({"action": "list"}), &db);
    assert_eq!(result["success"], true);
    assert_eq!(result["count"], 0);
    assert!(result["active"].is_null());
}

#[test]
fn test_create_and_list() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    let created = run(
        json!({"action": "create", "name": "My Project", "path": "/repo"}),
        &db,
    );
    assert_eq!(created["success"], true);
    assert_eq!(created["slug"], "my-project");
    assert_eq!(created["name"], "My Project");
    assert_eq!(created["path"], "/repo");
    assert_eq!(created["active"], true);

    let listed = run(json!({"action": "list"}), &db);
    assert_eq!(listed["count"], 1);
    assert_eq!(listed["active"], "my-project");
    assert_eq!(listed["projects"][0]["slug"], "my-project");
}

#[test]
fn test_create_duplicate_slug_fails() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    run(json!({"action": "create", "name": "Hello World"}), &db);
    let dup = run(json!({"action": "create", "name": "hello-world"}), &db);
    assert_eq!(dup["success"], false);
    assert!(dup["error"].as_str().unwrap().contains("already exists"));
}

#[test]
fn test_create_without_name_fails() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let set = ProjectToolSet::with_db_path(&db);
    let result = rt.block_on(async {
        set.execute("desktop_project", json!({"action": "create"}), &loom_core::ToolContext::default())
            .await
    });
    assert!(result.is_err());
}

#[test]
fn test_switch_by_name() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    run(json!({"action": "create", "name": "Alpha"}), &db);
    run(json!({"action": "create", "name": "Beta"}), &db);

    let switched = run(json!({"action": "switch", "name": "Alpha"}), &db);
    assert_eq!(switched["success"], true);
    assert_eq!(switched["slug"], "alpha");
    assert_eq!(switched["active"], true);

    let listed = run(json!({"action": "list"}), &db);
    assert_eq!(listed["active"], "alpha");
}

#[test]
fn test_switch_by_slug() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    run(json!({"action": "create", "name": "Alpha"}), &db);
    run(json!({"action": "create", "name": "Beta"}), &db);

    let switched = run(json!({"action": "switch", "name": "beta"}), &db);
    assert_eq!(switched["success"], true);
    assert_eq!(switched["slug"], "beta");
}

#[test]
fn test_switch_nonexistent_fails() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    run(json!({"action": "create", "name": "Alpha"}), &db);
    let result = run(json!({"action": "switch", "name": "Ghost"}), &db);
    assert_eq!(result["success"], false);
}

#[test]
fn test_invalid_action() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");
    let result = run(json!({"action": "delete"}), &db);
    assert_eq!(result["success"], false);
}

#[test]
fn test_persistence_across_instances() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("projects.json");

    run(json!({"action": "create", "name": "Persistent"}), &db);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let set2 = ProjectToolSet::with_db_path(&db);
    let listed = rt
        .block_on(async {
            set2.execute("desktop_project", json!({"action": "list"}), &loom_core::ToolContext::default())
                .await
        })
        .unwrap();
    assert_eq!(listed["count"], 1);
    assert_eq!(listed["projects"][0]["name"], "Persistent");
}