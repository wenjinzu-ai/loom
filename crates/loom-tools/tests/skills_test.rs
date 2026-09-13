use loom_tools::skills::SkillsToolSet;
use loom_tools::spec::ToolSet;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

fn run(tool: &str, args: serde_json::Value, skills_dir: &std::path::Path) -> serde_json::Value {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let set = SkillsToolSet::with_dir(skills_dir);
    rt.block_on(async move { set.execute(tool, args, &loom_core::ToolContext::default()).await.unwrap() })
}

#[test]
fn test_skills_list_empty() {
    let dir = TempDir::new().unwrap();
    let result = run("skills_list", json!({}), dir.path());
    assert_eq!(result["count"], 0);
    assert_eq!(result["skills"].as_array().unwrap().len(), 0);
}

#[test]
fn test_skills_list_with_skills() {
    let dir = TempDir::new().unwrap();

    let skill_dir = dir.path().join("code-review");
    fs::create_dir_all(&skill_dir).unwrap();
    fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: code-review\ndescription: Review code for quality\ncategory: development\n---\n# Code Review\nReview code carefully.",
    )
    .unwrap();

    let result = run("skills_list", json!({}), dir.path());
    assert_eq!(result["count"], 1);
    assert_eq!(result["skills"][0]["name"], "code-review");
    assert!(result["skills"][0]["description"]
        .as_str()
        .unwrap()
        .contains("Review code"));
    assert_eq!(result["skills"][0]["category"], "development");
}

#[test]
fn test_skills_list_category_filter() {
    let dir = TempDir::new().unwrap();

    let dev = dir.path().join("code-review");
    fs::create_dir_all(&dev).unwrap();
    fs::write(
        dev.join("SKILL.md"),
        "---\ndescription: Dev skill\ncategory: development\n---\nbody",
    )
    .unwrap();

    let writing = dir.path().join("writing");
    fs::create_dir_all(&writing).unwrap();
    fs::write(
        writing.join("SKILL.md"),
        "---\ndescription: Writing skill\ncategory: writing\n---\nbody",
    )
    .unwrap();

    let result = run(
        "skills_list",
        json!({"category": "development"}),
        dir.path(),
    );
    assert_eq!(result["count"], 1);
    assert_eq!(result["skills"][0]["name"], "code-review");
}

#[test]
fn test_skill_view_main() {
    let dir = TempDir::new().unwrap();
    let skill_dir = dir.path().join("my-skill");
    fs::create_dir_all(&skill_dir).unwrap();
    let content = "---\nname: my-skill\n---\n# My Skill\nInstructions here.";
    fs::write(skill_dir.join("SKILL.md"), content).unwrap();

    let result = run("skill_view", json!({"name": "my-skill"}), dir.path());
    assert_eq!(result["success"], true);
    assert_eq!(result["content"], content);
}

#[test]
fn test_skill_view_linked_files() {
    let dir = TempDir::new().unwrap();
    let skill_dir = dir.path().join("my-skill");
    fs::create_dir_all(skill_dir.join("references")).unwrap();
    fs::create_dir_all(skill_dir.join("templates")).unwrap();
    fs::write(skill_dir.join("SKILL.md"), "# Skill").unwrap();
    fs::write(skill_dir.join("references/api.md"), "# API").unwrap();
    fs::write(skill_dir.join("templates/config.yaml"), "key: value").unwrap();

    let result = run("skill_view", json!({"name": "my-skill"}), dir.path());
    assert!(result["linked_files"]["references"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f == "api.md"));
    assert!(result["linked_files"]["templates"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f == "config.yaml"));
}

#[test]
fn test_skill_view_file_path() {
    let dir = TempDir::new().unwrap();
    let skill_dir = dir.path().join("my-skill");
    fs::create_dir_all(skill_dir.join("references")).unwrap();
    fs::write(skill_dir.join("SKILL.md"), "# Skill").unwrap();
    fs::write(skill_dir.join("references/api.md"), "# API Doc").unwrap();

    let result = run(
        "skill_view",
        json!({"name": "my-skill", "file_path": "references/api.md"}),
        dir.path(),
    );
    assert_eq!(result["success"], true);
    assert_eq!(result["content"], "# API Doc");
    assert_eq!(result["file_path"], "references/api.md");
}

#[test]
fn test_skill_view_not_found() {
    let dir = TempDir::new().unwrap();
    let result = run("skill_view", json!({"name": "nonexistent"}), dir.path());
    assert_eq!(result["success"], false);
    assert!(result["error"].as_str().unwrap().contains("not found"));
}

#[test]
fn test_skill_view_path_traversal() {
    let dir = TempDir::new().unwrap();
    let result = run("skill_view", json!({"name": "../etc/passwd"}), dir.path());
    assert_eq!(result["success"], false);
}

#[test]
fn test_skill_manage_create_and_view() {
    let dir = TempDir::new().unwrap();
    let content = "---\nname: test-skill\n---\n# Test\nThis is a test skill.";
    let result = run(
        "skill_manage",
        json!({
            "operations": [{"action": "create", "name": "test-skill", "content": content}]
        }),
        dir.path(),
    );
    assert_eq!(result["success"], true);
    assert!(dir.path().join("test-skill").join("SKILL.md").exists());

    let view = run("skill_view", json!({"name": "test-skill"}), dir.path());
    assert_eq!(view["success"], true);
    assert_eq!(view["content"], content);
}

#[test]
fn test_skill_manage_create_duplicate_fails() {
    let dir = TempDir::new().unwrap();
    run(
        "skill_manage",
        json!({"operations": [{"action": "create", "name": "dup", "content": "x"}]}),
        dir.path(),
    );
    let result = run(
        "skill_manage",
        json!({"operations": [{"action": "create", "name": "dup", "content": "y"}]}),
        dir.path(),
    );
    assert_eq!(result["success"], false);
}

#[test]
fn test_skill_manage_update() {
    let dir = TempDir::new().unwrap();
    run(
        "skill_manage",
        json!({"operations": [{"action": "create", "name": "s", "content": "old"}]}),
        dir.path(),
    );
    let result = run(
        "skill_manage",
        json!({"operations": [{"action": "update", "name": "s", "content": "new"}]}),
        dir.path(),
    );
    assert_eq!(result["success"], true);

    let view = run("skill_view", json!({"name": "s"}), dir.path());
    assert_eq!(view["content"], "new");
}

#[test]
fn test_skill_manage_delete() {
    let dir = TempDir::new().unwrap();
    run(
        "skill_manage",
        json!({"operations": [{"action": "create", "name": "del", "content": "x"}]}),
        dir.path(),
    );
    let result = run(
        "skill_manage",
        json!({"operations": [{"action": "delete", "name": "del"}]}),
        dir.path(),
    );
    assert_eq!(result["success"], true);
    assert!(!dir.path().join("del").exists());
}

#[test]
fn test_skill_manage_patch() {
    let dir = TempDir::new().unwrap();
    run(
        "skill_manage",
        json!({"operations": [{"action": "create", "name": "p", "content": "old text here"}]}),
        dir.path(),
    );
    let result = run(
        "skill_manage",
        json!({"operations": [{
            "action": "patch",
            "name": "p",
            "file_path": "SKILL.md",
            "old_text": "old text",
            "new_text": "new text"
        }]}),
        dir.path(),
    );
    assert_eq!(result["success"], true);

    let view = run("skill_view", json!({"name": "p"}), dir.path());
    assert_eq!(view["content"], "new text here");
}

#[test]
fn test_skill_manage_batch_atomic() {
    let dir = TempDir::new().unwrap();
    let result = run(
        "skill_manage",
        json!({"operations": [
            {"action": "create", "name": "a", "content": "a"},
            {"action": "create", "name": "b", "content": "b"},
        ]}),
        dir.path(),
    );
    assert_eq!(result["success"], true);
    assert!(dir.path().join("a").exists());
    assert!(dir.path().join("b").exists());
}

#[test]
fn test_skill_manage_invalid_action() {
    let dir = TempDir::new().unwrap();
    let result = run(
        "skill_manage",
        json!({"operations": [{"action": "invalid", "name": "x"}]}),
        dir.path(),
    );
    assert_eq!(result["success"], false);
}
