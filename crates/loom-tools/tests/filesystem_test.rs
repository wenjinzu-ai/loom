use loom_tools::filesystem::FilesystemToolSet;
use loom_tools::spec::ToolSet;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

fn fs_set() -> FilesystemToolSet {
    FilesystemToolSet
}

#[tokio::test]
async fn test_read_file_with_line_numbers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    fs::write(&path, "line1\nline2\nline3\n").unwrap();

    let set = fs_set();
    let result = set
        .execute("read_file", json!({"path": path.to_str().unwrap()}), &loom_core::ToolContext::default())
        .await
        .unwrap();

    let content = result["content"].as_str().unwrap();
    assert!(content.contains("1|line1"));
    assert!(content.contains("2|line2"));
    assert!(content.contains("3|line3"));
    assert_eq!(result["total_lines"], 3);
    assert_eq!(result["truncated"], false);
}

#[tokio::test]
async fn test_read_file_offset_limit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    let content: Vec<String> = (1..=10).map(|i| format!("line{i}")).collect();
    fs::write(&path, content.join("\n")).unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "read_file", json!({"path": path.to_str().unwrap(), "offset": 3, "limit": 2}),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    let out = result["content"].as_str().unwrap();
    assert!(out.contains("3|line3"));
    assert!(out.contains("4|line4"));
    assert!(!out.contains("5|line5"));
}

#[tokio::test]
async fn test_write_file_creates_dirs() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a/b/c/test.txt");

    let set = fs_set();
    let result = set
        .execute(
            "write_file", json!({"path": path.to_str().unwrap(), "content": "hello"}),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);
    assert_eq!(fs::read_to_string(&path).unwrap(), "hello");
}

#[tokio::test]
async fn test_patch_exact_replace() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    fs::write(&path, "foo bar baz\nfoo bar baz\n").unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "patch", json!({
                "path": path.to_str().unwrap(),
                "old_string": "foo bar baz",
                "new_string": "qux quux",
                "replace_all": true
            }),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["replacements"], 2);
    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, "qux quux\nqux quux\n");
}

#[tokio::test]
async fn test_patch_non_unique_fails() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    fs::write(&path, "foo\nfoo\n").unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "patch", json!({
                "path": path.to_str().unwrap(),
                "old_string": "foo",
                "new_string": "bar"
            }),
            &loom_core::ToolContext::default(),
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_patch_ignores_trailing_whitespace() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    fs::write(&path, "foo   \nbar\n").unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "patch", json!({
                "path": path.to_str().unwrap(),
                "old_string": "foo\nbar",
                "new_string": "baz\nqux"
            }),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["replacements"], 1);
    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, "baz\nqux\n");
}

#[tokio::test]
async fn test_search_files_content() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.txt");
    fs::write(&path, "hello world\nfoo bar\nhello again\n").unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "search_files", json!({
                "pattern": "hello",
                "target": "content",
                "path": dir.path().to_str().unwrap()
            }),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["count"], 2);
    let matches = result["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["line"], 1);
    assert_eq!(matches[1]["line"], 3);
}

#[tokio::test]
async fn test_search_files_by_name() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("a.txt"), "x").unwrap();
    fs::write(dir.path().join("b.rs"), "x").unwrap();
    fs::write(dir.path().join("c.txt"), "x").unwrap();

    let set = fs_set();
    let result = set
        .execute(
            "search_files", json!({
                "pattern": "*.txt",
                "target": "files",
                "path": dir.path().to_str().unwrap()
            }),
            &loom_core::ToolContext::default(),
        )
        .await
        .unwrap();

    assert_eq!(result["count"], 2);
    let files = result["files"].as_array().unwrap();
    assert!(files.iter().any(|f| f == "a.txt"));
    assert!(files.iter().any(|f| f == "c.txt"));
}