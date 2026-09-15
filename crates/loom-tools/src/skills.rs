//! 技能（Skills）工具集
//!
//! - skills_list: 列出可用技能（名称 + 描述）
//! - skill_view: 加载技能的完整内容或关联文件
//! - skill_manage: 创建、更新、删除技能
//!
//! 技能是一个目录，包含 SKILL.md（YAML frontmatter + 说明）以及可选的
//! references/、templates/、scripts/、assets/ 子目录。

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

/// 技能描述最大长度（截断显示）
const MAX_DESCRIPTION_LENGTH: usize = 200;

fn default_skills_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LOOM_SKILLS_DIR") {
        PathBuf::from(dir)
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".loom").join("skills")
    } else {
        PathBuf::from(".loom").join("skills")
    }
}

pub struct SkillsToolSet {
    skills_dir: PathBuf,
}

impl SkillsToolSet {
    pub fn new() -> Self {
        Self {
            skills_dir: default_skills_dir(),
        }
    }

    pub fn with_dir<P: Into<PathBuf>>(dir: P) -> Self {
        Self {
            skills_dir: dir.into(),
        }
    }
}

impl Default for SkillsToolSet {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ToolSet for SkillsToolSet {
    fn name(&self) -> &str {
        "skills"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec {
                name: "skills_list".into(),
                description: "List available skills (name + description). Use skill_view(name) to load full content.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "description": "Optional category filter to narrow results"
                        }
                    },
                    "required": []
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["skills".into()],
            },
            ToolSpec {
                name: "skill_view".into(),
                description: "Skills allow for loading information about specific tasks and workflows, as well as scripts and templates. Load a skill's full content or access its linked files (references, templates, scripts). First call returns SKILL.md content plus a 'linked_files' dict showing available references/templates/scripts. To access those, call again with file_path parameter.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "The skill name (use skills_list to see available skills)."
                        },
                        "file_path": {
                            "type": "string",
                            "description": "OPTIONAL: Path to a linked file within the skill (e.g., 'references/api.md', 'templates/config.yaml', 'scripts/validate.py'). Omit to get the main SKILL.md content."
                        }
                    },
                    "required": ["name"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["skills".into()],
            },
            ToolSpec {
                name: "skill_manage".into(),
                description: "Create, update, or delete skills — your procedural memory for recurring task types. The call is an operations array (a single edit is a list of one); it applies atomically — any failure rolls back all changes. ⚠️ The 'delete' action is DESTRUCTIVE and CANNOT be undone. BEFORE performing any 'delete' operation, you MUST call the 'interrupt' tool with value={\"skill\": \"...\", \"reason\": \"...\"} and ask the human to confirm. Only proceed with 'delete' if the human explicitly approves.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "operations": {
                            "type": "array",
                            "description": "Array of operations to apply atomically.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["create", "update", "delete", "patch"],
                                        "description": "create: make a new skill. update: overwrite SKILL.md. delete: remove the skill directory. patch: edit a file in the skill."
                                    },
                                    "name": {"type": "string", "description": "Skill name (directory name under skills/)."},
                                    "content": {"type": "string", "description": "SKILL.md content for create/update."},
                                    "file_path": {"type": "string", "description": "For patch: relative path within the skill directory."},
                                    "old_text": {"type": "string", "description": "For patch: text to replace."},
                                    "new_text": {"type": "string", "description": "For patch: replacement text."}
                                },
                                "required": ["action", "name"]
                            }
                        }
                    },
                    "required": ["operations"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["skills".into()],
            },
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "skills_list" => self.skills_list(&args).await,
            "skill_view" => self.skill_view(&args).await,
            "skill_manage" => self.skill_manage(&args).await,
            _ => Err(loom_core::LoomError::CapabilityNotFound(tool_name.into())),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(stream::once(async move { Ok(v) })))
    }
}

impl SkillsToolSet {
    async fn skills_list(&self, args: &Value) -> Result<Value> {
        let category_filter = args["category"].as_str();
        let base = &self.skills_dir;

        if !base.exists() {
            return Ok(json!({
                "skills": [],
                "count": 0,
                "skills_dir": base.to_string_lossy().to_string(),
                "message": format!("Skills directory does not exist: {}", base.display()),
            }));
        }

        let mut skills = Vec::new();
        let entries = match fs::read_dir(base) {
            Ok(e) => e,
            Err(e) => {
                return Ok(json!({
                    "skills": [],
                    "count": 0,
                    "error": format!("Failed to read skills directory: {e}"),
                }));
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let skill_md = path.join("SKILL.md");
            if !skill_md.exists() {
                continue;
            }

            let content = match fs::read_to_string(&skill_md) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let description = extract_description(&content);
            let category = extract_frontmatter_field(&content, "category");

            if let Some(cat) = category_filter {
                if category.as_deref() != Some(cat) {
                    continue;
                }
            }

            skills.push(json!({
                "name": name,
                "description": truncate(&description, MAX_DESCRIPTION_LENGTH),
                "category": category,
            }));
        }

        skills.sort_by(|a, b| {
            a["name"]
                .as_str()
                .unwrap_or("")
                .cmp(b["name"].as_str().unwrap_or(""))
        });

        Ok(json!({
            "skills": skills,
            "count": skills.len(),
            "skills_dir": base.to_string_lossy().to_string(),
        }))
    }

    async fn skill_view(&self, args: &Value) -> Result<Value> {
        let name = match args["name"].as_str() {
            Some(n) if !n.is_empty() => n,
            _ => {
                return Err(loom_core::LoomError::Other(
                    "skill_view: 'name' is required".into(),
                ));
            }
        };

        if has_path_traversal(name) {
            return Ok(json!({
                "success": false,
                "error": "Skill name cannot contain '..' path traversal components or absolute paths.",
            }));
        }

        let base = &self.skills_dir;
        let skill_dir = base.join(name);

        if !skill_dir.exists() {
            return Ok(json!({
                "success": false,
                "error": format!("Skill '{name}' not found in {}", base.display()),
            }));
        }

        // 查看关联文件
        if let Some(file_path) = args["file_path"].as_str() {
            if has_path_traversal(file_path) {
                return Ok(json!({
                    "success": false,
                    "error": "file_path cannot contain '..' path traversal components.",
                }));
            }
            let full_path = skill_dir.join(file_path);
            if !full_path.starts_with(&skill_dir) {
                return Ok(json!({
                    "success": false,
                    "error": "file_path must be within the skill directory.",
                }));
            }
            match fs::read_to_string(&full_path) {
                Ok(content) => Ok(json!({
                    "success": true,
                    "name": name,
                    "file_path": file_path,
                    "content": content,
                })),
                Err(e) => Ok(json!({
                    "success": false,
                    "error": format!("Failed to read {file_path}: {e}"),
                })),
            }
        } else {
            // 返回 SKILL.md 内容 + 关联文件列表
            let skill_md = skill_dir.join("SKILL.md");
            let content = match fs::read_to_string(&skill_md) {
                Ok(c) => c,
                Err(e) => {
                    return Ok(json!({
                        "success": false,
                        "error": format!("Failed to read SKILL.md: {e}"),
                    }));
                }
            };

            let linked_files = list_linked_files(&skill_dir);

            Ok(json!({
                "success": true,
                "name": name,
                "content": content,
                "linked_files": linked_files,
            }))
        }
    }

    async fn skill_manage(&self, args: &Value) -> Result<Value> {
        let operations = match args["operations"].as_array() {
            Some(ops) if !ops.is_empty() => ops.clone(),
            _ => {
                return Err(loom_core::LoomError::Other(
                    "skill_manage: 'operations' must be a non-empty array".into(),
                ));
            }
        };

        let base = &self.skills_dir;
        let mut results = Vec::new();

        for op in &operations {
            let action = op["action"].as_str().unwrap_or("");
            let name = match op["name"].as_str() {
                Some(n) if !n.is_empty() => n,
                _ => {
                    results.push(json!({"success": false, "error": "name is required"}));
                    continue;
                }
            };

            if has_path_traversal(name) {
                results.push(json!({
                    "success": false,
                    "error": "Skill name cannot contain '..' path traversal or absolute paths."
                }));
                continue;
            }

            let skill_dir = base.join(name);

            match action {
                "create" => {
                    if skill_dir.exists() {
                        results.push(json!({
                            "success": false,
                            "error": format!("Skill '{name}' already exists. Use 'update' to overwrite.")
                        }));
                        continue;
                    }
                    let content = op["content"].as_str().unwrap_or("");
                    if let Err(e) = fs::create_dir_all(&skill_dir) {
                        results.push(json!({"success": false, "error": format!("Failed to create directory: {e}")}));
                        continue;
                    }
                    let skill_md = skill_dir.join("SKILL.md");
                    if let Err(e) = fs::write(&skill_md, content) {
                        let _ = fs::remove_dir_all(&skill_dir);
                        results.push(json!({"success": false, "error": format!("Failed to write SKILL.md: {e}")}));
                        continue;
                    }
                    results.push(json!({"success": true, "action": "create", "name": name}));
                }
                "update" => {
                    if !skill_dir.exists() {
                        results.push(json!({
                            "success": false,
                            "error": format!("Skill '{name}' does not exist. Use 'create' first.")
                        }));
                        continue;
                    }
                    let content = op["content"].as_str().unwrap_or("");
                    let skill_md = skill_dir.join("SKILL.md");
                    if let Err(e) = fs::write(&skill_md, content) {
                        results.push(json!({"success": false, "error": format!("Failed to write SKILL.md: {e}")}));
                        continue;
                    }
                    results.push(json!({"success": true, "action": "update", "name": name}));
                }
                "delete" => {
                    if !skill_dir.exists() {
                        results.push(json!({
                            "success": false,
                            "error": format!("Skill '{name}' does not exist.")
                        }));
                        continue;
                    }
                    if let Err(e) = fs::remove_dir_all(&skill_dir) {
                        results.push(json!({"success": false, "error": format!("Failed to delete skill: {e}")}));
                        continue;
                    }
                    results.push(json!({"success": true, "action": "delete", "name": name}));
                }
                "patch" => {
                    let file_path = match op["file_path"].as_str() {
                        Some(f) => f,
                        None => {
                            results.push(
                                json!({"success": false, "error": "patch requires 'file_path'"}),
                            );
                            continue;
                        }
                    };
                    if has_path_traversal(file_path) {
                        results.push(
                            json!({"success": false, "error": "file_path cannot contain '..'"}),
                        );
                        continue;
                    }
                    let full_path = skill_dir.join(file_path);
                    if !full_path.starts_with(&skill_dir) {
                        results.push(json!({"success": false, "error": "file_path must be within the skill directory"}));
                        continue;
                    }
                    let old_text = op["old_text"].as_str().unwrap_or("");
                    let new_text = op["new_text"].as_str().unwrap_or("");

                    if !full_path.exists() {
                        results.push(json!({"success": false, "error": format!("File {file_path} does not exist")}));
                        continue;
                    }
                    let content = match fs::read_to_string(&full_path) {
                        Ok(c) => c,
                        Err(e) => {
                            results.push(json!({"success": false, "error": format!("Failed to read file: {e}")}));
                            continue;
                        }
                    };
                    if !content.contains(old_text) {
                        results
                            .push(json!({"success": false, "error": "old_text not found in file"}));
                        continue;
                    }
                    let new_content = content.replace(old_text, new_text);
                    if let Err(e) = fs::write(&full_path, new_content) {
                        results.push(json!({"success": false, "error": format!("Failed to write file: {e}")}));
                        continue;
                    }
                    results.push(json!({"success": true, "action": "patch", "name": name, "file_path": file_path}));
                }
                _ => {
                    results.push(json!({
                        "success": false,
                        "error": format!("Unknown action '{action}'. Use: create, update, delete, patch")
                    }));
                }
            }
        }

        let all_success = results
            .iter()
            .all(|r| r["success"].as_bool().unwrap_or(false));
        Ok(json!({
            "success": all_success,
            "operations": results.len(),
            "results": results,
        }))
    }
}

fn has_path_traversal(name: &str) -> bool {
    let p = Path::new(name);
    if p.is_absolute() {
        return true;
    }
    name.contains("..") || name.contains('/') && name.contains("..")
}

fn extract_description(content: &str) -> String {
    // 尝试从 YAML frontmatter 提取 description
    if let Some(fm) = parse_frontmatter(content) {
        if let Some(desc) = fm.get("description") {
            return desc.clone();
        }
    }
    // 否则取正文前 200 字符
    let body = content.splitn(3, "---").nth(2).unwrap_or(content);
    body.trim().to_string()
}

fn extract_frontmatter_field(content: &str, field: &str) -> Option<String> {
    parse_frontmatter(content).and_then(|fm| fm.get(field).cloned())
}

fn parse_frontmatter(content: &str) -> Option<std::collections::HashMap<String, String>> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let after = &trimmed[3..];
    let end = after.find("---")?;
    let fm_text = &after[..end];

    let mut map = std::collections::HashMap::new();
    for line in fm_text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_string();
            let value = value
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            map.insert(key, value);
        }
    }
    Some(map)
}

fn list_linked_files(skill_dir: &Path) -> Value {
    let mut linked = serde_json::Map::new();
    for subdir in &["references", "templates", "scripts", "assets"] {
        let dir = skill_dir.join(subdir);
        if dir.is_dir() {
            let files: Vec<String> = fs::read_dir(&dir)
                .into_iter()
                .flatten()
                .flatten()
                .filter_map(|e| {
                    let p = e.path();
                    if p.is_file() {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                })
                .collect();
            if !files.is_empty() {
                linked.insert((*subdir).to_string(), json!(files));
            }
        }
    }
    Value::Object(linked)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}