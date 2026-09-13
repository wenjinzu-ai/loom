//! 项目工具集
//!
//! - desktop_project: 创建/切换/列出桌面项目（命名工作区）
//!
//! 项目是带名称的工作区，可锚定到仓库/文件夹。

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;

fn projects_db_path() -> PathBuf {
    if let Ok(dir) = std::env::var("LOOM_HOME") {
        PathBuf::from(dir).join("projects.json")
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".loom").join("projects.json")
    } else {
        PathBuf::from(".loom").join("projects.json")
    }
}

fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Project {
    id: String,
    slug: String,
    name: String,
    path: Option<String>,
    active: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ProjectsDb {
    projects: Vec<Project>,
}

pub struct ProjectToolSet {
    db_path: PathBuf,
}

impl ProjectToolSet {
    pub fn new() -> Self {
        Self {
            db_path: projects_db_path(),
        }
    }

    pub fn with_db_path<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            db_path: path.into(),
        }
    }

    fn load_db(&self) -> ProjectsDb {
        fs::read_to_string(&self.db_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_db(&self, db: &ProjectsDb) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).ok();
        }
        let content = serde_json::to_string_pretty(db).map_err(|e| {
            loom_core::LoomError::Other(format!("Failed to serialize projects: {e}"))
        })?;
        fs::write(&self.db_path, content).map_err(|e| {
            loom_core::LoomError::Other(format!("Failed to write projects db: {e}"))
        })?;
        Ok(())
    }
}

impl Default for ProjectToolSet {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ToolSet for ProjectToolSet {
    fn name(&self) -> &str {
        "project"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "desktop_project".into(),
            description: "Create or switch desktop Projects (named workspaces). create: create one and switch this chat into it — pass path to anchor it to a repo/folder. switch: move this chat into an existing project by name/slug/id. list: all projects + which is active.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["create", "switch", "list"]},
                    "name": {"type": "string", "description": "create: human name. switch: name, slug, or id."},
                    "path": {"type": "string", "description": "create: repo/folder to anchor to."},
                },
                "required": ["action"]
            }),
            output_schema: json!({"type": "object"}),
            streaming: false,
            tags: vec!["project".into()],
        }]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "desktop_project" => self.desktop_project(&args).await,
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

impl ProjectToolSet {
    async fn desktop_project(&self, args: &Value) -> Result<Value> {
        let action = args["action"].as_str().unwrap_or("list");
        let mut db = self.load_db();

        match action {
            "list" => {
                let projects: Vec<Value> = db
                    .projects
                    .iter()
                    .map(|p| {
                        json!({
                            "id": p.id,
                            "slug": p.slug,
                            "name": p.name,
                            "path": p.path,
                            "active": p.active,
                        })
                    })
                    .collect();
                let active = db
                    .projects
                    .iter()
                    .find(|p| p.active)
                    .map(|p| p.slug.clone());
                Ok(json!({
                    "success": true,
                    "projects": projects,
                    "active": active,
                    "count": db.projects.len(),
                }))
            }
            "create" => {
                let name = match args["name"].as_str() {
                    Some(n) if !n.is_empty() => n,
                    _ => {
                        return Err(loom_core::LoomError::Other(
                            "desktop_project: 'name' is required for create".into(),
                        ));
                    }
                };
                let slug = slugify(name);
                if slug.is_empty() {
                    return Err(loom_core::LoomError::Other(
                        "desktop_project: invalid name (no alphanumeric characters)".into(),
                    ));
                }

                if db.projects.iter().any(|p| p.slug == slug) {
                    return Ok(json!({
                        "success": false,
                        "error": format!("Project with slug '{slug}' already exists"),
                    }));
                }

                let id = uuid::Uuid::new_v4().to_string();
                let path = args["path"].as_str().map(|s| s.to_string());

                let project = Project {
                    id: id.clone(),
                    slug: slug.clone(),
                    name: name.to_string(),
                    path,
                    active: true,
                };

                for p in db.projects.iter_mut() {
                    p.active = false;
                }
                db.projects.push(project.clone());
                self.save_db(&db)?;

                Ok(json!({
                    "success": true,
                    "action": "create",
                    "id": id,
                    "slug": slug,
                    "name": name,
                    "path": project.path,
                    "active": true,
                }))
            }
            "switch" => {
                let target = match args["name"].as_str() {
                    Some(n) if !n.is_empty() => n,
                    _ => {
                        return Err(loom_core::LoomError::Other(
                            "desktop_project: 'name' is required for switch".into(),
                        ));
                    }
                };

                let found = db
                    .projects
                    .iter()
                    .find(|p| p.name == target || p.slug == target || p.id == target);

                if found.is_none() {
                    return Ok(json!({
                        "success": false,
                        "error": format!("No project matching '{target}'"),
                    }));
                }

                let target_slug = found.unwrap().slug.clone();
                for p in db.projects.iter_mut() {
                    p.active = p.slug == target_slug;
                }
                self.save_db(&db)?;

                let active = db.projects.iter().find(|p| p.active).unwrap();
                Ok(json!({
                    "success": true,
                    "action": "switch",
                    "id": active.id,
                    "slug": active.slug,
                    "name": active.name,
                    "path": active.path,
                    "active": true,
                }))
            }
            _ => Ok(json!({
                "success": false,
                "error": format!("Unknown action '{action}'. Use: create, switch, list"),
            })),
        }
    }
}