//! 工具集注册表：发现 + 按名路由执行

use crate::spec::{ToolSet, ToolSpec};
use parking_lot::RwLock;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

/// 工具注册表
///
/// 持有所有已注册的 ToolSet，提供工具发现与执行路由。
/// 内部维护 `tool_name → set_name` 反向索引，加速 `find_set` 查询。
pub struct ToolRegistry {
    sets: RwLock<HashMap<String, Arc<dyn ToolSet>>>,
    tool_index: RwLock<HashMap<String, String>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            sets: RwLock::new(HashMap::new()),
            tool_index: RwLock::new(HashMap::new()),
        }
    }

    /// 注册一个工具集
    pub fn register(&self, set: Arc<dyn ToolSet>) {
        let set_name = set.name().to_string();
        for tool in set.tools() {
            self.tool_index.write().insert(tool.name, set_name.clone());
        }
        self.sets.write().insert(set_name, set);
    }

    /// 发现所有工具的规格
    ///
    /// 自动将工具所属的 ToolSet name 追加到每个工具的 tags 中，
    /// 这样上层可以通过 `tags` 按工具集名过滤（如 `toolsets=["filesystem"]`）。
    pub fn discover(&self) -> Vec<ToolSpec> {
        self.sets
            .read()
            .iter()
            .flat_map(|(set_name, set)| {
                set.tools().into_iter().map(|mut tool| {
                    let set_tag = set_name.clone();
                    if !tool.tags.iter().any(|t| t == &set_tag) {
                        tool.tags.push(set_tag);
                    }
                    tool
                })
            })
            .collect()
    }

    /// 根据工具名查找其所属的 ToolSet
    pub fn find_set(&self, tool_name: &str) -> Option<Arc<dyn ToolSet>> {
        let set_name = self.tool_index.read().get(tool_name).cloned()?;
        self.sets.read().get(&set_name).cloned()
    }

    /// 列出所有已注册的工具集名
    pub fn list_sets(&self) -> Vec<String> {
        self.sets.read().keys().cloned().collect()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 构建内置工具集注册表
pub fn builtins() -> ToolRegistry {
    let registry = ToolRegistry::new();
    registry.register(Arc::new(crate::echo::EchoToolSet));
    registry.register(Arc::new(crate::filesystem::FilesystemToolSet));
    registry.register(Arc::new(crate::web::WebToolSet));
    registry.register(Arc::new(crate::code::CodeToolSet::new()));
    registry.register(Arc::new(crate::memory::MemoryToolSet::new()));
    registry.register(Arc::new(crate::todo::TodoToolSet::new()));
    registry.register(Arc::new(crate::clarify::ClarifyToolSet));
    registry.register(Arc::new(crate::skills::SkillsToolSet::new()));
    registry.register(Arc::new(crate::project::ProjectToolSet::new()));
    registry
}

/// 构建内置工具集注册表，使用外部提供的 MemoryToolSet（便于与 MemoryManager 共享存储）
pub fn builtins_with_memory(memory_toolset: Arc<dyn ToolSet>) -> ToolRegistry {
    let registry = ToolRegistry::new();
    registry.register(Arc::new(crate::echo::EchoToolSet));
    registry.register(Arc::new(crate::filesystem::FilesystemToolSet));
    registry.register(Arc::new(crate::web::WebToolSet));
    registry.register(Arc::new(crate::code::CodeToolSet::new()));
    registry.register(memory_toolset);
    registry.register(Arc::new(crate::todo::TodoToolSet::new()));
    registry.register(Arc::new(crate::clarify::ClarifyToolSet));
    registry.register(Arc::new(crate::skills::SkillsToolSet::new()));
    registry.register(Arc::new(crate::project::ProjectToolSet::new()));
    registry
}

/// 构建内置工具集注册表，使用 PostgreSQL 后端（memory + kanban + cron + todo）
pub async fn builtins_with_pool(pool: PgPool) -> anyhow::Result<ToolRegistry> {
    let registry = ToolRegistry::new();
    registry.register(Arc::new(crate::echo::EchoToolSet));
    registry.register(Arc::new(crate::filesystem::FilesystemToolSet));
    registry.register(Arc::new(crate::web::WebToolSet));
    registry.register(Arc::new(crate::code::CodeToolSet::new()));

    // todo: PostgreSQL 后端
    let todo_backend = crate::todo::PostgresTodoBackend::from_pool(pool.clone());
    todo_backend.migrate().await?;
    registry.register(Arc::new(crate::todo::TodoToolSet::with_backend(Arc::new(
        todo_backend,
    ))));

    registry.register(Arc::new(crate::clarify::ClarifyToolSet));
    registry.register(Arc::new(crate::skills::SkillsToolSet::new()));
    registry.register(Arc::new(crate::project::ProjectToolSet::new()));

    // memory: 共享 PostgreSQL KV 存储后端
    let mem_kv = loom_infra::PostgresJsonStore::from_pool(pool.clone());
    mem_kv.migrate().await?;
    registry.register(Arc::new(crate::memory::MemoryToolSet::with_kv(Arc::new(
        mem_kv,
    ))));

    // kanban
    let kanban_store = crate::kanban::KanbanStore::from_pool(pool.clone());
    kanban_store.migrate().await?;
    registry.register(Arc::new(crate::kanban::KanbanToolSet::new(Arc::new(
        kanban_store,
    ))));

    // cron
    let cron_store = crate::cron::CronStore::from_pool(pool);
    cron_store.migrate().await?;
    registry.register(Arc::new(crate::cron::CronToolSet::new(Arc::new(
        cron_store,
    ))));

    Ok(registry)
}

/// 构建内置工具集注册表（PostgreSQL 后端），使用外部提供的 MemoryToolSet
pub async fn builtins_with_pool_and_memory(
    pool: PgPool,
    memory_toolset: Arc<dyn ToolSet>,
) -> anyhow::Result<ToolRegistry> {
    let registry = ToolRegistry::new();
    registry.register(Arc::new(crate::echo::EchoToolSet));
    registry.register(Arc::new(crate::filesystem::FilesystemToolSet));
    registry.register(Arc::new(crate::web::WebToolSet));
    registry.register(Arc::new(crate::code::CodeToolSet::new()));

    let todo_backend = crate::todo::PostgresTodoBackend::from_pool(pool.clone());
    todo_backend.migrate().await?;
    registry.register(Arc::new(crate::todo::TodoToolSet::with_backend(Arc::new(
        todo_backend,
    ))));

    registry.register(Arc::new(crate::clarify::ClarifyToolSet));
    registry.register(Arc::new(crate::skills::SkillsToolSet::new()));
    registry.register(Arc::new(crate::project::ProjectToolSet::new()));
    registry.register(memory_toolset);

    let kanban_store = crate::kanban::KanbanStore::from_pool(pool.clone());
    kanban_store.migrate().await?;
    registry.register(Arc::new(crate::kanban::KanbanToolSet::new(Arc::new(
        kanban_store,
    ))));

    let cron_store = crate::cron::CronStore::from_pool(pool);
    cron_store.migrate().await?;
    registry.register(Arc::new(crate::cron::CronToolSet::new(Arc::new(
        cron_store,
    ))));

    Ok(registry)
}

/// 构建内置工具集注册表，并注入 Kanban 工具集（需要 PostgreSQL 连接）
pub fn builtins_with_kanban(kanban_store: Arc<crate::kanban::KanbanStore>) -> ToolRegistry {
    let registry = builtins();
    registry.register(Arc::new(crate::kanban::KanbanToolSet::new(kanban_store)));
    registry
}