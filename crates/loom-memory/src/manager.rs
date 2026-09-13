//! MemoryManager — 记忆管理器
//!
//! - builtin provider 永远第一位，最多 1 个 external provider
//! - provider 失败不阻塞其他 provider（每个钩子单独 try/catch）
//! - `prefetch_all`: 聚合所有 provider 的召回内容
//! - `sync_all`: 后台串行执行（信号量保证 turn N 在 N+1 前写入）
//! - `queue_prefetch_all`: 后台召回供下轮消费
//! - `build_system_prompt`: 聚合所有 provider 的 system_prompt_block
//! - `describe_recall`: 召回指示器
//! - `shutdown_all`: 优雅关闭，等待后台任务排空

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use loom_core::MemoryScope;
use loom_llm::ChatMessage;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, Semaphore};

use crate::context::build_memory_context_block;
use crate::provider::{MemoryProvider, ProviderToolSchema};
use crate::trivial::is_trivial_prompt;

/// 外部 provider prefetch 超时（秒）
const EXTERNAL_PREFETCH_TIMEOUT: Duration = Duration::from_secs(8);
/// shutdown 时等待后台任务排空的超时
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct MemoryManager {
    providers: RwLock<Vec<Arc<dyn MemoryProvider>>>,
    tool_to_provider: RwLock<HashMap<String, Arc<dyn MemoryProvider>>>,
    has_external: Mutex<bool>,
    /// 串行化后台 sync/prefetch 的信号量（单 permit）
    bg_semaphore: Arc<Semaphore>,
    shutting_down: Mutex<bool>,
}

impl MemoryManager {
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(Vec::new()),
            tool_to_provider: RwLock::new(HashMap::new()),
            has_external: Mutex::new(false),
            bg_semaphore: Arc::new(Semaphore::new(1)),
            shutting_down: Mutex::new(false),
        }
    }

    /// 注册 provider；builtin 永远接受，external 最多 1 个
    pub async fn add_provider(&self, provider: Arc<dyn MemoryProvider>) {
        let name = provider.name().to_string();
        if provider.is_external() {
            let mut has_ext = self.has_external.lock().await;
            if *has_ext {
                tracing::warn!(
                    "Rejected memory provider '{}': only one external provider allowed",
                    name
                );
                return;
            }
            *has_ext = true;
        }

        let mut tool_map = self.tool_to_provider.write().await;
        for schema in provider.get_tool_schemas() {
            let tool_name = schema.name;
            match tool_map.entry(tool_name.clone()) {
                std::collections::hash_map::Entry::Occupied(_) => {
                    tracing::warn!(
                        "Memory tool name conflict: '{}' already registered, ignoring from {}",
                        tool_name,
                        name
                    );
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(provider.clone());
                }
            }
        }

        self.providers.write().await.push(provider);
        tracing::info!("Memory provider '{}' registered", name);
    }

    /// 获取所有 provider 的快照
    async fn snapshot_providers(&self) -> Vec<Arc<dyn MemoryProvider>> {
        self.providers.read().await.clone()
    }

    /// 会话加载：初始化所有 provider（外部后端如 mem0 会建立会话上下文）
    /// 同时刷新 builtin provider 的系统提示词快照。
    pub async fn load_session(&self, scope: &MemoryScope, session_id: &str) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p.initialize(scope, session_id).await {
                tracing::warn!(
                    "Memory provider '{}' initialize failed: {}",
                    p.name(),
                    e
                );
            }
        }
    }

    /// 聚合所有 provider 的 system_prompt_block
    pub async fn build_system_prompt(&self, scope: &MemoryScope, session_id: &str) -> String {
        let providers = self.snapshot_providers().await;
        let mut blocks = Vec::new();
        for p in providers.iter() {
            match p.system_prompt_block(scope, session_id).await {
                Ok(Some(block)) if !block.trim().is_empty() => blocks.push(block),
                Ok(_) => {}
                Err(e) => tracing::warn!("Memory provider '{}' system_prompt_block failed: {}", p.name(), e),
            }
        }
        blocks.join("\n\n")
    }

    /// 聚合所有 provider 的 prefetch（trivial prompt 跳过）
    pub async fn prefetch_all(&self, scope: &MemoryScope, query: &str, session_id: &str) -> String {
        if is_trivial_prompt(query) {
            return String::new();
        }
        let providers = self.snapshot_providers().await;
        let mut parts = Vec::new();
        for p in providers.iter() {
            let result = if !p.is_external() {
                p.prefetch(scope, query, session_id).await
            } else {
                match tokio::time::timeout(EXTERNAL_PREFETCH_TIMEOUT, p.prefetch(scope, query, session_id)).await {
                    Ok(r) => r,
                    Err(_) => {
                        tracing::warn!("Memory provider '{}' prefetch timed out", p.name());
                        continue;
                    }
                }
            };
            match result {
                Ok(s) if !s.trim().is_empty() => parts.push(s),
                Ok(_) => {}
                Err(e) => tracing::debug!("Memory provider '{}' prefetch failed: {}", p.name(), e),
            }
        }
        let merged = parts.join("\n\n");
        build_memory_context_block(&merged)
    }

    /// 后台 prefetch（供下轮消费）
    pub async fn queue_prefetch_all(&self, scope: &MemoryScope, query: &str, session_id: &str) {
        if is_trivial_prompt(query) {
            return;
        }
        let providers = self.snapshot_providers().await;
        let scope = scope.clone();
        let query = query.to_string();
        let session_id = session_id.to_string();
        let sem = self.bg_semaphore.clone();
        tokio::spawn(async move {
            let _permit = sem.acquire().await;
            for p in providers.iter() {
                if let Err(e) = p.queue_prefetch(&scope, &query, &session_id).await {
                    tracing::debug!("Memory provider '{}' queue_prefetch failed: {}", p.name(), e);
                }
            }
        });
    }

    /// 后台同步本轮对话
    pub async fn sync_all(
        &self,
        scope: &MemoryScope,
        user_content: &str,
        assistant_content: &str,
        session_id: &str,
        messages: &[ChatMessage],
    ) {
        if is_trivial_prompt(user_content) {
            return;
        }
        let providers = self.snapshot_providers().await;
        let scope = scope.clone();
        let user_content = user_content.to_string();
        let assistant_content = assistant_content.to_string();
        let session_id = session_id.to_string();
        let messages = messages.to_vec();
        let sem = self.bg_semaphore.clone();
        tokio::spawn(async move {
            let _permit = sem.acquire().await;
            for p in providers.iter() {
                if let Err(e) = p
                    .sync_turn(&scope, &user_content, &assistant_content, &session_id, &messages)
                    .await
                {
                    tracing::warn!("Memory provider '{}' sync_turn failed: {}", p.name(), e);
                }
            }
        });
    }

    /// 压缩前聚合所有 provider 的洞察
    pub async fn on_pre_compress(&self, scope: &MemoryScope, messages: &[ChatMessage]) -> String {
        let providers = self.snapshot_providers().await;
        let mut insights = Vec::new();
        for p in providers.iter() {
            match p.on_pre_compress(scope, messages).await {
                Ok(s) if !s.trim().is_empty() => insights.push(s),
                Ok(_) => {}
                Err(e) => tracing::warn!("Memory provider '{}' on_pre_compress failed: {}", p.name(), e),
            }
        }
        insights.join("\n\n")
    }

    /// 每轮开始通知所有 provider
    pub async fn on_turn_start(&self, turn_number: usize, message: &str, session_id: &str) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p.on_turn_start(turn_number, message, session_id).await {
                tracing::warn!("Memory provider '{}' on_turn_start failed: {}", p.name(), e);
            }
        }
    }

    /// 内置 memory 工具写入成功后，镜像通知所有外部 provider
    ///
    /// 仅通知非 builtin 的外部 provider，保持两套记忆数据一致。
    /// 内置 provider 的写入由 `handle_tool_call` 直接完成。
    pub async fn notify_memory_tool_write(
        &self,
        action: &str,
        target: Option<&str>,
        content: Option<&str>,
        metadata: Value,
    ) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if !p.is_external() {
                continue;
            }
            if let Err(e) = p.on_memory_write(action, target, content, metadata.clone()).await {
                tracing::warn!(
                    "Memory provider '{}' on_memory_write (action={}) failed: {}",
                    p.name(),
                    action,
                    e
                );
            }
        }
    }

    /// 会话切换
    pub async fn on_session_switch(
        &self,
        new_session_id: &str,
        parent_session_id: Option<&str>,
        reset: bool,
        rewound: bool,
    ) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p
                .on_session_switch(new_session_id, parent_session_id, reset, rewound)
                .await
            {
                tracing::warn!("Memory provider '{}' on_session_switch failed: {}", p.name(), e);
            }
        }
    }

    /// 任务委派
    pub async fn on_delegation(&self, task: &str, result: &str, child_session_id: &str) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p.on_delegation(task, result, child_session_id).await {
                tracing::warn!("Memory provider '{}' on_delegation failed: {}", p.name(), e);
            }
        }
    }

    /// 会话结束
    pub async fn on_session_end(&self, scope: &MemoryScope, messages: &[ChatMessage]) {
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p.on_session_end(scope, messages).await {
                tracing::warn!("Memory provider '{}' on_session_end failed: {}", p.name(), e);
            }
        }
    }

    /// 召回指示器
    pub async fn describe_recall(&self) -> String {
        let providers = self.snapshot_providers().await;
        let mut segments = Vec::new();
        for p in providers.iter() {
            match p.recall_status().await {
                Ok(Some(status)) => {
                    let detail = if status.count == 1 {
                        "recalled 1 memory".to_string()
                    } else if status.count > 1 {
                        format!("recalled {} memories", status.count)
                    } else {
                        "recalled relevant memory".to_string()
                    };
                    segments.push(format!("{} {} — {}", status.glyph, status.provider_label, detail));
                }
                Ok(None) => {}
                Err(e) => tracing::debug!("Memory provider '{}' recall_status failed: {}", p.name(), e),
            }
        }
        segments.join("  ")
    }

    /// 路由工具调用到对应 provider
    pub async fn handle_tool_call(&self, name: &str, args: Value, scope: &MemoryScope, session_id: &str) -> anyhow::Result<Value> {
        let provider = {
            let map = self.tool_to_provider.read().await;
            map.get(name).cloned()
        };
        match provider {
            Some(p) => p.handle_tool_call(name, args, scope, session_id).await,
            None => anyhow::bail!("no memory provider handles tool '{}'", name),
        }
    }

    /// 获取所有 provider 的工具 schema
    pub async fn get_all_tool_schemas(&self) -> Vec<ProviderToolSchema> {
        let providers = self.snapshot_providers().await;
        let mut schemas = Vec::new();
        for p in providers.iter() {
            schemas.extend(p.get_tool_schemas());
        }
        schemas
    }

    /// 优雅关闭：等待后台任务排空，然后 shutdown 所有 provider
    pub async fn shutdown_all(&self) {
        *self.shutting_down.lock().await = true;
        // 获取全部 permit 等待串行任务排空
        let _ = tokio::time::timeout(
            SHUTDOWN_DRAIN_TIMEOUT,
            self.bg_semaphore.clone().acquire_many(1),
        )
        .await;
        let providers = self.snapshot_providers().await;
        for p in providers.iter() {
            if let Err(e) = p.shutdown().await {
                tracing::warn!("Memory provider '{}' shutdown failed: {}", p.name(), e);
            }
        }
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::new()
    }
}