//! 应用状态：组装 Registry / Lifecycle / Adapters / Orchestration / Bus / LLM / Agent

use crate::config::AppConfig;
use loom_adapters::{native::NativeAdapter, AdapterConfig, AdapterKind, AdapterManager};
use loom_agent::{
    AgentLoop, AgentLoopConfig, CheckpointStore, LifecycleManager, PostgresConfig, PostgresJsonStore,
    SessionStore, SessionStoreImpl,
};
use loom_agent::delegation::SessionWaker;
use loom_core::{
    AgentLifecycleManager, CapabilityExecutor, CapabilityRegistry, OrchestrationEngine,
};
use loom_llm::{LlmProvider, OpenAiProvider};
use loom_memory::{BuiltinMemoryProvider, MemoryManager, MemoryStore};
use loom_infra::{DagOrchestrator, InMemoryBus, InMemoryRegistry};
use loom_tools::{builtins_with_memory, builtins_with_pool_and_memory};
use std::sync::Arc;

pub struct AppState {
    pub registry: Arc<dyn CapabilityRegistry>,
    pub lifecycle: Arc<dyn AgentLifecycleManager>,
    /// TODO: 当前 chat 接口未直接使用 executor，预留供后续工具调用代理接口
    #[allow(dead_code)]
    pub executor: Arc<dyn CapabilityExecutor>,
    /// TODO: 预留供后续 DAG 编排接口（/orchestrate）
    #[allow(dead_code)]
    pub orchestration: Arc<dyn OrchestrationEngine>,
    /// TODO: 预留供后续事件订阅接口（/events/stream）
    #[allow(dead_code)]
    pub bus: Arc<InMemoryBus>,
    pub agent_loop: Arc<AgentLoop>,
    /// 多轮会话历史存储（PostgreSQL 持久化，连接失败时回退内存）
    pub sessions: Arc<dyn SessionStore>,
}

impl AppState {
    pub async fn build(config: &AppConfig) -> anyhow::Result<Self> {
        let registry: Arc<dyn CapabilityRegistry> = Arc::new(InMemoryRegistry::new());

        let lifecycle = Arc::new(LifecycleManager::new());

        let bus = Arc::new(InMemoryBus::new());

        // 构建数据库连接池，供 checkpoint / sessions / kanban 共享
        let pg_config: PostgresConfig = config.database.clone().into();
        let db_pool = match pg_config.connect().await {
            Ok(pool) => Some(pool),
            Err(e) => {
                tracing::warn!(
                    "PostgreSQL connection failed (host={}, dbname={}): {} — falling back to in-memory",
                    pg_config.host, pg_config.dbname, e
                );
                None
            }
        };
        let has_db = db_pool.is_some();

        // 构建共享的 JsonKeyValueStore：优先 PostgreSQL，连接失败则回退到内存
        // checkpoint 和 session_store 复用同一个 KV 存储实例
        let kv_store: Arc<dyn loom_agent::JsonKeyValueStore> = if let Some(pool) = db_pool.clone() {
            let store = PostgresJsonStore::from_pool(pool);
            store.migrate().await?;
            tracing::info!(
                "using PostgreSQL kv store (db={})",
                pg_config.dbname
            );
            Arc::new(store)
        } else {
            tracing::warn!("PostgreSQL unavailable, falling back to in-memory kv store");
            Arc::new(loom_agent::InMemoryJsonStore::new())
        };

        // checkpoint saver 基于共享 KV 存储
        let checkpoint_saver: Arc<dyn loom_core::CheckpointSaver> =
            Arc::new(CheckpointStore::new(kv_store.clone()));

        // 会话存储基于共享 KV 存储
        let sessions: Arc<dyn SessionStore> =
            Arc::new(SessionStoreImpl::new(kv_store.clone()));

        // 构建共享记忆存储：MemoryStore 基于同一 KV 存储，namespace 由 MemoryScope 隔离
        let memory_store = Arc::new(MemoryStore::new(kv_store.clone()));
        let memory_provider = Arc::new(BuiltinMemoryProvider::new(memory_store.clone()));

        // 构建记忆管理器，注册 builtin provider
        let memory_manager = Arc::new(MemoryManager::new());
        memory_manager.add_provider(memory_provider.clone()).await;

        // 构建 MemoryToolSet，与 MemoryManager 共享同一 provider
        let memory_toolset: Arc<dyn loom_tools::ToolSet> =
            Arc::new(loom_tools::memory::MemoryToolSet::with_provider(memory_provider));

        // 构建工具注册表：有数据库时注入 memory/kanban/cron 工具集
        let tool_registry = if let Some(pool) = db_pool {
            builtins_with_pool_and_memory(pool, memory_toolset)
                .await
                .map_err(|e| anyhow::anyhow!("tool registry init: {e}"))?
        } else {
            builtins_with_memory(memory_toolset)
        };

        let native = Arc::new(NativeAdapter::new().with_tools(tool_registry))
            as Arc<dyn loom_adapters::Adapter>;

        let native_specs = native
            .discover(&AdapterConfig {
                kind: AdapterKind::Native,
                endpoint: "native".into(),
                auth: None,
            })
            .await?;
        for spec in native_specs {
            registry.register(spec).await?;
        }

        let adapter_manager = Arc::new(AdapterManager::new(native));
        let executor: Arc<dyn CapabilityExecutor> = adapter_manager.clone();

        let orchestration: Arc<dyn OrchestrationEngine> =
            Arc::new(DagOrchestrator::new(registry.clone(), executor.clone()));

        let llm_config: loom_llm::OpenAiConfig = config.llm.clone().into();
        tracing::info!(
            "using LLM provider: model={}, base_url={}",
            llm_config.model,
            llm_config.base_url
        );
        // 在 llm_config 被移动前提取 model，用于填充 AgentLoopConfig
        let llm_model = llm_config.model.clone();
        let llm: Arc<dyn LlmProvider> = Arc::new(OpenAiProvider::new(llm_config));

        let mut agent_config: AgentLoopConfig = config.agent.clone().into();
        // AgentLoopConfig.model 需从 LLM 配置中获取（AgentConfig 不包含 model）
        agent_config.model = llm_model;

        // 根据模型上下文窗口自动计算压缩阈值
        // 若配置了 max_context_tokens，则 threshold = max_context_tokens * compression_ratio
        if let Some(ctx_window) = config.llm.max_context_tokens {
            let ratio = config.llm.compression_ratio.clamp(0.1, 0.95);
            let auto_threshold = (ctx_window as f32 * ratio) as usize;
            tracing::info!(
                "auto compression threshold: {} tokens (context_window={}, ratio={})",
                auto_threshold,
                ctx_window,
                ratio
            );
            agent_config.compression.threshold_tokens = auto_threshold;
        }

        let agent_loop = Arc::new(
            AgentLoop::new(
                llm,
                registry.clone(),
                executor.clone(),
                lifecycle.clone(),
                agent_config,
            )
            .with_checkpoint_saver(checkpoint_saver)
            .with_memory_manager(memory_manager)
            .with_delegation_persistence(kv_store.clone()),
        );

        // 计算自我唤醒基础 URL（推模式投递用）
        let self_base_url = config
            .server
            .self_base_url
            .clone()
            .unwrap_or_else(|| format!("http://{}:{}", config.server.host, config.server.port));

        // 注册会话唤醒器：后台任务完成时通过 self-POST /chat 唤醒父会话
        let waker: Arc<dyn SessionWaker> = Arc::new(crate::waker::SelfPostWaker::new(self_base_url.clone()));
        agent_loop.with_session_waker(waker);

        lifecycle.set_runner(agent_loop.clone());

        // 持久化恢复：从 PostgreSQL / 内存加载进程重启前未完成的后台任务
        // 原 Running 状态的任务会标记为 Unknown（进程已死，结果未知）
        let recovered = agent_loop.recover_background_tasks().await;
        if recovered > 0 {
            tracing::info!("recovered {} background task(s) from persistent store", recovered);
        }

        let db_status = if has_db { "postgres" } else { "memory-only" };
        tracing::info!("loom runtime assembled (registry+lifecycle+adapters+orchestration+bus+llm+agent, db={db_status})");

        Ok(Self {
            registry,
            lifecycle,
            executor,
            orchestration,
            bus,
            agent_loop,
            sessions,
        })
    }
}