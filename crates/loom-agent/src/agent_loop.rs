use async_trait::async_trait;
use loom_core::{
    AgentLifecycleManager, AgentRunner, CapabilityExecutor, CapabilityRegistry,
    CheckpointMetadata, CheckpointSaver, MemoryScope, Result, ToolContext,
};
use loom_llm::{ChatMessage, LlmProvider, Role};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::checkpoint::{in_memory_checkpoint_saver, make_checkpoint_scoped};
use crate::config::AgentLoopConfig;
use crate::context::token::estimate_tokens;
use crate::context::{ContextCompressor, ContextEngine};
use crate::delegation::DelegationManager;
use crate::hitl::AgentEvent;
use crate::loop_helpers::{find_interrupt_call, finished_event, last_turn_pair};
use crate::loop_types::{AgentState, LoopContext};
use crate::prompt::build_system_prompt;
use crate::tools::{dispatch_tool_calls, ToolResolver};
use std::time::{Duration, Instant};

// 重新导出，保持对外 API 路径不变（loom_agent::agent_loop::AgentRunResult 等）
pub use crate::loop_types::{AgentRunOutcome, AgentRunResult, AgentRunResultWithHistory};

/// Agent 对话循环 — Agent OS 的大脑
///
/// conversation_loop：
/// ```text
/// loop {
///   1. 构建 prompt（system + 历史 + 工具 schema）
///   2. LLM.chat() → 可能返回 tool_calls
///   3. 有 tool_calls → 并发执行工具 → 回填结果 → continue
///      - spawn_agent 直接调用 lifecycle.launch()（inline tool）
///        * 支持 tasks 批量委派（并行执行）
///        * 顶层 Agent 可 background=true 异步返回 handle
///        * 子 Agent 始终同步阻塞等待结果
///        * 支持 action=list/stop 控制子 Agent
///      - 其他工具走 CapabilityExecutor
///   4. 否则 → 返回最终文本
/// }
/// ```
#[derive(Clone)]
pub struct AgentLoop {
    pub(crate) llm: Arc<dyn LlmProvider>,
    pub(crate) registry: Arc<dyn CapabilityRegistry>,
    pub(crate) executor: Arc<dyn CapabilityExecutor>,
    pub(crate) config: AgentLoopConfig,
    /// 任务委派管理器（spawn_agent 工具的全部逻辑）
    pub(crate) delegation: DelegationManager,
    /// 单轮并发工具调用限制信号量
    pub(crate) tool_semaphore: Arc<Semaphore>,
    /// 工作目录（用于加载上下文文件）
    pub(crate) workspace_dir: Option<PathBuf>,
    /// 缓存的稳定层提示词（stable tier）
    pub(crate) cached_stable_prompt: Arc<Mutex<Option<String>>>,
    /// 检查点存储（用于 checkpoint + human-in-the-loop）
    pub(crate) checkpoint_saver: Arc<dyn CheckpointSaver>,
    /// 上下文压缩引擎
    pub(crate) compressor: Arc<ContextCompressor>,
    /// 执行进度事件发送器（可选，用于实时推送工具调用过程）
    pub(crate) progress_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::result::ProgressEvent>>,
    /// 记忆管理器（可选）：协调记忆召回 / 同步 / 生命周期钩子
    pub(crate) memory: Option<Arc<loom_memory::MemoryManager>>,
    /// 记忆作用域（tenant_id + user_id），用于多租户隔离
    pub(crate) scope: Option<MemoryScope>,
    /// 会话 ID（chat session），用于后台子 Agent 结果跨轮次回注
    /// 顶层 Agent 从 chat 请求继承，子 Agent 从父 Agent 继承。
    pub(crate) session_id: Option<String>,
    /// 父中断传播取消令牌：当父 Agent 被取消时，触发同步子 Agent 停止
    pub(crate) cancel_token: Option<tokio_util::sync::CancellationToken>,
}

/// 活性看门狗句柄：(后台任务 JoinHandle, 停止信号 Sender)
type WatchdogHandle = (tokio::task::JoinHandle<()>, tokio::sync::oneshot::Sender<()>);

/// 活性看门狗启动结果：(是否启用, 最近活动时间戳, 看门狗句柄)
type LivenessWatchdog = (
    bool,
    Arc<std::sync::Mutex<Instant>>,
    Option<WatchdogHandle>,
);

impl AgentLoop {
    pub fn new(
        llm: Arc<dyn LlmProvider>,
        registry: Arc<dyn CapabilityRegistry>,
        executor: Arc<dyn CapabilityExecutor>,
        lifecycle: Arc<dyn AgentLifecycleManager>,
        config: AgentLoopConfig,
    ) -> Self {
        let delegation = DelegationManager::new(lifecycle, config.clone());
        let tool_semaphore = Arc::new(Semaphore::new(config.max_concurrent_tools));
        let context_length = llm.context_length().unwrap_or(0);
        let context_config = config.compression.clone().into_context_config(context_length);
        let compressor = Arc::new(ContextCompressor::new(context_config, Some(llm.clone())));
        Self {
            llm,
            registry,
            executor,
            config,
            delegation,
            tool_semaphore,
            workspace_dir: std::env::current_dir().ok(),
            cached_stable_prompt: Arc::new(Mutex::new(None)),
            checkpoint_saver: in_memory_checkpoint_saver(),
            compressor,
            progress_tx: None,
            memory: None,
            scope: None,
            session_id: None,
            cancel_token: None,
        }
    }

    /// 设置进度事件发送器（用于实时推送工具调用过程）
    pub fn with_progress_sender(
        mut self,
        tx: tokio::sync::mpsc::UnboundedSender<crate::result::ProgressEvent>,
    ) -> Self {
        self.progress_tx = Some(tx);
        self
    }

    /// 发送进度事件（忽略发送失败，不影响主流程）
    fn emit_progress(&self, event: crate::result::ProgressEvent) {
        if let Some(tx) = &self.progress_tx {
            let _ = tx.send(event);
        }
    }

    /// 设置压缩配置
    pub fn with_compression_config(mut self, config: crate::config::CompressionConfig) -> Self {
        let context_length = self.llm.context_length().unwrap_or(0);
        let context_config = config.into_context_config(context_length);
        self.compressor = Arc::new(ContextCompressor::new(context_config, Some(self.llm.clone())));
        self
    }

    /// 设置检查点存储（用于持久化和 human-in-the-loop）
    pub fn with_checkpoint_saver(mut self, saver: Arc<dyn CheckpointSaver>) -> Self {
        self.checkpoint_saver = saver;
        self
    }

    /// 获取检查点存储引用
    pub fn checkpoint_saver(&self) -> Arc<dyn CheckpointSaver> {
        self.checkpoint_saver.clone()
    }

    /// 设置工作目录（用于上下文文件查找）
    pub fn with_workspace_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.workspace_dir = Some(dir.into());
        self
    }

    /// 设置记忆管理器（注入记忆召回 / 同步 / 生命周期钩子）
    pub fn with_memory_manager(mut self, mgr: Arc<loom_memory::MemoryManager>) -> Self {
        self.memory = Some(mgr);
        self
    }

    /// 设置记忆作用域（tenant_id + user_id），实现多租户隔离
    pub fn with_scope(mut self, scope: MemoryScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// 设置会话 ID（chat session），用于后台子 Agent 结果跨轮次回注
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// 设置父中断传播取消令牌
    ///
    /// 当该 token 被 cancel 时，正在执行的同步子 Agent 会被主动停止。
    pub fn with_cancel_token(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// 取出指定会话的所有已完成后台子 Agent 结果（用于跨轮次回注）
    ///
    /// 取出后从内部存储清除，避免重复注入。
    pub fn drain_background_results(
        &self,
        session_id: &str,
    ) -> Vec<(uuid::Uuid, String, crate::result::SubagentResult)> {
        self.delegation.drain_background_results_by_session(session_id)
    }

    /// exactly-once 投递：认领指定会话的所有 Pending 已完成任务
    ///
    /// 将 delivery_state 从 Pending 改为 Claimed（递增 attempts）。
    /// 调用方在成功注入历史后必须调用 `complete_background_delivery` 确认；
    /// 失败则调用 `release_background_delivery` 回退重试。
    pub fn claim_completed_tasks(
        &self,
        session_id: &str,
    ) -> Vec<crate::delegation::BackgroundTask> {
        self.delegation.claim_completed_tasks(session_id)
    }

    /// exactly-once 投递：确认投递成功，删除任务及其持久化记录
    ///
    /// `claim_ids` 与 `child_agent_ids` 一一对应；claim 不匹配的任务不会被删除。
    pub fn complete_background_delivery(
        &self,
        child_agent_ids: &[uuid::Uuid],
        claim_ids: &[Option<String>],
    ) {
        self.delegation
            .complete_background_delivery(child_agent_ids, claim_ids)
    }

    /// exactly-once 投递：释放认领，回退到 Pending 以便重试
    ///
    /// `claim_ids` 与 `child_agent_ids` 一一对应；claim 不匹配的任务不会被释放。
    /// 超过 MAX_DELIVERY_ATTEMPTS 的任务会被标记为 Dropped 并从持久化中删除。
    /// 返回被标记为 Dropped 的任务 ID 列表。
    pub fn release_background_delivery(
        &self,
        child_agent_ids: &[uuid::Uuid],
        claim_ids: &[Option<String>],
    ) -> Vec<uuid::Uuid> {
        self.delegation
            .release_background_delivery(child_agent_ids, claim_ids)
    }

    /// 设置会话唤醒器（推模式投递回调）
    pub fn with_session_waker(&self, waker: std::sync::Arc<dyn crate::delegation::SessionWaker>) {
        self.delegation.with_session_waker(waker);
    }

    /// 启用后台任务持久化（PostgreSQL / 内存）
    ///
    /// 启用后，后台子 Agent 任务的状态会持久化到 KV store，
    /// 进程重启后可通过 `recover_background_tasks` 恢复。
    pub fn with_delegation_persistence(self, kv: std::sync::Arc<dyn loom_core::JsonKeyValueStore>) -> Self {
        self.delegation.with_persistence(kv);
        self
    }

    /// 从持久化存储恢复未完成的后台任务（进程重启后调用）
    ///
    /// 原 Running 状态的任务会标记为 Unknown（进程已死，结果未知）。
    /// 返回恢复的任务数量。
    pub async fn recover_background_tasks(&self) -> usize {
        self.delegation.recover_persisted().await
    }

    /// 构建工具执行上下文（scope + session_id）
    ///
    /// session_id 优先级：self.session_id（chat 会话 ID）> fallback（agent_id/thread_id）。
    /// 使用 chat 会话 ID 作为 session_id，确保后台子 Agent 结果能跨轮次回注到同一父会话。
    fn build_tool_context(&self, fallback: &str) -> ToolContext {
        let scope = self.scope.clone().unwrap_or_default();
        let session_id = self.session_id.clone().unwrap_or_else(|| fallback.to_string());
        ToolContext::new(scope, session_id)
    }

    /// 返回当前记忆作用域（未设置时返回默认全局 scope）
    pub(crate) fn memory_scope(&self) -> MemoryScope {
        self.scope.clone().unwrap_or_default()
    }

    /// 获取或构建稳定层提示词（缓存）
    pub(crate) async fn get_stable_prompt(&self) -> String {
        let mut cache = self.cached_stable_prompt.lock().await;
        if let Some(stable) = cache.as_ref() {
            return stable.clone();
        }
        let parts = build_system_prompt(&self.config, "", "", &[], self.workspace_dir.as_deref());
        *cache = Some(parts.stable.clone());
        parts.stable
    }

    /// 失效缓存（配置变化或需要重建时调用）
    pub async fn invalidate_prompt_cache(&self) {
        *self.cached_stable_prompt.lock().await = None;
    }

    /// Agent 主循环（run_goal_with_event / resume / resume_from_checkpoint 共享）
    ///
    /// 每轮：LLM.chat → 检查 interrupt → 执行工具 → 保存 checkpoint → 继续
    /// 当消息 token 超过阈值时，自动压缩上下文（头尾保护 + 中间摘要）。
    ///
    /// 增强：
    /// - API 重试：指数退避 + 错误分类（429/5xx 重试，413 压缩恢复）
    /// - 工具 guardrails：去重、循环检测、连续失败检测
    pub(crate) async fn run_loop(&self, mut ctx: LoopContext) -> Result<(AgentEvent, Vec<ChatMessage>)> {
        let (tools, mut guardrails, valid_tool_names) = self.init_tools_and_guardrails(&ctx).await;
        let start = Instant::now();
        let (liveness_enabled, last_activity, _watchdog_handle) =
            self.start_liveness_watchdog(ctx.agent_id);

        loop {
            if self.begin_iteration(&mut ctx, liveness_enabled, &last_activity, start) {
                // 达到最大迭代次数：进入人工确认（HITL），让用户决定是否继续或接受当前结果
                let event = self.handle_max_iterations_interrupt(&mut ctx).await?;
                return Ok((event, ctx.messages));
            }

            self.on_turn_start_memory(&ctx).await;

            // 低成本工具修剪 + 压缩预检 + 执行压缩
            self.preflight_compression(&mut ctx).await?;

            // 记忆召回：基于最近一条用户消息 prefetch
            self.recall_memory_for_turn(&mut ctx).await;

            // 带重试的 LLM 调用，ContextOverflow 时压缩后重试一次
            let resp = self.call_llm_with_recovery(&mut ctx, &tools).await?;

            // 记录本轮 LLM token 使用量
            self.record_llm_usage(&ctx, &resp);

            // 无工具调用 → 正常结束
            if resp.tool_calls.is_empty() {
                let event = self.handle_final_response(&mut ctx, resp, start).await;
                return Ok((event, ctx.messages));
            }

            // 检测 interrupt 工具调用 → 触发 human-in-the-loop
            if let Some(interrupt_tc) = find_interrupt_call(&resp.tool_calls) {
                let interrupt_value = interrupt_tc.arguments.get("value").cloned();
                let event = self
                    .handle_interrupt(&mut ctx, interrupt_value, resp.tool_calls)
                    .await?;
                return Ok((event, ctx.messages));
            }

            // 工具 guardrails：校验 / 去重 / 循环检测
            let (deduped_calls, invalid_results, halt_event) = self.run_guardrails(
                &mut ctx,
                &resp,
                &mut guardrails,
                &valid_tool_names,
                start,
            );
            if let Some(event) = halt_event {
                return Ok((event, ctx.messages));
            }

            // 若去重后无有效调用，则把无效工具的错误返回给模型
            if deduped_calls.is_empty() {
                ctx.messages.push(ChatMessage::assistant_with_tool_calls(resp.tool_calls.clone()));
                for (tc_id, err_msg) in &invalid_results {
                    ctx.messages.push(ChatMessage::tool(tc_id, err_msg));
                }
                ctx.tool_calls_made += invalid_results.len();
                continue;
            }

            // 执行一轮工具调用并回填结果
            let halt_event = self
                .execute_tools_round(&mut ctx, deduped_calls, invalid_results, &mut guardrails, start)
                .await?;
            if let Some(event) = halt_event {
                return Ok((event, ctx.messages));
            }

            // 工具执行后：记忆同步 + turn_complete + step checkpoint
            self.post_tool_housekeeping(&mut ctx).await?;
        }
    }
    // ── run_loop 辅助方法 ──────────────────────────────────────────────

    /// 解析本轮可用工具并构建 guardrails / 白名单
    async fn init_tools_and_guardrails(
        &self,
        ctx: &LoopContext,
    ) -> (
        Vec<loom_llm::ToolDefinition>,
        crate::tools::ToolGuardrailController,
        std::collections::HashSet<String>,
    ) {
        let tools = ToolResolver::new(self.registry.as_ref(), &self.config)
            .available_tools(&ctx.toolsets, &ctx.parent_toolsets, ctx.delegate_depth)
            .await;
        let guardrails = crate::tools::ToolGuardrailController::new(self.config.guardrails.clone());
        let valid_tool_names: std::collections::HashSet<String> =
            tools.iter().map(|t| t.name.clone()).collect();
        (tools, guardrails, valid_tool_names)
    }

    /// 启动活性看门狗（若启用），返回 (enabled, last_activity, handle)
    fn start_liveness_watchdog(&self, agent_id: Uuid) -> LivenessWatchdog {
        let liveness = self.config.liveness.clone();
        let last_activity = Arc::new(std::sync::Mutex::new(Instant::now()));
        if !liveness.enabled {
            return (false, last_activity, None);
        }
        let la = last_activity.clone();
        let agent_id = agent_id.to_string();
        let timeout = Duration::from_secs(liveness.timeout_secs);
        let interval = Duration::from_secs(liveness.check_interval_secs);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let elapsed = la.lock().unwrap().elapsed();
                        if elapsed >= timeout {
                            tracing::warn!(
                                "[liveness] agent {} has been inactive for {:.0}s (timeout={}s). \
                                 The loop may be stuck on a slow tool or API call.",
                                agent_id,
                                elapsed.as_secs_f64(),
                                timeout.as_secs()
                            );
                        }
                    }
                    _ = &mut stop_rx => {
                        break;
                    }
                }
            }
        });
        (true, last_activity, Some((handle, stop_tx)))
    }

    /// 迭代开始：心跳 + 计数 + 超限检查。
    ///
    /// 返回 `true` 表示已达到最大迭代次数，调用方应发起一次总结 LLM 调用后结束。
    /// 返回 `false` 表示正常继续。
    fn begin_iteration(
        &self,
        ctx: &mut LoopContext,
        liveness_enabled: bool,
        last_activity: &Arc<std::sync::Mutex<Instant>>,
        _start: Instant,
    ) -> bool {
        if liveness_enabled {
            *last_activity.lock().unwrap() = Instant::now();
        }
        ctx.iterations += 1;
        if let Some(state) = &ctx.activity_state {
            if let Ok(mut s) = state.lock() {
                s.iterations = ctx.iterations as u64;
                s.api_call_count = ctx.iterations as u64;
                s.current_tool = None;
                s.last_activity_ts = Some(chrono::Utc::now());
            }
        }
        self.emit_progress(crate::result::ProgressEvent::Iteration {
            iteration: ctx.iterations,
        });
        if ctx.iterations > self.config.max_iterations {
            tracing::warn!(
                "agent {} reached max iterations ({})",
                ctx.agent_id,
                self.config.max_iterations
            );
            return true;
        }
        tracing::debug!("agent {} iteration {} (session_id={:?})", ctx.agent_id, ctx.iterations, self.session_id);
        false
    }

    /// 记忆 turn-start 钩子
    async fn on_turn_start_memory(&self, ctx: &LoopContext) {
        if let Some(memory) = &self.memory {
            let turn_msg = ctx
                .messages
                .iter()
                .rev()
                .find(|m| m.role == Role::User)
                .and_then(|m| m.content.clone())
                .unwrap_or_default();
            memory
                .on_turn_start(ctx.iterations, &turn_msg, &ctx.thread_id)
                .await;
        }
    }

    /// 低成本工具修剪 + 压缩预检 + 执行压缩
    async fn preflight_compression(&self, ctx: &mut LoopContext) -> Result<()> {
        let status = self.compressor.get_status(&ctx.messages);
        if status.approaching_limit && !self.compressor.should_compress(&ctx.messages) {
            let before = ctx.messages.len();
            ctx.messages = self.compressor.prune_tool_results_only(ctx.messages.clone());
            if ctx.messages.len() != before {
                tracing::debug!(
                    "agent {} light tool-output pruning: {}→{} messages (usage {:.1}%)",
                    ctx.agent_id,
                    before,
                    ctx.messages.len(),
                    status.usage_percent
                );
            }
        }

        let preflight = self.compressor.should_compress_preflight(&ctx.messages);
        if !preflight.should_compress {
            return Ok(());
        }
        tracing::debug!(
            "agent {} compression preflight: {}",
            ctx.agent_id,
            preflight.reason
        );

        let mut memory_insights: Option<String> = None;
        if let Some(memory) = &self.memory {
            let scope = self.memory_scope();
            let insights = memory.on_pre_compress(&scope, &ctx.messages).await;
            if !insights.is_empty() {
                tracing::debug!(
                    "memory pre-compress insights for agent {}: {} chars",
                    ctx.agent_id,
                    insights.chars().count()
                );
                memory_insights = Some(insights);
            }
        }
        match self
            .compressor
            .compress(ctx.messages.clone(), memory_insights)
            .await
        {
            Ok(Some(compressed)) => {
                let stats = self.compressor.stats();
                tracing::info!(
                    "agent {} compressing context: {}→{} tokens, {}→{} messages, \
                     tool_outputs_pruned={}, deduped={}, skill_pruned={}, llm_summary={}",
                    ctx.agent_id,
                    stats.tokens_before,
                    stats.tokens_after,
                    stats.messages_before,
                    stats.messages_after,
                    stats.tool_outputs_pruned,
                    stats.tool_outputs_deduped,
                    stats.skill_results_pruned,
                    stats.used_llm_summary
                );
                ctx.messages = compressed;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    "agent {} context compression failed, continuing without compression: {}",
                    ctx.agent_id,
                    e
                );
            }
        }
        Ok(())
    }

    /// 记忆召回：基于最近一条用户消息 prefetch
    async fn recall_memory_for_turn(&self, ctx: &mut LoopContext) {
        if let Some(memory) = &self.memory {
            if let Some(latest_user) = ctx
                .messages
                .iter()
                .rev()
                .find(|m| m.role == Role::User)
                .and_then(|m| m.content.as_ref())
            {
                let scope = self.memory_scope();
                let recall = memory.prefetch_all(&scope, latest_user, &ctx.thread_id).await;
                if !recall.is_empty() {
                    ctx.messages.push(ChatMessage::system(recall));
                }
            }
        }
    }

    /// 带重试的 LLM 调用，ContextOverflow 时压缩后重试一次
    async fn call_llm_with_recovery(
        &self,
        ctx: &mut LoopContext,
        tools: &[loom_llm::ToolDefinition],
    ) -> Result<loom_llm::ChatResponse> {
        tracing::debug!(
            "[agent_loop] llm chat: agent_id={}, session_id={:?}, messages={}, tools={}, estimated_tokens={}",
            ctx.agent_id,
            self.session_id,
            ctx.messages.len(),
            tools.len(),
            estimate_tokens(&ctx.messages)
        );
        match crate::api_retry::chat_with_retry(
            &*self.llm,
            ctx.messages.clone(),
            tools.to_vec(),
            &self.config.api_retry,
        )
        .await
        {
            Ok(resp) => Ok(resp),
            Err(loom_core::LoomError::LlmApi(api_err))
                if api_err.kind == loom_core::LlmErrorKind::ContextOverflow =>
            {
                tracing::warn!(
                    "[agent_loop] context overflow (status={:?}), compressing and retrying",
                    api_err.status_code
                );
                match self.compressor.compress(ctx.messages.clone(), None).await {
                    Ok(Some(compressed)) => {
                        let stats = self.compressor.stats();
                        tracing::info!(
                            "[agent_loop] context overflow recovery: {}→{} tokens, {}→{} messages",
                            stats.tokens_before,
                            stats.tokens_after,
                            stats.messages_before,
                            stats.messages_after
                        );
                        ctx.messages = compressed;
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[agent_loop] context overflow but compression produced no change"
                        );
                    }
                    Err(e) => {
                        tracing::warn!("[agent_loop] context overflow compression failed: {}", e);
                    }
                }
                crate::api_retry::chat_with_retry(
                    &*self.llm,
                    ctx.messages.clone(),
                    tools.to_vec(),
                    &self.config.api_retry,
                )
                .await
            }
            Err(e) => Err(e),
        }
    }

    /// 记录本轮 LLM token 使用量
    fn record_llm_usage(&self, ctx: &LoopContext, resp: &loom_llm::ChatResponse) {
        let (prompt_tokens, completion_tokens) = match &resp.usage {
            Some(usage) => (usage.prompt_tokens, usage.completion_tokens),
            None => (estimate_tokens(&ctx.messages), 0),
        };
        self.compressor
            .record_token_usage(prompt_tokens, completion_tokens);
        self.compressor.note_message_count(ctx.messages.len());
    }

    /// 处理无工具调用的最终回复
    async fn handle_final_response(
        &self,
        ctx: &mut LoopContext,
        resp: loom_llm::ChatResponse,
        start: Instant,
    ) -> AgentEvent {
        let final_text = resp
            .content
            .unwrap_or_else(|| "(empty response)".to_string());
        ctx.messages.push(ChatMessage::assistant(final_text.clone()));
        if let Some(memory) = &self.memory {
            let scope = self.memory_scope();
            let (u, a) = last_turn_pair(&ctx.messages);
            memory.sync_all(&scope, &u, &a, &ctx.thread_id, &ctx.messages).await;
            memory.on_session_end(&scope, &ctx.messages).await;
        }
        tracing::info!(
            "agent {} finished after {} iterations (session_id={:?})",
            ctx.agent_id,
            ctx.iterations,
            self.session_id
        );
        finished_event(
            &ctx.messages,
            ctx.iterations,
            ctx.tool_calls_made,
            &ctx.tool_names,
            final_text,
            start,
        )
    }

    /// 处理 interrupt 工具调用（HITL）
    async fn handle_interrupt(
        &self,
        ctx: &mut LoopContext,
        interrupt_value: Option<serde_json::Value>,
        all_tool_calls: Vec<loom_llm::ToolCall>,
    ) -> Result<AgentEvent> {
        let interrupt_value = interrupt_value.unwrap_or_else(|| {
            tracing::debug!("interrupt tool called without 'value' argument, using {{}}");
            json!({})
        });
        tracing::debug!(
            "[HITL] interrupt detected: agent_id={}, iteration={}, value={}",
            ctx.agent_id,
            ctx.iterations,
            interrupt_value
        );
        ctx.messages.push(ChatMessage::assistant_with_tool_calls(all_tool_calls.clone()));
        ctx.tool_calls_made += all_tool_calls.len();
        let cp = self.save_checkpoint(ctx, "interrupt").await?;
        ctx.last_checkpoint_id = Some(cp.id.clone());
        tracing::info!("agent {} interrupted at checkpoint {} (session_id={:?})", ctx.agent_id, cp.id, self.session_id);
        if let Some(memory) = &self.memory {
            let scope = self.memory_scope();
            let (u, a) = last_turn_pair(&ctx.messages);
            memory.sync_all(&scope, &u, &a, &ctx.thread_id, &ctx.messages).await;
            memory.on_session_end(&scope, &ctx.messages).await;
        }
        Ok(AgentEvent::Interrupt {
            value: interrupt_value,
            checkpoint_id: cp.id,
            thread_id: ctx.thread_id.clone(),
        })
    }

    /// 达到最大迭代次数时，进入人工确认（HITL）。
    ///
    /// 保存 checkpoint 后返回 Interrupt 事件，前端展示提示让用户选择：
    /// - 继续执行（恢复后 reset iteration 计数）
    /// - 停止并让 AI 总结当前结果
    async fn handle_max_iterations_interrupt(&self, ctx: &mut LoopContext) -> Result<AgentEvent> {
        tracing::info!(
            "agent {} max iterations reached, entering HITL confirmation (session_id={:?})",
            ctx.agent_id,
            self.session_id
        );
        let cp = self.save_checkpoint(ctx, "max_iterations").await?;
        ctx.last_checkpoint_id = Some(cp.id.clone());
        if let Some(memory) = &self.memory {
            let scope = self.memory_scope();
            let (u, a) = last_turn_pair(&ctx.messages);
            memory.sync_all(&scope, &u, &a, &ctx.thread_id, &ctx.messages).await;
            memory.on_session_end(&scope, &ctx.messages).await;
        }
        let value = json!({
            "type": "max_iterations",
            "message": format!("已达到最大迭代次数（{}），请选择下一步操作：", self.config.max_iterations),
            "iterations": ctx.iterations,
            "max_iterations": self.config.max_iterations,
            "tool_calls_made": ctx.tool_calls_made,
            "options": [
                {"label": "继续执行", "value": {"action": "continue"}},
                {"label": "停止并总结", "value": {"action": "summarize"}}
            ]
        });
        Ok(AgentEvent::Interrupt {
            value,
            checkpoint_id: cp.id,
            thread_id: ctx.thread_id.clone(),
        })
    }

    /// 工具 guardrails：校验 / 去重 / 循环检测
    /// 返回 (有效调用, 无效结果, 结束事件)
    fn run_guardrails(
        &self,
        ctx: &mut LoopContext,
        resp: &loom_llm::ChatResponse,
        guardrails: &mut crate::tools::ToolGuardrailController,
        valid_tool_names: &std::collections::HashSet<String>,
        start: Instant,
    ) -> (
        Vec<loom_llm::ToolCall>,
        Vec<(String, String)>,
        Option<AgentEvent>,
    ) {
        let (valid_calls, invalid_results) =
            guardrails.validate_tool_names(resp.tool_calls.clone(), valid_tool_names);
        let deduped_calls = guardrails.deduplicate(valid_calls);
        let loop_verdict = guardrails.check_and_record(&deduped_calls);
        match &loop_verdict {
            crate::tools::GuardrailVerdict::Halt(msg) => {
                tracing::warn!("[guardrail] halt: {}", msg);
                ctx.messages.push(ChatMessage::assistant_with_tool_calls(
                    resp.tool_calls.clone(),
                ));
                for tc in &deduped_calls {
                    ctx.messages.push(ChatMessage::tool(&tc.id, msg.clone()));
                }
                ctx.tool_calls_made += deduped_calls.len();
                for tc in &deduped_calls {
                    ctx.tool_names.push(tc.name.clone());
                }
                let event = finished_event(
                    &ctx.messages,
                    ctx.iterations,
                    ctx.tool_calls_made,
                    &ctx.tool_names,
                    format!("Execution halted by guardrail: {msg}"),
                    start,
                );
                (vec![], invalid_results, Some(event))
            }
            crate::tools::GuardrailVerdict::Warn(msg) => {
                tracing::debug!("[guardrail] warn: {}", msg);
                (deduped_calls, invalid_results, None)
            }
            crate::tools::GuardrailVerdict::Pass => (deduped_calls, invalid_results, None),
        }
    }

    /// 执行一轮工具调用并回填结果。返回 Some 表示因连续失败而结束。
    async fn execute_tools_round(
        &self,
        ctx: &mut LoopContext,
        deduped_calls: Vec<loom_llm::ToolCall>,
        invalid_results: Vec<(String, String)>,
        guardrails: &mut crate::tools::ToolGuardrailController,
        start: Instant,
    ) -> Result<Option<AgentEvent>> {
        ctx.tool_calls_made += deduped_calls.len() + invalid_results.len();
        for tc in &deduped_calls {
            ctx.tool_names.push(tc.name.clone());
        }
        tracing::debug!(
            "agent {}: {} tool call(s) after dedup/validation: {:?}",
            ctx.agent_id,
            deduped_calls.len(),
            deduped_calls.iter().map(|t| &t.name).collect::<Vec<_>>()
        );

        ctx.messages.push(ChatMessage::assistant_with_tool_calls(deduped_calls.clone()));

        for tc in &deduped_calls {
            let args = if tc.arguments.is_null() {
                None
            } else {
                Some(tc.arguments.clone())
            };
            self.emit_progress(crate::result::ProgressEvent::ToolStart {
                tool: tc.name.clone(),
                arguments: args,
            });
        }

        if let Err(e) = self.save_checkpoint(ctx, "pre_tool_exec").await {
            tracing::error!(
                "[agent_loop] persist-before-execute failed for agent {}: {}. \
                 Aborting turn to avoid running destructive tools from in-memory state.",
                ctx.agent_id,
                e
            );
            return Err(e);
        }

        let tool_ctx = self.build_tool_context(&ctx.thread_id);
        if let Some(state) = &ctx.activity_state {
            if let Ok(mut s) = state.lock() {
                s.current_tool = deduped_calls.first().map(|tc| tc.name.clone());
                s.last_activity_ts = Some(chrono::Utc::now());
            }
        }
        let results = dispatch_tool_calls(
            &deduped_calls,
            ctx.agent_id,
            ctx.parent_agent_id,
            ctx.delegate_depth,
            &ctx.toolsets,
            &tool_ctx,
            &self.delegation,
            self.registry.as_ref(),
            self.executor.as_ref(),
            &self.tool_semaphore,
            self.cancel_token.clone(),
        )
        .await;
        if let Some(state) = &ctx.activity_state {
            if let Ok(mut s) = state.lock() {
                s.current_tool = None;
                s.last_activity_ts = Some(chrono::Utc::now());
            }
        }
        tracing::debug!(
            "[agent_loop] tool dispatch done: agent_id={}, results={}, errors={}",
            ctx.agent_id,
            results.len(),
            results.iter().filter(|r| r.is_error).count()
        );

        let failure_results: Vec<(String, bool)> = results
            .iter()
            .map(|r| (r.name.clone(), r.is_error))
            .collect();
        let failure_verdict = guardrails.record_results(&failure_results);
        match &failure_verdict {
            crate::tools::GuardrailVerdict::Halt(msg) => {
                tracing::warn!("[guardrail] consecutive failures halt: {}", msg);
                for r in &results {
                    ctx.messages.push(ChatMessage::tool(&r.tool_call_id, &r.content));
                    self.emit_progress(crate::result::ProgressEvent::ToolEnd {
                        tool: r.name.clone(),
                        result: r.content.clone(),
                        is_error: r.is_error,
                    });
                }
                if let Some(last) = ctx.messages.last_mut() {
                    if let Some(ref mut content) = last.content {
                        content.push_str(&format!("\n\n[GUARDRAIL HALT] {msg}"));
                    }
                }
                let event = finished_event(
                    &ctx.messages,
                    ctx.iterations,
                    ctx.tool_calls_made,
                    &ctx.tool_names,
                    format!("Execution halted by guardrail: {msg}"),
                    start,
                );
                return Ok(Some(event));
            }
            crate::tools::GuardrailVerdict::Warn(msg) => {
                tracing::debug!("[guardrail] failure warn: {}", msg);
            }
            crate::tools::GuardrailVerdict::Pass => {}
        }

        for r in &results {
            ctx.messages.push(ChatMessage::tool(&r.tool_call_id, &r.content));
            self.emit_progress(crate::result::ProgressEvent::ToolEnd {
                tool: r.name.clone(),
                result: r.content.clone(),
                is_error: r.is_error,
            });
        }
        for (tc_id, err_msg) in &invalid_results {
            ctx.messages.push(ChatMessage::tool(tc_id, err_msg));
        }
        Ok(None)
    }

    /// 工具执行后：记忆同步 + turn_complete + step checkpoint
    async fn post_tool_housekeeping(&self, ctx: &mut LoopContext) -> Result<()> {
        if let Some(memory) = &self.memory {
            let scope = self.memory_scope();
            let (u, a) = last_turn_pair(&ctx.messages);
            memory.sync_all(&scope, &u, &a, &ctx.thread_id, &ctx.messages).await;
        }
        if let Err(e) = self.compressor.on_turn_complete(&ctx.messages).await {
            tracing::warn!(
                "agent {} compressor on_turn_complete error: {}",
                ctx.agent_id,
                e
            );
        }
        let interval = self.config.checkpoint_interval;
        if interval > 0 && ctx.iterations.is_multiple_of(interval) {
            let cp = self.save_checkpoint(ctx, "step").await?;
            ctx.last_checkpoint_id = Some(cp.id);
        }
        Ok(())
    }

    /// 保存检查点
    async fn save_checkpoint(
        &self,
        ctx: &LoopContext,
        source: &str,
    ) -> Result<loom_core::Checkpoint> {
        let scope = self.scope.clone().unwrap_or_default();

        let state = AgentState::from_runtime(ctx, &scope, Some(self.compressor.snapshot_state()));
        let channel_values = serde_json::to_value(state)?;

        let metadata = CheckpointMetadata {
            step: ctx.iterations as u64,
            source: source.to_string(),
            extra: std::collections::HashMap::new(),
        };

        let cp = make_checkpoint_scoped(
            &ctx.thread_id,
            ctx.last_checkpoint_id.clone(),
            channel_values,
            metadata,
            scope.tenant_id,
            scope.user_id,
        );
        self.checkpoint_saver.put(cp).await
    }
}

#[async_trait]
impl AgentRunner for AgentLoop {
    async fn run(&self, spec: &loom_core::AgentSpec) -> Result<loom_core::AgentOutput> {
        // 子 Agent 继承父 Agent 的 scope（租户/用户隔离）和 session_id（后台结果回注）
        let mut scoped = self.clone();
        if let Some(scope) = spec.scope.clone() {
            scoped = scoped.with_scope(scope);
        }
        if let Some(session_id) = spec.session_id.clone() {
            scoped = scoped.with_session_id(session_id);
        }
        let result = scoped
            .run_goal(
                &spec.goal,
                &spec.context,
                spec.agent_id,
                spec.parent_agent_id,
                spec.delegate_depth,
                &spec.toolsets,
                &spec.parent_toolsets,
                spec.activity_state.clone(),
            )
            .await;
        match result {
            Ok(r) => Ok(loom_core::AgentOutput::success_with_meta(
                r.final_response,
                r.iterations,
                r.tool_calls_made,
                r.tool_names,
                r.duration_ms,
                r.tool_trace,
            )),
            Err(e) => {
                let msg = e.to_string();
                let kind = loom_core::AgentErrorKind::from_error_message(&msg);
                Ok(loom_core::AgentOutput::failure_with_kind(
                    msg, 0, 0, kind,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_core::CheckpointConfig;
    use loom_llm::{ChatChunk, ChatResponse, Role, ToolDefinition};

    /// 旧格式 checkpoint（缺少新字段）能正确反序列化，
    /// 新字段使用默认值，保证向后兼容。
    #[test]
    fn test_agent_state_backward_compat() {
        let old_format = serde_json::json!({
            "messages": [
                {"role": "system", "content": "you are an agent"},
                {"role": "user", "content": "hello"}
            ],
            "iterations": 3,
            "tool_calls_made": 2,
            "tool_names": ["search", "read_file"]
        });

        let state: AgentState = serde_json::from_value(old_format).unwrap();
        assert_eq!(state.iterations, 3);
        assert_eq!(state.tool_calls_made, 2);
        assert_eq!(state.tool_names, vec!["search", "read_file"]);
        assert_eq!(state.messages.len(), 2);

        assert!(state.toolsets.is_empty());
        assert!(state.parent_toolsets.is_empty());
        assert_eq!(state.delegate_depth, 0);
        assert!(state.scope_json.is_none());
        assert!(state.last_user_message.is_none());
    }

    /// 包含完整字段的 AgentState 能正确序列化/反序列化
    #[test]
    fn test_agent_state_full_roundtrip() {
        let state = AgentState {
            messages: vec![
                ChatMessage::system("sys".to_string()),
                ChatMessage::user("goal".to_string()),
                ChatMessage::assistant("response".to_string()),
            ],
            iterations: 5,
            tool_calls_made: 3,
            tool_names: vec!["t1".to_string(), "t2".to_string()],
            toolsets: vec!["default".to_string()],
            parent_toolsets: vec![],
            delegate_depth: 1,
            parent_agent_id: Some(Uuid::nil()),
            scope_json: Some(r#"{"tenant_id":"t1","user_id":"u1"}"#.to_string()),
            last_user_message: Some("goal".to_string()),
            compression_state: None,
        };

        let json = serde_json::to_value(&state).unwrap();
        let restored: AgentState = serde_json::from_value(json).unwrap();

        assert_eq!(restored.iterations, 5);
        assert_eq!(restored.tool_calls_made, 3);
        assert_eq!(restored.toolsets, vec!["default"]);
        assert_eq!(restored.delegate_depth, 1);
        assert_eq!(
            restored.scope_json.as_deref(),
            Some(r#"{"tenant_id":"t1","user_id":"u1"}"#)
        );
        assert_eq!(restored.last_user_message.as_deref(), Some("goal"));
        assert_eq!(restored.messages.last().unwrap().role, Role::Assistant);
    }

    /// 验证 save_checkpoint 保存了完整的运行时上下文（toolsets/delegate_depth/scope）
    #[tokio::test]
    async fn test_save_checkpoint_persists_full_state() {
        let saver = crate::checkpoint::in_memory_checkpoint_saver();

        let config = AgentLoopConfig::default();
        let lifecycle: Arc<dyn AgentLifecycleManager> = Arc::new(MockLifecycle);
        let agent_loop = AgentLoop {
            llm: Arc::new(MockLlmProvider),
            registry: Arc::new(MockRegistry),
            executor: Arc::new(MockExecutor),
            config: config.clone(),
            delegation: DelegationManager::new(lifecycle, config.clone()),
            tool_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            workspace_dir: None,
            cached_stable_prompt: Arc::new(Mutex::new(None)),
            checkpoint_saver: saver.clone(),
            compressor: Arc::new(ContextCompressor::new(
                config.compression.clone().into_context_config(0),
                None,
            )),
            progress_tx: None,
            memory: None,
            scope: Some(loom_core::MemoryScope::new(
                Some("tenant-1".to_string()),
                Some("user-1".to_string()),
            )),
            session_id: None,
            cancel_token: None,
        };

        let ctx = LoopContext {
            messages: vec![
                ChatMessage::system("sys".to_string()),
                ChatMessage::user("hi".to_string()),
            ],
            iterations: 2,
            tool_calls_made: 1,
            tool_names: vec!["search".to_string()],
            thread_id: "thread-test".to_string(),
            last_checkpoint_id: None,
            agent_id: Uuid::new_v4(),
            parent_agent_id: None,
            delegate_depth: 0,
            toolsets: vec!["default".to_string()],
            parent_toolsets: vec![],
            activity_state: None,
        };

        let cp = agent_loop.save_checkpoint(&ctx, "step").await.unwrap();

        let config = CheckpointConfig::new("thread-test")
            .with_tenant(Some("tenant-1".to_string()), Some("user-1".to_string()));
        let tuple = saver
            .get_tuple(&config)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tuple.checkpoint.id, cp.id);

        let state: AgentState =
            serde_json::from_value(tuple.checkpoint.channel_values).unwrap();
        assert_eq!(state.iterations, 2);
        assert_eq!(state.tool_calls_made, 1);
        assert_eq!(state.tool_names, vec!["search"]);
        assert_eq!(state.toolsets, vec!["default"]);
        assert_eq!(state.delegate_depth, 0);
        assert!(state.scope_json.is_some());
        assert_eq!(state.last_user_message.as_deref(), Some("hi"));
    }

    // ---- Mock 实现（仅用于 save_checkpoint 测试，不触发真实 LLM/工具调用）----

    struct MockLlmProvider;
    #[async_trait]
    impl LlmProvider for MockLlmProvider {
        async fn chat(
            &self,
            _messages: Vec<ChatMessage>,
            _tools: Vec<ToolDefinition>,
        ) -> Result<ChatResponse> {
            unimplemented!()
        }
        async fn chat_stream(
            &self,
            _messages: Vec<ChatMessage>,
            _tools: Vec<ToolDefinition>,
        ) -> Result<futures::stream::BoxStream<'static, Result<ChatChunk>>> {
            unimplemented!()
        }
    }

    struct MockRegistry;
    #[async_trait]
    impl CapabilityRegistry for MockRegistry {
        async fn register(&self, _spec: loom_core::CapabilitySpec) -> Result<Uuid> {
            unimplemented!()
        }
        async fn unregister(&self, _id: &Uuid) -> Result<()> {
            unimplemented!()
        }
        async fn get(&self, _id: &Uuid) -> Result<loom_core::CapabilitySpec> {
            unimplemented!()
        }
        async fn get_by_name(&self, _name: &str) -> Result<loom_core::CapabilitySpec> {
            unimplemented!()
        }
        async fn list(&self) -> Result<Vec<loom_core::CapabilitySpec>> {
            unimplemented!()
        }
        async fn list_by_kind(
            &self,
            _kind: loom_core::CapabilityKind,
        ) -> Result<Vec<loom_core::CapabilitySpec>> {
            unimplemented!()
        }
        async fn list_by_tags(&self, _tags: &[String]) -> Result<Vec<loom_core::CapabilitySpec>> {
            unimplemented!()
        }
    }

    struct MockExecutor;
    #[async_trait]
    impl CapabilityExecutor for MockExecutor {
        async fn execute(
            &self,
            _spec: &loom_core::CapabilitySpec,
            _args: serde_json::Value,
            _ctx: &ToolContext,
        ) -> Result<loom_core::CapabilityExecution> {
            unimplemented!()
        }
        async fn execute_stream(
            &self,
            _spec: &loom_core::CapabilitySpec,
            _args: serde_json::Value,
            _ctx: &ToolContext,
        ) -> Result<futures::stream::BoxStream<'static, Result<serde_json::Value>>> {
            unimplemented!()
        }
    }

    struct MockLifecycle;
    #[async_trait]
    impl AgentLifecycleManager for MockLifecycle {
        async fn launch(&self, _req: loom_core::AgentLaunchRequest) -> Result<loom_core::AgentHandle> {
            unimplemented!()
        }
        async fn stop(&self, _id: &Uuid) -> Result<()> {
            unimplemented!()
        }
        async fn pause(&self, _id: &Uuid) -> Result<()> {
            unimplemented!()
        }
        async fn resume(&self, _id: &Uuid) -> Result<()> {
            unimplemented!()
        }
        async fn destroy(&self, _id: &Uuid) -> Result<()> {
            unimplemented!()
        }
        async fn get_status(&self, _id: &Uuid) -> Result<loom_core::AgentHandle> {
            unimplemented!()
        }
        async fn list(&self) -> Result<Vec<loom_core::AgentHandle>> {
            unimplemented!()
        }
        async fn send_message(&self, _id: &Uuid, _msg: loom_core::AgentMessage) -> Result<()> {
            unimplemented!()
        }
        async fn get_activity_summary(
            &self,
            _id: &Uuid,
        ) -> Result<loom_core::ActivitySummary> {
            unimplemented!()
        }
        async fn wait_for_result(&self, _id: &Uuid) -> Result<loom_core::AgentOutput> {
            unimplemented!()
        }
    }
}