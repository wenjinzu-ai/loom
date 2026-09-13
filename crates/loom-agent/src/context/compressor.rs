//! ContextCompressor — 默认上下文压缩引擎
//!
//! 编排所有压缩阶段：
//! 1. 主动工具修剪（可选，基于 token 压力）
//! 2. 工具输出基础修剪（超长截断）
//! 3. 重复工具输出去重
//! 4. 大技能结果修剪为标记
//! 5. 压缩边界计算（头部保护 + 尾部预算）
//! 6. LLM 辅助结构化摘要（失败时 fallback）
//! 7. 技能修剪标记重注入
//! 8. 组装最终消息（前缀 + 摘要 + 头尾）

use std::sync::Mutex;

use async_trait::async_trait;
use loom_llm::{ChatMessage, LlmProvider, Role};
use std::sync::Arc;

use super::boundary::compute_compress_window;
use super::config::CompressionConfig;
use super::engine::{CompressPreflight, ContextEngine, EngineStatus};
use super::state::{CompressionStats, CompressionState, CompressionStateSnapshot};
use super::summary::{
    assemble_summary_message, fallback_summary, generate_summary, SummaryResult,
    TRUNCATED_SUMMARY_MARKER,
};
use super::token::{estimate_tokens, TokenTracker};
use super::tool_pruning::{
    dedupe_tool_results, prune_large_skill_results, prune_tool_outputs, pressure_demote_tail,
    reinject_pruned_skill_markers,
};

/// 压缩延续用户消息：当压缩后无 user 消息时注入，保持对话格式合法
const COMPRESSION_CONTINUATION_USER_CONTENT: &str =
    "(context compaction complete — previous turns condensed above. \
    Continue from the most recent state; no new user instruction was provided.)";

/// 默认上下文压缩引擎
pub struct ContextCompressor {
    config: Mutex<CompressionConfig>,
    /// 辅助 LLM provider（用于生成摘要）
    auxiliary_llm: Option<Arc<dyn LlmProvider>>,
    /// 摘要 fallback provider（主辅助 LLM 失败时使用）
    fallback_llm: Option<Arc<dyn LlmProvider>>,
    /// 固定的摘要路由（单次消费，用于摘要失败后重试时切换到 fallback）
    pinned_summary_route: Mutex<Option<Arc<dyn LlmProvider>>>,
    /// 压缩状态（跨调用持久）
    state: Mutex<CompressionState>,
    /// token 追踪
    token_tracker: Mutex<TokenTracker>,
    /// 统计信息
    stats: Mutex<CompressionStats>,
}

impl ContextCompressor {
    /// 创建压缩引擎
    ///
    /// # 参数
    /// - `config`：压缩配置
    /// - `auxiliary_llm`：辅助 LLM provider，None 时使用 fallback 摘要
    pub fn new(config: CompressionConfig, auxiliary_llm: Option<Arc<dyn LlmProvider>>) -> Self {
        Self::with_fallback(config, auxiliary_llm, None)
    }

    /// 创建压缩引擎（带 fallback LLM）
    ///
    /// # 参数
    /// - `config`：压缩配置
    /// - `auxiliary_llm`：主辅助 LLM provider
    /// - `fallback_llm`：摘要失败时的 fallback provider
    pub fn with_fallback(
        config: CompressionConfig,
        auxiliary_llm: Option<Arc<dyn LlmProvider>>,
        fallback_llm: Option<Arc<dyn LlmProvider>>,
    ) -> Self {
        if let Err(e) = config.validate() {
            // 严重配置错误（阈值超出上下文长度）会导致压缩永不触发或立即触发，
            // 必须 panic 而非静默使用，避免在生产环境中产生难以排查的上下文溢出。
            // 其他参数错误（百分比越界等）记录警告并继续，因为有合理的回退行为。
            let ctx_len = config.context_length;
            if ctx_len > 0 && config.threshold_tokens() >= ctx_len {
                panic!("invalid compression config: {}", e);
            }
            tracing::warn!("compression config validation failed: {} (using anyway)", e);
        }
        Self {
            config: Mutex::new(config),
            auxiliary_llm,
            fallback_llm,
            pinned_summary_route: Mutex::new(None),
            state: Mutex::new(CompressionState::new()),
            token_tracker: Mutex::new(TokenTracker::new()),
            stats: Mutex::new(CompressionStats::default()),
        }
    }

    /// 固定摘要路由到 fallback（单次消费）
    ///
    /// 当摘要调用因超时中止后，重试时切换到 fallback_chain 路由，单次消费。
    pub fn pin_summary_route(&self) {
        if let Some(fb) = &self.fallback_llm {
            *self.pinned_summary_route.lock().unwrap() = Some(fb.clone());
            tracing::info!("Summary route pinned to fallback provider");
        }
    }

    /// 获取并清除固定的摘要路由（单次消费）
    fn take_pinned_summary_route(&self) -> Option<Arc<dyn LlmProvider>> {
        self.pinned_summary_route.lock().unwrap().take()
    }

    /// 获取配置快照
    fn config(&self) -> CompressionConfig {
        self.config.lock().unwrap().clone()
    }

    /// 判断是否应主动修剪工具输出（基于 token 压力）
    fn should_proactively_prune(&self, messages: &[ChatMessage]) -> bool {
        let cfg = self.config();
        if cfg.proactive_prune_tokens == 0 {
            return false;
        }
        let tokens = self.current_tokens(messages);
        tokens >= cfg.proactive_prune_tokens
    }

    fn current_tokens(&self, messages: &[ChatMessage]) -> usize {
        self.token_tracker
            .lock()
            .unwrap()
            .current_tokens(messages)
    }

    /// 处理摘要生成成功
    fn on_summary_success(
        &self,
        result: SummaryResult,
        pruned_skill_names: &[String],
        local_stats: &mut CompressionStats,
    ) -> String {
        let mut s = reinject_pruned_skill_markers(&result.text, pruned_skill_names);
        if result.truncated {
            // 截断的摘要不作为 checkpoint 持久化，避免信息永久丢失
            tracing::warn!("LLM summary was truncated (finish_reason=length or over max_tokens); not persisting as previous_summary");
            s.push_str(&format!("\n\n[{}]", TRUNCATED_SUMMARY_MARKER));
        } else {
            self.state.lock().unwrap().mark_success(s.clone());
        }
        local_stats.used_llm_summary = true;
        s
    }

    /// 处理摘要生成失败
    ///
    /// - 标记失败状态（启动冷却）
    /// - 若 `abort_on_summary_failure`，返回 `None`（表示放弃摘要，调用方返回已修剪消息）
    /// - 否则返回规则式 fallback 摘要文本
    fn on_summary_failure(
        &self,
        cfg: &CompressionConfig,
        messages_to_summarize: &[ChatMessage],
        pruned_skill_names: &[String],
        local_stats: &mut CompressionStats,
        messages: &[ChatMessage],
    ) -> Option<String> {
        self.state.lock().unwrap().mark_failure(cfg.cooldown_seconds);
        if cfg.abort_on_summary_failure {
            // 放弃摘要但保留已完成的工具修剪成果
            local_stats.messages_after = messages.len();
            local_stats.tokens_after = estimate_tokens(messages);
            local_stats.used_llm_summary = false;
            *self.stats.lock().unwrap() = local_stats.clone();
            None
        } else {
            let fallback = fallback_summary(messages_to_summarize);
            let reinjected = reinject_pruned_skill_markers(&fallback, pruned_skill_names);
            local_stats.used_llm_summary = false;
            Some(reinjected)
        }
    }

    /// 记录当前消息数（与 record_token_usage 配合使用）
    pub(crate) fn note_message_count(&self, count: usize) {
        self.token_tracker.lock().unwrap().note_message_count(count);
    }

    /// 导出压缩状态快照（用于持久化到 checkpoint）
    pub(crate) fn snapshot_state(&self) -> CompressionStateSnapshot {
        self.state.lock().unwrap().snapshot()
    }

    /// 从快照恢复压缩状态（resume 时调用）
    pub(crate) fn restore_state(&self, snapshot: CompressionStateSnapshot) {
        *self.state.lock().unwrap() = CompressionState::restore(snapshot);
    }
}

#[async_trait]
impl ContextEngine for ContextCompressor {
    fn should_compress(&self, messages: &[ChatMessage]) -> bool {
        if messages.len() < 4 {
            return false;
        }
        let tokens = self.current_tokens(messages);
        let cfg = self.config();
        tokens >= cfg.threshold_tokens()
    }

    fn prune_tool_results_only(&self, mut messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
        let cfg = self.config();
        let _ = prune_tool_outputs(&mut messages, cfg.max_tool_output_chars);
        let _ = dedupe_tool_results(&mut messages);
        let len = messages.len();
        let _ = pressure_demote_tail(&mut messages, len, cfg.max_tool_output_chars);
        messages
    }

    async fn compress(
        &self,
        mut messages: Vec<ChatMessage>,
        memory_context: Option<String>,
    ) -> anyhow::Result<Option<Vec<ChatMessage>>> {
        if messages.is_empty() {
            return Ok(None);
        }

        // 获取一次配置快照，避免整个压缩过程中多次加锁
        let cfg = self.config();

        let mut local_stats = CompressionStats {
            messages_before: messages.len(),
            tokens_before: estimate_tokens(&messages),
            ..Default::default()
        };

        // 阶段 1: 主动工具修剪（token 压力下）
        if self.should_proactively_prune(&messages) {
            let n = prune_tool_outputs(
                &mut messages,
                cfg.proactive_prune_min_result_chars,
            );
            local_stats.tool_outputs_pruned += n;
        }

        // 阶段 2: 基础工具输出修剪（超长截断）
        local_stats.tool_outputs_pruned +=
            prune_tool_outputs(&mut messages, cfg.max_tool_output_chars);

        // 阶段 3: 重复工具输出去重
        local_stats.tool_outputs_deduped += dedupe_tool_results(&mut messages);

        // 阶段 4: 大技能结果修剪为标记
        let (skill_pruned, pruned_skill_names) = prune_large_skill_results(&mut messages);
        local_stats.skill_results_pruned += skill_pruned;

        // 阶段 5: 计算压缩窗口
        let window = compute_compress_window(
            &messages,
            cfg.protect_first_n,
            cfg.tail_mode,
            cfg.tail_token_budget(),
            cfg.protect_last_n,
            cfg.min_tail_user_messages,
        );

        let Some(window) = window else {
            // 无可摘要区域，但已做了工具修剪，返回修剪后的消息
            local_stats.messages_after = messages.len();
            local_stats.tokens_after = estimate_tokens(&messages);
            *self.stats.lock().unwrap() = local_stats;
            return Ok(Some(messages));
        };

        // 阶段 5.5: 压力降级 — token 压力高时，降级尾部保护区域内的大工具输出
        let current_est = estimate_tokens(&messages);
        if current_est >= cfg.threshold_tokens() {
            let demoted = pressure_demote_tail(
                &mut messages,
                window.compress_end,
                cfg.max_tool_output_chars,
            );
            local_stats.tail_outputs_demoted += demoted;
        }

        // 阶段 6: 生成摘要
        let messages_to_summarize = &messages[window.compress_start..window.compress_end];
        if messages_to_summarize.is_empty() {
            local_stats.messages_after = messages.len();
            local_stats.tokens_after = estimate_tokens(&messages);
            *self.stats.lock().unwrap() = local_stats;
            return Ok(Some(messages));
        }

        // 先提取状态决策，避免持有 MutexGuard 跨越 await
        let (should_skip, prev_summary) = {
            let state = self.state.lock().unwrap();
            (state.should_skip_llm_summary(), state.previous_summary.clone())
        };

        // 摘要路由固定：优先消费被固定的 fallback 路由（单次）
        let pinned_route = self.take_pinned_summary_route();
        let primary_provider = pinned_route
            .or_else(|| self.auxiliary_llm.clone())
            .filter(|_| !should_skip);

        // 生成摘要：返回 Option<String>，None 表示放弃摘要（abort_on_summary_failure）
        let summary_opt: Option<String> = match primary_provider {
            None => {
                // 冷却中 / terminal failure / 无辅助 LLM → fallback
                let fallback = fallback_summary(messages_to_summarize);
                let reinjected = reinject_pruned_skill_markers(&fallback, &pruned_skill_names);
                local_stats.used_llm_summary = false;
                Some(reinjected)
            }
            Some(provider) => {
                match generate_summary(
                    &provider,
                    messages_to_summarize,
                    prev_summary.as_deref(),
                    memory_context.as_deref(),
                    cfg.max_summary_tokens(),
                    cfg.summary_input_max_chars(),
                )
                .await
                {
                    Ok(result) => {
                        Some(self.on_summary_success(result, &pruned_skill_names, &mut local_stats))
                    }
                    Err(e) => {
                        tracing::warn!("LLM summary generation failed: {:?}", e);
                        // 主路由失败：若有 fallback provider，尝试一次 fallback 重试
                        if let Some(fb) = self.fallback_llm.clone() {
                            tracing::info!("Retrying summary with fallback provider");
                            match generate_summary(
                                &fb,
                                messages_to_summarize,
                                prev_summary.as_deref(),
                                memory_context.as_deref(),
                                cfg.max_summary_tokens(),
                                cfg.summary_input_max_chars(),
                            )
                            .await
                            {
                                Ok(result) => Some(self.on_summary_success(
                                    result,
                                    &pruned_skill_names,
                                    &mut local_stats,
                                )),
                                Err(e2) => {
                                    tracing::warn!("Fallback summary also failed: {:?}", e2);
                                    self.on_summary_failure(
                                        &cfg,
                                        messages_to_summarize,
                                        &pruned_skill_names,
                                        &mut local_stats,
                                        &messages,
                                    )
                                }
                            }
                        } else {
                            self.on_summary_failure(
                                &cfg,
                                messages_to_summarize,
                                &pruned_skill_names,
                                &mut local_stats,
                                &messages,
                            )
                        }
                    }
                }
            }
        };

        let Some(summary) = summary_opt else {
            // abort_on_summary_failure：放弃摘要，返回已修剪的消息
            return Ok(Some(messages));
        };

        // 阶段 7: 组装最终消息
        let head = &messages[..window.compress_start];
        let tail = &messages[window.compress_end..];

        let summary_msg = ChatMessage::user(assemble_summary_message(&summary));

        let mut compressed: Vec<ChatMessage> = Vec::with_capacity(head.len() + 1 + tail.len());
        compressed.extend_from_slice(head);
        compressed.push(summary_msg);
        compressed.extend_from_slice(tail);

        // 边界处理：若压缩后无 user 消息，注入 synthetic user 保持对话格式合法
        let has_user_after = compressed
            .iter()
            .any(|m| m.role == Role::User);
        if !has_user_after {
            compressed.push(ChatMessage::user(COMPRESSION_CONTINUATION_USER_CONTENT));
        }

        local_stats.messages_after = compressed.len();
        local_stats.tokens_after = estimate_tokens(&compressed);
        *self.stats.lock().unwrap() = local_stats;

        // 重置 token 缓存（消息结构已变）
        self.token_tracker.lock().unwrap().reset_cache();

        Ok(Some(compressed))
    }

    fn record_token_usage(&self, prompt_tokens: usize, completion_tokens: usize) {
        self.token_tracker
            .lock()
            .unwrap()
            .update_from_response(prompt_tokens, completion_tokens);
    }

    fn stats(&self) -> CompressionStats {
        self.stats.lock().unwrap().clone()
    }

    fn reset(&self) {
        self.state.lock().unwrap().reset();
        *self.token_tracker.lock().unwrap() = TokenTracker::new();
        *self.stats.lock().unwrap() = CompressionStats::default();
    }

    fn should_compress_preflight(&self, messages: &[ChatMessage]) -> CompressPreflight {
        let should = self.should_compress(messages);
        let reason = if should {
            let cfg = self.config();
            let tokens = self.current_tokens(messages);
            format!(
                "token threshold exceeded ({} >= {})",
                tokens,
                cfg.threshold_tokens()
            )
        } else {
            "within budget".to_string()
        };
        CompressPreflight {
            should_compress: should,
            reason,
        }
    }

    fn get_status(&self, messages: &[ChatMessage]) -> EngineStatus {
        let cfg = self.config();
        let last_prompt_tokens = self.current_tokens(messages);
        let threshold = cfg.threshold_tokens();
        let usage_percent = if threshold > 0 {
            (last_prompt_tokens as f64 / threshold as f64) * 100.0
        } else {
            0.0
        };
        EngineStatus {
            last_prompt_tokens,
            threshold_tokens: threshold,
            usage_percent,
            approaching_limit: usage_percent >= 80.0,
        }
    }

    fn update_model(&self, _model: &str, context_length: Option<usize>) {
        if let Some(cl) = context_length {
            let mut cfg = self.config.lock().unwrap();
            if cl > 0 {
                cfg.context_length = cl;
                tracing::info!("ContextEngine model updated: context_length={}", cl);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_llm::Role;

    fn make_msgs(n: usize) -> Vec<ChatMessage> {
        let mut msgs = vec![
            ChatMessage::system("system".to_string()),
            ChatMessage::user("goal".to_string()),
        ];
        for i in 0..n {
            msgs.push(ChatMessage::assistant(format!("response number {i} with some content")));
            msgs.push(ChatMessage::tool(
                format!("tc_{i}"),
                format!("tool result output for call {i} with detailed information {}", "y".repeat(200)),
            ));
            if i % 5 == 4 {
                msgs.push(ChatMessage::user(format!("follow up question {i}")));
            }
        }
        msgs
    }

    #[tokio::test]
    async fn test_compress_no_llm_fallback() {
        let config = CompressionConfig {
            context_length: 8000,
            threshold_percent: 0.1,
            ..Default::default()
        };
        let compressor = ContextCompressor::new(config, None);
        let msgs = make_msgs(30);
        let result = compressor.compress(msgs, None).await.unwrap();
        assert!(result.is_some());
        let compressed = result.unwrap();
        // 压缩后消息数应减少
        assert!(compressed.len() < 30 * 2 + 2);
        // 应包含摘要消息（带 SUMMARY_PREFIX）
        let has_summary = compressed
            .iter()
            .any(|m| m.content.as_ref().map(|c| c.starts_with("[CONTEXT COMPACTION")).unwrap_or(false));
        assert!(has_summary);
    }

    #[tokio::test]
    async fn test_should_compress_threshold() {
        let config = CompressionConfig {
            context_length: 8000,
            threshold_percent: 0.5,
            ..Default::default()
        };
        let compressor = ContextCompressor::new(config, None);
        // 短消息不应触发
        let short = vec![ChatMessage::user("hi")];
        assert!(!compressor.should_compress(&short));
    }

    #[tokio::test]
    async fn test_compress_keeps_head_and_tail() {
        let config = CompressionConfig {
            context_length: 8000,
            threshold_percent: 0.05,
            protect_first_n: 2,
            ..Default::default()
        };
        let compressor = ContextCompressor::new(config, None);
        let msgs = make_msgs(40);
        let original_last = msgs.last().unwrap().clone();
        let result = compressor.compress(msgs.clone(), None).await.unwrap().unwrap();
        // 头部 system 和 user 应保留
        assert_eq!(result[0].role, Role::System);
        assert_eq!(result[1].role, Role::User);
        // 尾部最后一条应保留（无论是什么角色）
        assert_eq!(result.last().unwrap().role, original_last.role);
    }
}