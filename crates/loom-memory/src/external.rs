//! External Memory Provider — 外部记忆后端示例
//!
//! 可插拔记忆架构（mem0 / honcho 等外部后端），
//! 提供 `ExternalMemoryProvider` 作为接入外部记忆服务的参考实现。
//!
//! 设计要点：
//! - 通过 `ExternalMemoryTransport` trait 抽象 HTTP/gRPC 通信层，用户可注入自定义实现
//! - 内置 `HttpTransport` 需要启用 `http` feature 后使用 reqwest
//! - 实现 `MemoryProvider` 的关键生命周期钩子，将操作转发到外部服务
//! - 故障隔离：任何外部服务错误仅记录日志，不阻塞主流程

use async_trait::async_trait;
use loom_core::MemoryScope;
use loom_llm::ChatMessage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::provider::{MemoryProvider, ProviderToolSchema, RecallStatus};

/// 外部记忆服务配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalMemoryConfig {
    /// 服务端点 URL（如 https://api.mem0.ai/v1）
    pub endpoint: String,
    /// API 密钥（通过 Authorization header 传递）
    #[serde(default)]
    pub api_key: Option<String>,
    /// 单次请求超时（毫秒），0 表示不设超时
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// 召回结果最大条目数
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    /// 召回结果最小相似度阈值（0.0 - 1.0）
    #[serde(default = "default_min_score")]
    pub min_score: f32,
}

impl Default for ExternalMemoryConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            api_key: None,
            timeout_ms: default_timeout_ms(),
            max_results: default_max_results(),
            min_score: default_min_score(),
        }
    }
}

fn default_timeout_ms() -> u64 {
    3000
}
fn default_max_results() -> usize {
    10
}
fn default_min_score() -> f32 {
    0.3
}

/// 外部记忆服务的传输层抽象
///
/// 用户可实现此 trait 接入任意 HTTP/gRPC 客户端。
/// 所有方法返回 `anyhow::Result`，错误由 provider 记录但不阻塞主流程。
#[async_trait]
pub trait ExternalMemoryTransport: Send + Sync {
    /// 向量召回：根据 query 检索相关记忆
    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &str,
        session_id: &str,
        max_results: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<RecalledMemory>>;

    /// 同步一轮对话到外部记忆
    async fn sync_turn(
        &self,
        scope: &MemoryScope,
        user_content: &str,
        assistant_content: &str,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> anyhow::Result<()>;

    /// 镜像写入操作（当内置 memory 工具写入后调用）
    async fn mirror_write(
        &self,
        action: &str,
        target: Option<&str>,
        content: Option<&str>,
        metadata: Value,
    ) -> anyhow::Result<()>;

    /// 会话结束时通知外部服务
    async fn session_end(&self, scope: &MemoryScope, messages: &[ChatMessage]) -> anyhow::Result<()>;

    /// 会话切换时通知外部服务
    async fn session_switch(
        &self,
        new_session_id: &str,
        parent_session_id: Option<&str>,
        reset: bool,
        rewound: bool,
    ) -> anyhow::Result<()>;

    /// 任务委派时通知外部服务
    async fn delegation(
        &self,
        task: &str,
        result: &str,
        child_session_id: &str,
    ) -> anyhow::Result<()>;
}

/// 召回的记忆条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecalledMemory {
    /// 记忆内容
    pub content: String,
    /// 相似度分数（0.0 - 1.0）
    pub score: f32,
    /// 记忆来源标签（如 "mem0"、"honcho"）
    pub source: String,
}

/// 外部记忆 Provider
///
/// 包裹 `ExternalMemoryTransport`，实现 `MemoryProvider` trait。
/// 将生命周期钩子转发到外部记忆服务，并将召回结果格式化为系统提示词块。
pub struct ExternalMemoryProvider {
    config: ExternalMemoryConfig,
    transport: Arc<dyn ExternalMemoryTransport>,
    /// provider 显示名
    label: String,
}

impl ExternalMemoryProvider {
    /// 创建外部记忆 Provider
    ///
    /// - `config`：服务配置
    /// - `transport`：传输层实现
    /// - `label`：provider 显示名（如 "mem0"、"honcho"）
    pub fn new(
        config: ExternalMemoryConfig,
        transport: Arc<dyn ExternalMemoryTransport>,
        label: impl Into<String>,
    ) -> Self {
        Self {
            config,
            transport,
            label: label.into(),
        }
    }

    /// 服务是否配置就绪
    pub fn is_configured(&self) -> bool {
        !self.config.endpoint.is_empty()
    }

    fn format_recall(&self, memories: &[RecalledMemory]) -> String {
        if memories.is_empty() {
            return String::new();
        }
        let mut block = String::from("## Relevant Memory\n");
        for m in memories {
            block.push_str(&format!("- {} (score={:.2}, source={})\n", m.content, m.score, m.source));
        }
        block
    }
}

#[async_trait]
impl MemoryProvider for ExternalMemoryProvider {
    fn name(&self) -> &str {
        &self.label
    }

    async fn is_available(&self) -> bool {
        self.is_configured()
    }

    async fn prefetch(
        &self,
        scope: &MemoryScope,
        query: &str,
        session_id: &str,
    ) -> anyhow::Result<String> {
        if !self.is_configured() {
            return Ok(String::new());
        }
        match self
            .transport
            .recall(scope, query, session_id, self.config.max_results, self.config.min_score)
            .await
        {
            Ok(memories) => Ok(self.format_recall(&memories)),
            Err(e) => {
                tracing::warn!("[external-memory] recall failed: {}", e);
                Ok(String::new())
            }
        }
    }

    async fn sync_turn(
        &self,
        scope: &MemoryScope,
        user_content: &str,
        assistant_content: &str,
        session_id: &str,
        messages: &[ChatMessage],
    ) -> anyhow::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if let Err(e) = self
            .transport
            .sync_turn(scope, user_content, assistant_content, session_id, messages)
            .await
        {
            tracing::warn!("[external-memory] sync_turn failed: {}", e);
        }
        Ok(())
    }

    async fn on_memory_write(
        &self,
        action: &str,
        target: Option<&str>,
        content: Option<&str>,
        metadata: Value,
    ) -> anyhow::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if let Err(e) = self.transport.mirror_write(action, target, content, metadata).await {
            tracing::warn!("[external-memory] mirror_write failed: {}", e);
        }
        Ok(())
    }

    async fn on_pre_compress(&self, _scope: &MemoryScope, _messages: &[ChatMessage]) -> anyhow::Result<String> {
        Ok(String::new())
    }

    async fn on_session_end(&self, scope: &MemoryScope, messages: &[ChatMessage]) -> anyhow::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if let Err(e) = self.transport.session_end(scope, messages).await {
            tracing::warn!("[external-memory] session_end failed: {}", e);
        }
        Ok(())
    }

    async fn on_session_switch(
        &self,
        new_session_id: &str,
        parent_session_id: Option<&str>,
        reset: bool,
        rewound: bool,
    ) -> anyhow::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if let Err(e) = self
            .transport
            .session_switch(new_session_id, parent_session_id, reset, rewound)
            .await
        {
            tracing::warn!("[external-memory] session_switch failed: {}", e);
        }
        Ok(())
    }

    async fn on_delegation(
        &self,
        task: &str,
        result: &str,
        child_session_id: &str,
    ) -> anyhow::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if let Err(e) = self.transport.delegation(task, result, child_session_id).await {
            tracing::warn!("[external-memory] delegation failed: {}", e);
        }
        Ok(())
    }

    async fn recall_status(&self) -> anyhow::Result<Option<RecallStatus>> {
        if !self.is_configured() {
            return Ok(None);
        }
        Ok(Some(RecallStatus {
            provider_label: self.label.clone(),
            glyph: "🧠".to_string(),
            count: -1,
        }))
    }

    fn get_tool_schemas(&self) -> Vec<ProviderToolSchema> {
        Vec::new()
    }
}