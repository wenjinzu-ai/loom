//! MemoryProvider trait — 记忆提供者抽象
//!
//! 定义记忆子系统的完整生命周期钩子：
//! - 显式记忆（curated）：`system_prompt_block`
//! - 隐式召回（recall）：`prefetch` / `queue_prefetch` / `recall_status`
//! - 持久化：`sync_turn`
//! - 工具：`get_tool_schemas` / `handle_tool_call` / `on_memory_write`
//! - 会话边界：`on_pre_compress` / `on_session_end` / `on_session_switch` / `on_delegation`
//! - 生命周期：`is_available` / `initialize` / `shutdown` / `on_turn_start`

use async_trait::async_trait;
use loom_core::MemoryScope;
use loom_llm::ChatMessage;
use serde_json::Value;

/// 召回状态（用于 UI 指示器）
#[derive(Debug, Clone)]
pub struct RecallStatus {
    /// provider 显示名
    pub provider_label: String,
    /// 图标（emoji）
    pub glyph: String,
    /// 召回条目数（<=0 表示有内容但无数离散计数）
    pub count: i32,
}

/// 工具 schema（provider 可暴露专属工具）
#[derive(Debug, Clone)]
pub struct ProviderToolSchema {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// 记忆提供者 trait
///
/// 所有方法都有默认 no-op 实现，provider 只需覆盖关心的钩子。
/// 失败不应 panic，由 MemoryManager 捕获并记录。
#[async_trait]
pub trait MemoryProvider: Send + Sync {
    /// provider 名称（"builtin" 保留给内置）
    fn name(&self) -> &str;

    /// 是否为外部 provider（非 builtin）
    ///
    /// 用于 MemoryManager 限制外部 provider 数量、路由工具写入镜像等逻辑。
    /// 默认返回 true（外部），builtin provider 覆盖为 false。
    fn is_external(&self) -> bool {
        self.name() != "builtin"
    }

    /// 配置/凭证是否就绪
    async fn is_available(&self) -> bool {
        true
    }

    /// 会话启动时初始化
    async fn initialize(&self, _scope: &MemoryScope, _session_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// 静态系统提示词块（如 builtin 的 MEMORY.md/USER.md 渲染）
    /// 返回 None 或空字符串表示无内容
    async fn system_prompt_block(&self, _scope: &MemoryScope, _session_id: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    /// 每轮前召回相关上下文（必须快，用缓存）
    async fn prefetch(&self, _scope: &MemoryScope, _query: &str, _session_id: &str) -> anyhow::Result<String> {
        Ok(String::new())
    }

    /// 后台异步召回，下轮消费
    async fn queue_prefetch(&self, _scope: &MemoryScope, _query: &str, _session_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// 召回状态（用于 UI 指示器）
    async fn recall_status(&self) -> anyhow::Result<Option<RecallStatus>> {
        Ok(None)
    }

    /// 每轮开始时通知 provider
    ///
    /// provider 可感知当前轮次的运行时状态。
    async fn on_turn_start(
        &self,
        _turn_number: usize,
        _message: &str,
        _session_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 每轮后持久化对话
    async fn sync_turn(
        &self,
        _scope: &MemoryScope,
        _user_content: &str,
        _assistant_content: &str,
        _session_id: &str,
        _messages: &[ChatMessage],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// provider 专属工具 schema
    fn get_tool_schemas(&self) -> Vec<ProviderToolSchema> {
        Vec::new()
    }

    /// 处理 provider 专属工具调用
    async fn handle_tool_call(&self, _name: &str, _args: Value, _scope: &MemoryScope, _session_id: &str) -> anyhow::Result<Value> {
        anyhow::bail!("tool not handled by this provider")
    }

    /// 内置 memory 工具写入后镜像通知外部 provider
    ///
    /// 当内置 `memory` 工具成功写入（add/replace/remove）后，
    /// 将写入操作镜像通知给所有外部 provider，保持两套记忆数据一致。
    ///
    /// # 参数
    /// - `action`：写入动作（"add" / "replace" / "remove"）
    /// - `target`：操作目标（如记忆条目 ID）
    /// - `content`：写入内容
    /// - `metadata`：附加元数据
    async fn on_memory_write(
        &self,
        _action: &str,
        _target: Option<&str>,
        _content: Option<&str>,
        _metadata: Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 压缩前提取洞察（返回将加入 summary 的文本）
    async fn on_pre_compress(&self, _scope: &MemoryScope, _messages: &[ChatMessage]) -> anyhow::Result<String> {
        Ok(String::new())
    }

    /// 会话结束
    async fn on_session_end(&self, _scope: &MemoryScope, _messages: &[ChatMessage]) -> anyhow::Result<()> {
        Ok(())
    }

    /// 会话切换
    ///
    /// - `parent_session_id`：父会话 ID（委派子会话场景）
    /// - `reset`：是否全新会话（/reset）
    /// - `rewound`：同 ID 但 transcript 被截断
    async fn on_session_switch(
        &self,
        _new_session_id: &str,
        _parent_session_id: Option<&str>,
        _reset: bool,
        _rewound: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 任务委派
    ///
    /// 外部 provider 可记录委派任务的上下文。
    async fn on_delegation(
        &self,
        _task: &str,
        _result: &str,
        _child_session_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 清理资源
    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}