use loom_core::{CapabilityKind, CapabilityRegistry};
use loom_llm::ToolDefinition;

use crate::config::AgentLoopConfig;
use super::defs::{interrupt_tool_def, spawn_agent_tool_def};
use super::dispatch::capability_to_tool_def;

/// 工具集解析器
///
/// 职责（delegate_tool_toolsets）：
/// 1. 若 `toolsets` 非空，只返回 tags 匹配的工具
/// 2. 子 Agent（delegate_depth > 0）的工具集必须与父 Agent 的 `parent_toolsets` 做交集
///    —— "Children never gain tools the parent lacks"
/// 3. 子 Agent 额外排除阻塞工具（memory、ask_user、show_message、cronjob_manage）
///
/// 始终包含 `spawn_agent`（若允许委派且深度未超限）和 `interrupt`（系统级工具）。
pub struct ToolResolver<'a> {
    registry: &'a dyn CapabilityRegistry,
    config: &'a AgentLoopConfig,
}

impl<'a> ToolResolver<'a> {
    pub fn new(registry: &'a dyn CapabilityRegistry, config: &'a AgentLoopConfig) -> Self {
        Self { registry, config }
    }

    pub async fn available_tools(
        &self,
        toolsets: &[String],
        parent_toolsets: &[String],
        delegate_depth: u32,
    ) -> Vec<ToolDefinition> {
        // 子 Agent 工具集交集：只能使用父 Agent 也拥有的工具集
        let resolved_toolsets = if delegate_depth > 0 && !parent_toolsets.is_empty() {
            // 父 Agent 有工具集限制 → 子 Agent 请求的 toolsets 必须是父 toolsets 的子集
            if toolsets.is_empty() {
                // 子 Agent 未指定 → 继承父 Agent 的全部工具集
                parent_toolsets.to_vec()
            } else {
                // 子 Agent 指定了 → 取交集
                toolsets
                    .iter()
                    .filter(|t| parent_toolsets.contains(t))
                    .cloned()
                    .collect::<Vec<_>>()
            }
        } else {
            toolsets.to_vec()
        };

        let specs = if resolved_toolsets.is_empty() {
            self.registry
                .list_by_kind(CapabilityKind::Tool)
                .await
                .unwrap_or_default()
        } else {
            self.registry
                .list_by_tags(&resolved_toolsets)
                .await
                .unwrap_or_default()
        };

        // 子 Agent 阻塞工具列表（DELEGATE_BLOCKED_TOOLS）
        // - memory*: 子 Agent 不应读写父会话记忆
        // - ask_user/show_message: 子 Agent 不应触发用户交互
        // - cronjob_manage: 子 Agent 不应以父名义调度工作
        let blocked_for_child = |name: &str| -> bool {
            if delegate_depth == 0 {
                return false;
            }
            name.starts_with("memory")
                || name == "memory"
                || name == "ask_user"
                || name == "show_message"
                || name == "cronjob_manage"
        };

        let mut tools: Vec<ToolDefinition> = specs
            .iter()
            .filter(|spec| !blocked_for_child(&spec.name))
            .map(capability_to_tool_def)
            .collect();

        // 子 Agent 深度未超限时才允许继续委派
        let can_delegate = self.config.allow_delegation
            && delegate_depth < self.config.max_delegation_depth;
        if can_delegate {
            tools.push(spawn_agent_tool_def());
        }

        // interrupt 工具用于 human-in-the-loop
        tools.push(interrupt_tool_def());

        tools
    }
}