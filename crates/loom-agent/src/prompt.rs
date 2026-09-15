use chrono::Local;
use loom_llm::ToolDefinition;
use std::path::Path;

use crate::config::{AgentLoopConfig, ContextFilesConfig, PromptConfig};

/// 提示词构建结果，按三层缓存架构组织
///
/// system_prompt 的 stable/context/volatile 分层：
/// - stable: 整个会话不变，用于 LLM 前缀缓存
/// - context: 随工作目录/请求变化
/// - volatile: 每轮可能变化（tools、timestamp）
#[derive(Debug, Clone)]
pub struct PromptParts {
    pub stable: String,
    pub context: String,
    pub volatile: String,
}

impl PromptParts {
    pub fn joined(&self) -> String {
        let mut parts = Vec::new();
        if !self.stable.is_empty() {
            parts.push(self.stable.as_str());
        }
        if !self.context.is_empty() {
            parts.push(self.context.as_str());
        }
        if !self.volatile.is_empty() {
            parts.push(self.volatile.as_str());
        }
        parts.join("\n\n")
    }
}

/// 构建完整的系统提示词（分层架构）
///
/// 如果配置了结构化的 prompt 分段（identity/guidance/tool_rules），
/// 则使用分层构建；否则回退到传统的单模板 system_prompt。
pub fn build_system_prompt(
    config: &AgentLoopConfig,
    goal: &str,
    context: &str,
    tools: &[ToolDefinition],
    workspace_dir: Option<&Path>,
) -> PromptParts {
    if config.prompt.identity.is_empty()
        && config.prompt.guidance.is_empty()
        && config.prompt.tool_rules.is_empty()
    {
        // 回退到传统单模板
        let full = build_legacy_prompt(&config.system_prompt, goal, context, tools);
        return PromptParts {
            stable: full,
            context: String::new(),
            volatile: build_volatile(tools),
        };
    }

    // stable tier: identity + guidance + tool_rules（会话内不变）
    let stable = build_stable(&config.prompt);

    // context tier: goal + 请求上下文 + 外部上下文文件
    let context_files = if config.prompt.context_files.enabled {
        load_context_files(&config.prompt.context_files, workspace_dir)
    } else {
        String::new()
    };
    let context = build_context(goal, context, &context_files);

    // volatile tier: 可用工具列表（可能随注册变化）
    let volatile = build_volatile(tools);

    PromptParts {
        stable,
        context,
        volatile,
    }
}

/// 构建 stable tier（身份 + 指导 + 工具规则）
fn build_stable(prompt: &PromptConfig) -> String {
    let mut parts = Vec::new();
    if !prompt.identity.is_empty() {
        parts.push(prompt.identity.as_str());
    }
    if !prompt.guidance.is_empty() {
        parts.push(prompt.guidance.as_str());
    }
    if !prompt.tool_rules.is_empty() {
        parts.push(prompt.tool_rules.as_str());
    }
    parts.join("\n\n")
}

/// 构建 context tier（目标 + 请求上下文 + 外部文件）
fn build_context(goal: &str, request_context: &str, context_files: &str) -> String {
    let mut parts = Vec::new();

    parts.push(format!("## Goal\n{goal}"));

    if !request_context.is_empty() {
        parts.push(format!("## Context\n{request_context}"));
    }

    if !context_files.is_empty() {
        parts.push(format!("## Workspace Context\n{context_files}"));
    }

    parts.join("\n\n")
}

/// 构建 volatile tier（当前日期时间 + 工具列表）
///
/// 日期精确到秒，让 LLM 知道当前具体时间。
/// 时区偏移让 LLM 和工具知道当前所在时区。
fn build_volatile(tools: &[ToolDefinition]) -> String {
    let now = Local::now();
    let weekday = now.format("%A").to_string();
    let date = now.format("%B %d, %Y %H:%M:%S").to_string();
    let offset = now.format("%z").to_string();
    let offset_display = if offset.len() >= 5 {
        format!("UTC{}:{}", &offset[..3], &offset[3..5])
    } else {
        format!("UTC{offset}")
    };
    let date_line = format!("Today's date: {weekday}, {date} ({offset_display})");

    let tools_section = if tools.is_empty() {
        "## Available Tools\n(no tools available)".to_string()
    } else {
        let tools_desc = tools
            .iter()
            .map(|t| format!("- **{}**: {}", t.name, t.description))
            .collect::<Vec<_>>()
            .join("\n");
        format!("## Available Tools\n{tools_desc}")
    };

    format!("{date_line}\n\n{tools_section}")
}

/// 从工作目录向上查找并加载上下文文件
///
/// AGENTS.md 逐层合并逻辑：
/// - 从当前工作目录开始，向上查找
/// - 找到第一个匹配的文件即返回（优先使用更近的目录）
fn load_context_files(config: &ContextFilesConfig, workspace_dir: Option<&Path>) -> String {
    let dir = match workspace_dir {
        Some(d) => d.to_path_buf(),
        None => match std::env::current_dir() {
            Ok(d) => d,
            Err(_) => return String::new(),
        },
    };

    let mut loaded = Vec::new();

    // 查找 git root 作为停止点（避免无限向上查找）
    let git_root = find_git_root(&dir);

    let mut current = dir.as_path();
    loop {
        for name in &config.names {
            let path = current.join(name);
            if path.is_file() {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    let truncated = if content.chars().count() > config.max_chars {
                        content.chars().take(config.max_chars).collect::<String>()
                            + "\n... [truncated]"
                    } else {
                        content
                    };
                    loaded.push(format!("### {}\n{}", name, truncated.trim()));
                }
            }
        }

        // 如果已到达 git root，停止向上查找
        if let Some(root) = &git_root {
            if current == root.as_path() {
                break;
            }
        }

        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }

    loaded.join("\n\n")
}

/// 查找包含 .git 的最近祖先目录
fn find_git_root(start: &Path) -> Option<std::path::PathBuf> {
    let mut current = start;
    loop {
        if current.join(".git").exists() {
            return Some(current.to_path_buf());
        }
        current = current.parent()?;
    }
}

/// 传统单模板提示词构建（向后兼容）
fn build_legacy_prompt(
    template: &str,
    goal: &str,
    context: &str,
    tools: &[ToolDefinition],
) -> String {
    let tools_desc = if tools.is_empty() {
        "(no tools available)".to_string()
    } else {
        tools
            .iter()
            .map(|t| format!("- {}: {}", t.name, t.description))
            .collect::<Vec<_>>()
            .join("\n")
    };

    template
        .replace("{goal}", goal)
        .replace(
            "{context}",
            if context.is_empty() {
                "(none)"
            } else {
                context
            },
        )
        .replace("{tools}", &tools_desc)
}