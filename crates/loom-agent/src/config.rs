use serde::{Deserialize, Serialize};

/// Agent 循环配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLoopConfig {
    /// 模型名称
    pub model: String,
    /// 系统提示词模板（{goal} {context} {tools} 会被替换）
    /// 若提供了 prompt 分段配置，则忽略此字段
    #[serde(default)]
    pub system_prompt: String,
    /// 最大迭代次数（防止死循环）
    pub max_iterations: usize,
    /// 是否允许委派子 Agent
    pub allow_delegation: bool,
    /// 最大委派深度（防止无限递归）
    pub max_delegation_depth: u32,
    /// 最大并发子 Agent 数
    pub max_concurrent_children: usize,
    /// 结构化提示词配置（stable/context/volatile 分层 + 外部文件加载）
    #[serde(default)]
    pub prompt: PromptConfig,
    /// 上下文压缩配置
    #[serde(default)]
    pub compression: CompressionConfig,
    /// checkpoint 保存间隔（每 N 轮保存一次 step checkpoint，0 表示不保存 step checkpoint）
    /// interrupt checkpoint 不受此限制，始终保存
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: usize,
    /// 单轮中并发执行的最大工具数（防止 join_all 无限制并发耗尽资源）
    #[serde(default = "default_max_concurrent_tools")]
    pub max_concurrent_tools: usize,
    /// API 重试配置（指数退避重试）
    #[serde(default)]
    pub api_retry: ApiRetryConfig,
    /// 工具调用防护配置（去重、循环检测、失败分类）
    #[serde(default)]
    pub guardrails: GuardrailConfig,
    /// 活性看门狗配置（liveness_watchdog）
    #[serde(default)]
    pub liveness: LivenessConfig,
    /// 委派摘要最大字符数（子 Agent 结果返回父 Agent 前截断）
    /// 防止子 Agent 大结果撑爆父上下文
    #[serde(default = "default_delegate_summary_max_chars")]
    pub delegate_summary_max_chars: usize,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            model: "gpt-4o-mini".to_string(),
            system_prompt: DEFAULT_SYSTEM_PROMPT.to_string(),
            max_iterations: 20,
            allow_delegation: true,
            max_delegation_depth: 3,
            max_concurrent_children: 5,
            prompt: PromptConfig::default(),
            compression: CompressionConfig::default(),
            checkpoint_interval: default_checkpoint_interval(),
            max_concurrent_tools: default_max_concurrent_tools(),
            api_retry: ApiRetryConfig::default(),
            guardrails: GuardrailConfig::default(),
            liveness: LivenessConfig::default(),
            delegate_summary_max_chars: default_delegate_summary_max_chars(),
        }
    }
}

/// 活性看门狗配置（liveness_watchdog）
///
/// 用于检测 agent 循环是否卡死（长时间无活动）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessConfig {
    /// 是否启用活性看门狗
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 活动超时（秒），超过此时间无活动则视为卡死
    #[serde(default = "default_liveness_timeout_secs")]
    pub timeout_secs: u64,
    /// 看门狗检查间隔（秒）
    #[serde(default = "default_liveness_check_interval_secs")]
    pub check_interval_secs: u64,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            timeout_secs: default_liveness_timeout_secs(),
            check_interval_secs: default_liveness_check_interval_secs(),
        }
    }
}

fn default_liveness_timeout_secs() -> u64 {
    300
}
fn default_liveness_check_interval_secs() -> u64 {
    30
}

/// API 重试配置（turn_api_error 的指数退避）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiRetryConfig {
    /// 最大重试次数（不含首次请求）
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// 初始退避延迟（毫秒）
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
    /// 最大退避延迟（毫秒）
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
    /// 单次请求超时（秒），0 表示不设超时
    #[serde(default)]
    pub request_timeout_secs: u64,
}

impl Default for ApiRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            base_delay_ms: default_base_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
            request_timeout_secs: 0,
        }
    }
}

fn default_max_retries() -> u32 {
    5
}
fn default_base_delay_ms() -> u64 {
    1000
}
fn default_max_delay_ms() -> u64 {
    30_000
}

/// 工具调用防护配置（tool_guardrails）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardrailConfig {
    /// 同一 (tool_name, args_hash) 在滑动窗口内最多重复次数
    #[serde(default = "default_max_duplicate_calls")]
    pub max_duplicate_calls: u32,
    /// 同一工具连续失败次数上限（超过则 guardrail halt）
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
    /// 循环检测滑动窗口大小（最近 N 次工具调用）
    #[serde(default = "default_loop_window")]
    pub loop_window: usize,
    /// 是否启用工具调用去重
    #[serde(default = "default_true")]
    pub deduplicate: bool,
    /// 单轮 web_search 调用上限（_LOOP_CAPS max_web_searches）
    #[serde(default = "default_max_web_searches")]
    pub max_web_searches: u32,
    /// 单轮 spawn_agent 调用上限（_LOOP_CAPS max_subagents）
    #[serde(default = "default_max_subagents")]
    pub max_subagents: u32,
}

impl Default for GuardrailConfig {
    fn default() -> Self {
        Self {
            max_duplicate_calls: default_max_duplicate_calls(),
            max_consecutive_failures: default_max_consecutive_failures(),
            loop_window: default_loop_window(),
            deduplicate: true,
            max_web_searches: default_max_web_searches(),
            max_subagents: default_max_subagents(),
        }
    }
}

fn default_max_duplicate_calls() -> u32 {
    3
}
fn default_max_consecutive_failures() -> u32 {
    5
}
fn default_loop_window() -> usize {
    20
}
fn default_max_web_searches() -> u32 {
    50
}
fn default_max_subagents() -> u32 {
    50
}

/// 上下文压缩配置
///
/// 注意：此结构体保留旧字段以兼容配置文件。
/// AgentLoop 内部会将其转换为 `crate::context::CompressionConfig`（动态预算）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// 触发压缩的 token 阈值
    #[serde(default = "default_threshold_tokens")]
    pub threshold_tokens: usize,
    /// 保留的尾部轮数（最近 N 个 user/assistant 交换）
    #[serde(default = "default_tail_turns")]
    pub tail_turns: usize,
    /// 单条工具输出最大字符数（超出截断）
    #[serde(default = "default_max_tool_output_chars")]
    pub max_tool_output_chars: usize,
    /// 摘要目标字符数
    #[serde(default = "default_summary_target_chars")]
    pub summary_target_chars: usize,
}

impl CompressionConfig {
    /// 转换为动态预算配置
    ///
    /// `context_length` 为模型上下文窗口大小，用于计算动态阈值和尾部预算。
    pub fn into_context_config(self, context_length: usize) -> crate::context::CompressionConfig {
        crate::context::CompressionConfig {
            threshold_percent: if context_length > 0 {
                (self.threshold_tokens as f32 / context_length as f32).clamp(0.1, 0.9)
            } else {
                0.50
            },
            protect_first_n: 3,
            protect_last_n: self.tail_turns * 2,
            summary_target_ratio: if context_length > 0 {
                (self.summary_target_chars as f32 / 4.0 / context_length as f32).clamp(0.05, 0.4)
            } else {
                0.20
            },
            tail_mode: crate::context::TailMode::Lean,
            lean_tail_floor: 500,
            lean_tail_cap: 4000,
            context_length,
            fallback_threshold_tokens: Some(self.threshold_tokens),
            cooldown_seconds: 600,
            max_tool_output_chars: self.max_tool_output_chars,
            proactive_prune_tokens: 0,
            proactive_prune_min_result_chars: 8000,
            abort_on_summary_failure: false,
            min_tail_user_messages: 1,
        }
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            threshold_tokens: default_threshold_tokens(),
            tail_turns: default_tail_turns(),
            max_tool_output_chars: default_max_tool_output_chars(),
            summary_target_chars: default_summary_target_chars(),
        }
    }
}

fn default_threshold_tokens() -> usize {
    12_000
}
fn default_tail_turns() -> usize {
    6
}
fn default_max_tool_output_chars() -> usize {
    4_000
}
fn default_summary_target_chars() -> usize {
    2_000
}

fn default_checkpoint_interval() -> usize {
    // 默认每轮都保存（保持原行为），可通过配置调大以减少 I/O
    1
}

fn default_max_concurrent_tools() -> usize {
    // 默认允许最多 10 个工具并发执行，防止 LLM 一次返回大量工具调用时资源耗尽
    10
}

fn default_delegate_summary_max_chars() -> usize {
    // 委派摘要预算：默认 8000 字符（约 2000 tokens），防止子 Agent 大结果撑爆父上下文
    8_000
}

/// 结构化提示词配置
///
/// 三层架构：stable / context / volatile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptConfig {
    /// 稳定层：身份定义
    #[serde(default)]
    pub identity: String,
    /// 稳定层：执行指导（工具使用、委派规则等）
    #[serde(default)]
    pub guidance: String,
    /// 稳定层：工具调用规则
    #[serde(default)]
    pub tool_rules: String,
    /// 上下文文件加载配置
    #[serde(default)]
    pub context_files: ContextFilesConfig,
}

impl Default for PromptConfig {
    fn default() -> Self {
        Self {
            identity: DEFAULT_IDENTITY.to_string(),
            guidance: DEFAULT_GUIDANCE.to_string(),
            tool_rules: DEFAULT_TOOL_RULES.to_string(),
            context_files: ContextFilesConfig::default(),
        }
    }
}

/// 上下文文件加载配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextFilesConfig {
    /// 是否启用外部上下文文件加载
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 要加载的文件名列表（按优先级排列，从工作目录向上查找）
    #[serde(default = "default_context_file_names")]
    pub names: Vec<String>,
    /// 单个文件最大字符数
    #[serde(default = "default_max_chars")]
    pub max_chars: usize,
}

impl Default for ContextFilesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            names: default_context_file_names(),
            max_chars: default_max_chars(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_context_file_names() -> Vec<String> {
    vec![
        "AGENTS.md".to_string(),
        "AGENTS.override.md".to_string(),
        ".cursorrules".to_string(),
        "SOUL.md".to_string(),
    ]
}

fn default_max_chars() -> usize {
    20_000
}

pub const DEFAULT_SYSTEM_PROMPT: &str = r#"You are a helpful AI agent operating within the Loom Agent OS.

Your goal: {goal}

Context:
{context}

Available tools:
{tools}

Rules:
- Use tools to accomplish your goal when appropriate
- For complex sub-tasks, delegate to sub-agents via spawn_agent
- spawn_agent supports a single "goal" OR a "tasks" array for batch delegation
- When "tasks" is provided, all sub-agents run concurrently and results are returned together
- For top-level agents (no parent), spawn_agent with background=true returns immediately with a handle;
  the result becomes available later. Sub-agents always block synchronously.
- Use action="list" to list running sub-agents, action="stop" to stop a sub-agent by subagent_id
- After all necessary tool calls, provide a concise final summary of what was accomplished
"#;

pub const DEFAULT_IDENTITY: &str =
    "You are Loom Agent, an AI agent operating within the Loom Agent OS. \
Be direct: match the length of your reply to the weight of the ask. \
No filler, no restating the request back, no narrating tool calls. \
Use tools to accomplish your goal; for complex sub-tasks, delegate to sub-agents.";

pub const DEFAULT_GUIDANCE: &str = r#"## Execution Guidance
- Use tools to accomplish your goal when appropriate
- For complex sub-tasks, delegate to sub-agents via spawn_agent
- spawn_agent supports a single "goal" OR a "tasks" array for batch delegation
- When "tasks" is provided, all sub-agents run concurrently and results are returned together
- For top-level agents (no parent), spawn_agent with background=true returns immediately with a handle; the result becomes available later. Sub-agents always block synchronously.
- After all necessary tool calls, provide a concise final summary of what was accomplished"#;

pub const DEFAULT_TOOL_RULES: &str = r#"## Tool Rules
- Use action="list" to list running sub-agents, action="stop" to stop a sub-agent by subagent_id
- When calling spawn_agent, provide a clear goal for the sub-agent
- Sub-agent results are returned as JSON; parse them to extract useful information
- If a tool call fails, try an alternative approach or explain the issue"#;