//! 配置加载模块
//!
//! 从 config.toml 加载应用配置，包括：
//! - 服务器监听地址
//! - LLM Provider 配置
//! - Agent 循环配置
//! - 数据库配置

use loom_agent::{
    AgentLoopConfig, ApiRetryConfig, CompressionConfig, ContextFilesConfig, GuardrailConfig,
    LivenessConfig, PostgresConfig, PromptConfig,
};
use loom_llm::OpenAiConfig;
use serde::Deserialize;
use std::path::Path;

/// 应用配置
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    pub llm: LlmConfig,
    pub agent: AgentConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// 日志配置
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// 日志过滤表达式，等同于 tracing EnvFilter 的语法
    /// 例如："loom=debug,tower_http=info" 或 "info,loom_agent=debug"
    /// 支持的级别：trace < debug < info < warn < error
    /// 优先级：RUST_LOG 环境变量 > 此配置 > 默认值
    #[serde(default = "default_log_filter")]
    pub filter: String,
}

fn default_log_filter() -> String {
    "loom=debug,tower_http=info".into()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: default_log_filter(),
        }
    }
}

/// 服务器配置
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// 自我唤醒用的基础 URL（推模式投递）。
    /// 后台任务完成后，completion watcher 会向 `${self_base_url}/chat` 发送空 POST 唤醒会话。
    /// 不设置时回退为 `http://{host}:{port}`。
    #[serde(default)]
    pub self_base_url: Option<String>,
}

/// 数据库配置
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_host")]
    pub host: String,
    #[serde(default = "default_db_port")]
    pub port: u16,
    #[serde(default = "default_db_user")]
    pub user: String,
    #[serde(default = "default_db_password")]
    pub password: String,
    #[serde(default = "default_db_name")]
    pub dbname: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_db_host() -> String {
    "127.0.0.1".into()
}
fn default_db_port() -> u16 {
    5432
}
fn default_db_user() -> String {
    "postgres".into()
}
fn default_db_password() -> String {
    "postgres".into()
}
fn default_db_name() -> String {
    "postgres".into()
}
fn default_max_connections() -> u32 {
    5
}
fn default_connect_timeout() -> u64 {
    10
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: default_db_host(),
            port: default_db_port(),
            user: default_db_user(),
            password: default_db_password(),
            dbname: default_db_name(),
            max_connections: default_max_connections(),
            connect_timeout_secs: default_connect_timeout(),
        }
    }
}

impl From<DatabaseConfig> for PostgresConfig {
    fn from(c: DatabaseConfig) -> Self {
        // 以配置文件值为基础，环境变量存在时覆盖（环境变量优先级更高）
        let mut cfg = PostgresConfig {
            host: c.host,
            port: c.port,
            user: c.user,
            password: c.password,
            dbname: c.dbname,
            max_connections: c.max_connections,
            connect_timeout_secs: c.connect_timeout_secs,
        };
        if let Ok(v) = std::env::var("POSTGRES_HOST") {
            cfg.host = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_PORT") {
            if let Ok(port) = v.parse() {
                cfg.port = port;
            }
        }
        if let Ok(v) = std::env::var("POSTGRES_USER") {
            cfg.user = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_PASSWORD") {
            cfg.password = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_DBNAME") {
            cfg.dbname = v;
        }
        if let Ok(v) = std::env::var("POSTGRES_MAX_CONNECTIONS") {
            if let Ok(n) = v.parse() {
                cfg.max_connections = n;
            }
        }
        cfg
    }
}

/// LLM 配置（直接映射到 OpenAiConfig）
#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    #[serde(default)]
    pub temperature: Option<f32>,
    /// 单次响应的最大 token 数（不设置则使用模型默认值）
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// 模型的上下文窗口大小（输入+输出总上限）
    ///
    /// 不同模型厂商的上下文窗口不同（如 gpt-4o=128K, qwen-turbo=32K）。
    /// 用于自动计算压缩阈值：threshold = max_context_tokens * compression_ratio。
    /// 不设置时使用压缩配置中的 threshold_tokens 原值。
    #[serde(default)]
    pub max_context_tokens: Option<usize>,
    /// 压缩阈值占上下文窗口的比例（0.0~1.0），默认 0.75
    /// 即上下文达到窗口的 75% 时触发压缩，留出 25% 给响应。
    #[serde(default = "default_compression_ratio")]
    pub compression_ratio: f32,
}

fn default_compression_ratio() -> f32 {
    0.75
}

/// Agent 配置
#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    pub max_iterations: usize,
    pub allow_delegation: bool,
    pub max_delegation_depth: u32,
    pub max_concurrent_children: usize,
    #[serde(default)]
    pub system_prompt: String,
    #[serde(default)]
    pub prompt: AgentPromptConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: usize,
    #[serde(default = "default_max_concurrent_tools")]
    pub max_concurrent_tools: usize,
    #[serde(default)]
    pub api_retry: ApiRetryConfig,
    #[serde(default)]
    pub guardrails: GuardrailConfig,
    #[serde(default)]
    pub liveness: LivenessConfig,
    #[serde(default = "default_delegate_summary_max_chars")]
    pub delegate_summary_max_chars: usize,
}

fn default_checkpoint_interval() -> usize {
    1
}

fn default_max_concurrent_tools() -> usize {
    10
}

fn default_delegate_summary_max_chars() -> usize {
    8_000
}

/// 结构化提示词配置（映射到 loom-agent 的 PromptConfig）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AgentPromptConfig {
    #[serde(default)]
    pub identity: String,
    #[serde(default)]
    pub guidance: String,
    #[serde(default)]
    pub tool_rules: String,
    #[serde(default)]
    pub context_files: AgentContextFilesConfig,
}

/// 上下文文件加载配置
#[derive(Debug, Clone, Deserialize)]
pub struct AgentContextFilesConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_context_names")]
    pub names: Vec<String>,
    #[serde(default = "default_max_chars")]
    pub max_chars: usize,
}

impl Default for AgentContextFilesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            names: default_context_names(),
            max_chars: default_max_chars(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_context_names() -> Vec<String> {
    vec![
        "AGENTS.md".into(),
        "AGENTS.override.md".into(),
        ".cursorrules".into(),
        "SOUL.md".into(),
    ]
}
fn default_max_chars() -> usize {
    20_000
}

impl AppConfig {
    /// 从 TOML 文件加载配置
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// 从默认位置加载配置（config.toml）
    ///
    /// 优先级：`--config <path>` CLI 参数 > `LOOM_CONFIG` 环境变量 > 默认搜索路径
    pub fn load_default() -> anyhow::Result<Self> {
        // 1. CLI 参数 --config <path>
        if let Some(path) = parse_config_from_args() {
            tracing::info!("loading config from CLI --config: {}", path);
            return Self::from_file(path);
        }

        // 2. 环境变量 LOOM_CONFIG
        if let Ok(path) = std::env::var("LOOM_CONFIG") {
            if !path.is_empty() {
                tracing::info!("loading config from LOOM_CONFIG: {}", path);
                return Self::from_file(path);
            }
        }

        // 3. 默认搜索路径
        let paths = ["config.toml", "./config.toml", "../config.toml"];
        for path in &paths {
            if Path::new(path).exists() {
                tracing::info!("loading config from {}", path);
                return Self::from_file(path);
            }
        }
        anyhow::bail!(
            "config file not found, searched: {}. Provide via --config <path> or LOOM_CONFIG env var",
            paths.join(", ")
        )
    }
}

/// 从命令行参数中解析 `--config <path>`，返回第一个匹配的路径
fn parse_config_from_args() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--config" || args[i] == "-c" {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
        } else if let Some(stripped) = args[i].strip_prefix("--config=") {
            return Some(stripped.to_string());
        }
        i += 1;
    }
    None
}

impl From<LlmConfig> for OpenAiConfig {
    fn from(c: LlmConfig) -> Self {
        OpenAiConfig {
            base_url: c.base_url,
            api_key: c.api_key,
            model: c.model,
            temperature: c.temperature,
            max_tokens: c.max_tokens,
        }
    }
}

impl From<AgentConfig> for AgentLoopConfig {
    fn from(c: AgentConfig) -> Self {
        AgentLoopConfig {
            model: AgentLoopConfig::default().model,
            system_prompt: c.system_prompt,
            max_iterations: c.max_iterations,
            allow_delegation: c.allow_delegation,
            max_delegation_depth: c.max_delegation_depth,
            max_concurrent_children: c.max_concurrent_children,
            prompt: PromptConfig {
                identity: c.prompt.identity,
                guidance: c.prompt.guidance,
                tool_rules: c.prompt.tool_rules,
                context_files: ContextFilesConfig {
                    enabled: c.prompt.context_files.enabled,
                    names: c.prompt.context_files.names,
                    max_chars: c.prompt.context_files.max_chars,
                },
            },
            compression: c.compression,
            checkpoint_interval: c.checkpoint_interval,
            max_concurrent_tools: c.max_concurrent_tools,
            api_retry: c.api_retry,
            guardrails: c.guardrails,
            liveness: c.liveness,
            delegate_summary_max_chars: c.delegate_summary_max_chars,
        }
    }
}