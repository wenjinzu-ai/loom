use thiserror::Error;
use uuid::Uuid;

/// LLM API 错误分类
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmErrorKind {
    /// 429 限流
    RateLimit,
    /// 5xx 服务端错误
    ServerError,
    /// 413 / context_length_exceeded 上下文溢出
    ContextOverflow,
    /// 401 / 403 认证失败
    Authentication,
    /// 402 / content_policy 内容策略拦截
    ContentPolicy,
    /// 400 请求无效（不可重试）
    InvalidRequest,
    /// 网络超时
    Timeout,
    /// 连接错误
    Connection,
    /// 未知错误
    Unknown,
}

impl LlmErrorKind {
    /// 是否可重试（限流、服务端错误、超时、连接错误可重试）
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            LlmErrorKind::RateLimit
                | LlmErrorKind::ServerError
                | LlmErrorKind::Timeout
                | LlmErrorKind::Connection
                | LlmErrorKind::Unknown
        )
    }
}

/// LLM API 错误（结构化，带 HTTP 状态码和分类）
#[derive(Debug, Clone)]
pub struct LlmApiError {
    pub kind: LlmErrorKind,
    pub status_code: Option<u16>,
    pub message: String,
    /// Retry-After header 秒数（429 时）
    pub retry_after_secs: Option<u64>,
}

impl std::fmt::Display for LlmApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.kind {
            LlmErrorKind::RateLimit => "rate_limit",
            LlmErrorKind::ServerError => "server_error",
            LlmErrorKind::ContextOverflow => "context_overflow",
            LlmErrorKind::Authentication => "authentication",
            LlmErrorKind::ContentPolicy => "content_policy",
            LlmErrorKind::InvalidRequest => "invalid_request",
            LlmErrorKind::Timeout => "timeout",
            LlmErrorKind::Connection => "connection",
            LlmErrorKind::Unknown => "unknown",
        };
        write!(
            f,
            "{} (status={:?}): {}",
            kind, self.status_code, self.message
        )
    }
}

/// 工具失败分类
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolFailureKind {
    /// 瞬时失败（网络、超时），可重试
    Transient,
    /// 永久失败（参数错误、权限不足），不应重试
    Permanent,
}

impl ToolFailureKind {
    pub fn from_error_message(msg: &str) -> Self {
        let lower = msg.to_lowercase();
        if lower.contains("timeout")
            || lower.contains("timed out")
            || lower.contains("connection reset")
            || lower.contains("connection refused")
            || lower.contains("temporarily unavailable")
            || lower.contains("rate limit")
            || lower.contains("too many requests")
        {
            ToolFailureKind::Transient
        } else {
            ToolFailureKind::Permanent
        }
    }
}

#[derive(Debug, Error)]
pub enum LoomError {
    #[error("capability not found: {0}")]
    CapabilityNotFound(String),

    #[error("capability already registered: {0}")]
    CapabilityAlreadyRegistered(String),

    #[error("agent not found: {0}")]
    AgentNotFound(Uuid),

    #[error("invalid agent state transition: {from} -> {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("adapter not found for source: {0:?}")]
    AdapterNotFound(String),

    #[error("isolation level not supported: {0:?}")]
    IsolationNotSupported(String),

    #[error("tool execution failed: {0}")]
    ToolExecution(String),

    #[error("agent execution failed: {0}")]
    AgentExecution(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("timeout")]
    Timeout,

    #[error("cancelled")]
    Cancelled,

    #[error("LLM API error: {0}")]
    LlmApi(LlmApiError),

    #[error("guardrail halt: {0}")]
    GuardrailHalt(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, LoomError>;