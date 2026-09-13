use std::time::Duration;

use loom_core::{LlmApiError, LlmErrorKind, Result};
use loom_llm::{ChatMessage, ChatResponse, LlmProvider, ToolDefinition};

use crate::config::ApiRetryConfig;

/// 带指数退避重试的 LLM 调用
///
/// 重试策略：
/// - 可重试错误（RateLimit/ServerError/Timeout/Connection/Unknown）：指数退避重试
/// - 429 限流：优先使用 Retry-After header，否则指数退避
/// - ContextOverflow：不重试，直接返回，由调用方压缩上下文后重新调用
/// - Authentication/ContentPolicy/InvalidRequest：不重试，直接返回
pub async fn chat_with_retry(
    provider: &dyn LlmProvider,
    messages: Vec<ChatMessage>,
    tools: Vec<ToolDefinition>,
    config: &ApiRetryConfig,
) -> Result<ChatResponse> {
    let mut attempt: u32 = 0;
    loop {
        let result = if config.request_timeout_secs > 0 {
            tokio::time::timeout(
                Duration::from_secs(config.request_timeout_secs),
                provider.chat(messages.clone(), tools.clone()),
            )
            .await
            .map_err(|_| loom_core::LoomError::LlmApi(LlmApiError {
                kind: LlmErrorKind::Timeout,
                status_code: None,
                message: format!(
                    "LLM request timed out after {}s",
                    config.request_timeout_secs
                ),
                retry_after_secs: None,
            }))?
        } else {
            provider.chat(messages.clone(), tools.clone()).await
        };

        match result {
            Ok(resp) => return Ok(resp),
            Err(loom_core::LoomError::LlmApi(api_err)) => {
                if attempt >= config.max_retries {
                    tracing::warn!(
                        "[api_retry] max retries ({}) exhausted for {:?}",
                        config.max_retries,
                        api_err.kind
                    );
                    return Err(loom_core::LoomError::LlmApi(api_err));
                }
                if !api_err.kind.retryable() {
                    tracing::debug!(
                        "[api_retry] non-retryable error {:?}, not retrying",
                        api_err.kind
                    );
                    return Err(loom_core::LoomError::LlmApi(api_err));
                }
                let delay = compute_delay(&api_err, config, attempt);
                tracing::warn!(
                    "[api_retry] attempt {}/{} failed (kind={:?}), retrying in {}ms",
                    attempt + 1,
                    config.max_retries,
                    api_err.kind,
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// 计算重试延迟：429 优先用 retry_after，否则指数退避
fn compute_delay(api_err: &LlmApiError, config: &ApiRetryConfig, attempt: u32) -> Duration {
    if api_err.kind == LlmErrorKind::RateLimit {
        if let Some(secs) = api_err.retry_after_secs {
            return Duration::from_secs(secs);
        }
    }
    let exp = config.base_delay_ms * 2u64.pow(attempt);
    let capped = exp.min(config.max_delay_ms);
    Duration::from_millis(capped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_delay_rate_limit_with_retry_after() {
        let config = ApiRetryConfig::default();
        let err = LlmApiError {
            kind: LlmErrorKind::RateLimit,
            status_code: Some(429),
            message: "rate limited".into(),
            retry_after_secs: Some(10),
        };
        let delay = compute_delay(&err, &config, 0);
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    fn test_compute_delay_exponential_backoff() {
        let config = ApiRetryConfig {
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            ..Default::default()
        };
        let err = LlmApiError {
            kind: LlmErrorKind::ServerError,
            status_code: Some(500),
            message: "internal error".into(),
            retry_after_secs: None,
        };
        assert_eq!(compute_delay(&err, &config, 0), Duration::from_millis(1000));
        assert_eq!(compute_delay(&err, &config, 1), Duration::from_millis(2000));
        assert_eq!(compute_delay(&err, &config, 2), Duration::from_millis(4000));
        assert_eq!(compute_delay(&err, &config, 5), Duration::from_millis(30000));
    }
}