//! 会话唤醒器：后台任务完成时通过 self-POST 唤醒父会话
//!
//! 推模式投递机制：子 Agent 完成后，completion watcher
//! 调用 `wake`，本实现向 `${self_base_url}/chat` 发送空 POST 请求，
//! 触发父会话在新一轮中拉取并回注已完成的后台结果。

use loom_agent::delegation::SessionWaker;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// 同一 session 的唤醒去抖窗口：在此时间内重复 wake 会被忽略。
const DEDUP_WINDOW: Duration = Duration::from_secs(1);
/// self-POST 最大重试次数。
const MAX_RETRIES: u32 = 3;
/// 重试退避基数（指数退避：base * 2^attempt）。
const RETRY_BASE: Duration = Duration::from_millis(100);

/// 基于 HTTP self-POST 的会话唤醒器
pub struct SelfPostWaker {
    base_url: String,
    client: reqwest::Client,
    /// 记录每个 session 最近一次成功 wake 的时间，用于去重。
    last_wake: Mutex<HashMap<String, Instant>>,
}

impl SelfPostWaker {
    pub fn new(base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            base_url,
            client,
            last_wake: Mutex::new(HashMap::new()),
        }
    }
}

impl SessionWaker for SelfPostWaker {
    fn wake(&self, session_id: &str) {
        // 去重：同一 session 在 DEDUP_WINDOW 内只 wake 一次，避免短时间内
        // 多个后台任务完成时触发重复 self-POST。
        {
            let mut last = self.last_wake.lock();
            let now = Instant::now();
            if let Some(&prev) = last.get(session_id) {
                if now.duration_since(prev) < DEDUP_WINDOW {
                    tracing::debug!(
                        "[waker] skipping duplicate wake for session {} (within {:?})",
                        session_id,
                        DEDUP_WINDOW
                    );
                    return;
                }
            }
            last.insert(session_id.to_string(), now);
        }

        let url = format!("{}/chat", self.base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "message": "",
            "session_id": session_id,
        });
        let client = self.client.clone();
        let sid = session_id.to_string();
        // 异步发起 HTTP self-POST，不阻塞 completion watcher
        tokio::spawn(async move {
            for attempt in 0..=MAX_RETRIES {
                match client.post(&url).json(&body).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            tracing::debug!(
                                "[waker] successfully woke session {} via self-POST (attempt {})",
                                sid,
                                attempt + 1
                            );
                            return;
                        }
                        tracing::warn!(
                            "[waker] self-POST wake for session {} returned {} (attempt {}/{})",
                            sid,
                            resp.status(),
                            attempt + 1,
                            MAX_RETRIES + 1
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[waker] self-POST wake for session {} failed: {} (attempt {}/{})",
                            sid,
                            e,
                            attempt + 1,
                            MAX_RETRIES + 1
                        );
                    }
                }
                if attempt < MAX_RETRIES {
                    let backoff = RETRY_BASE * 2u32.pow(attempt);
                    tokio::time::sleep(backoff).await;
                }
            }
            tracing::error!(
                "[waker] exhausted {} retries waking session {}",
                MAX_RETRIES,
                sid
            );
        });
    }
}