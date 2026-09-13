//! 消息总线
//!
//! 支持点对点消息、请求-响应、发布订阅。

use async_trait::async_trait;
use loom_core::{AgentMessage, AgentResponse, Event, MessageBus, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

struct Inbox {
    tx: mpsc::Sender<AgentMessage>,
}

pub struct InMemoryBus {
    inboxes: parking_lot::RwLock<HashMap<Uuid, Inbox>>,
    topics: parking_lot::RwLock<HashMap<String, Vec<mpsc::Sender<Event>>>>,
    pending: Mutex<HashMap<String, oneshot::Sender<AgentResponse>>>,
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self {
            inboxes: parking_lot::RwLock::new(HashMap::new()),
            topics: parking_lot::RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub fn register_agent(&self, agent_id: Uuid) -> mpsc::Receiver<AgentMessage> {
        let (tx, rx) = mpsc::channel(256);
        self.inboxes.write().insert(agent_id, Inbox { tx });
        rx
    }

    /// 投递一个响应（由处理请求的 Agent 调用）
    pub fn respond(&self, correlation_id: &str, response: AgentResponse) {
        if let Some(tx) = self.pending.lock().remove(correlation_id) {
            let _ = tx.send(response);
        }
    }
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageBus for InMemoryBus {
    async fn send(&self, msg: AgentMessage) -> Result<()> {
        let tx = {
            let inboxes = self.inboxes.read();
            inboxes.get(&msg.to).map(|inbox| inbox.tx.clone())
        };
        if let Some(tx) = tx {
            tx.send(msg)
                .await
                .map_err(|_| loom_core::LoomError::Other("agent inbox closed".into()))?;
        }
        Ok(())
    }

    async fn request(&self, msg: AgentMessage) -> Result<AgentResponse> {
        let corr_id = msg
            .correlation_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let mut msg = msg;
        msg.correlation_id = Some(corr_id.clone());

        let (tx, rx) = oneshot::channel::<AgentResponse>();
        self.pending.lock().insert(corr_id.clone(), tx);

        self.send(msg).await?;

        match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(loom_core::LoomError::Other(
                "request responder dropped".into(),
            )),
            Err(_) => {
                self.pending.lock().remove(&corr_id);
                Err(loom_core::LoomError::Timeout)
            }
        }
    }

    async fn publish(&self, event: Event) -> Result<()> {
        let subscribers = {
            let topics = self.topics.read();
            topics.get(&event.topic).cloned().unwrap_or_default()
        };
        for tx in subscribers {
            let _ = tx.send(event.clone()).await;
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Event>> {
        let (tx, rx) = mpsc::channel(256);
        self.topics
            .write()
            .entry(topic.to_string())
            .or_default()
            .push(tx);
        Ok(rx)
    }
}
