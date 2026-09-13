use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;

/// 消息内容类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageContent {
    Text(String),
    Json(serde_json::Value),
    ToolCall {
        name: String,
        args: serde_json::Value,
    },
    ToolResult {
        call_id: String,
        result: serde_json::Value,
    },
    StreamChunk(serde_json::Value),
}

/// Agent 间消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    pub id: Uuid,
    pub from: Uuid,
    pub to: Uuid,
    pub content: MessageContent,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl AgentMessage {
    pub fn new(from: Uuid, to: Uuid, content: MessageContent) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to,
            content,
            correlation_id: None,
            metadata: HashMap::new(),
        }
    }
}

/// Agent 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub message_id: Uuid,
    pub content: MessageContent,
}

/// 事件（用于 Pub/Sub）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub topic: String,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 消息总线 trait
#[async_trait::async_trait]
pub trait MessageBus: Send + Sync {
    async fn send(&self, msg: AgentMessage) -> Result<()>;
    async fn request(&self, msg: AgentMessage) -> Result<AgentResponse>;
    async fn publish(&self, event: Event) -> Result<()>;
    async fn subscribe(&self, topic: &str) -> Result<tokio::sync::mpsc::Receiver<Event>>;
}
