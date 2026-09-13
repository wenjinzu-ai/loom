//! 澄清工具集
//!
//! - ask_user: 向用户提问并等待回答（同步阻塞等待）
//! - show_message: 向用户显示消息（非阻塞）

use crate::spec::{ToolSet, ToolSpec};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use loom_core::{Result, ToolContext};
use serde_json::{json, Value};

pub struct ClarifyToolSet;

#[async_trait]
impl ToolSet for ClarifyToolSet {
    fn name(&self) -> &str {
        "clarify"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![
            ToolSpec {
                name: "ask_user".into(),
                description:
                    "Ask the user a question and wait for their response. Use this when crucial information is missing, ambiguous, or conflicting — do not guess. This blocks execution until the user replies.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "questions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "One or more questions to ask the user."
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Maximum seconds to wait for a reply (default 300).",
                            "default": 300
                        }
                    },
                    "required": ["questions"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["clarify".into()],
            },
            ToolSpec {
                name: "show_message".into(),
                description:
                    "Display a message to the user. Non-blocking — use for progress updates, warnings, or informational notices.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "string",
                            "description": "The message to show the user."
                        },
                        "level": {
                            "type": "string",
                            "enum": ["info", "warning", "success", "error"],
                            "description": "Message severity level.",
                            "default": "info"
                        }
                    },
                    "required": ["message"]
                }),
                output_schema: json!({"type": "object"}),
                streaming: false,
                tags: vec!["clarify".into()],
            },
        ]
    }

    async fn execute(&self, tool_name: &str, args: Value, _ctx: &ToolContext) -> Result<Value> {
        match tool_name {
            "ask_user" => self.ask_user(&args).await,
            "show_message" => self.show_message(&args).await,
            _ => Err(loom_core::LoomError::CapabilityNotFound(tool_name.into())),
        }
    }

    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>> {
        let v = self.execute(tool_name, args, ctx).await?;
        Ok(Box::pin(stream::once(async move { Ok(v) })))
    }
}

impl ClarifyToolSet {
    async fn ask_user(&self, args: &Value) -> Result<Value> {
        let questions: Vec<String> = match args["questions"].as_array() {
            Some(arr) => arr
                .iter()
                .filter_map(|q| q.as_str().map(|s| s.to_string()))
                .collect(),
            None => Vec::new(),
        };

        if questions.is_empty() {
            return Err(loom_core::LoomError::Other(
                "ask_user: 'questions' must be a non-empty array of strings".into(),
            ));
        }

        let timeout = args["timeout"].as_u64().unwrap_or(300).min(600);

        // 在实际运行时，这里会通过 UI 层（如 GUI/CLI）与用户交互。
        // 当前实现：在终端中提示并等待输入。
        // 在无头环境（测试/CI）中，返回一个提示信息表示需要用户输入。
        //
        // 注意：此实现仅为占位，真实场景应由上层 runtime 接管用户交互流程。
        // 为避免在测试/无终端环境下阻塞，这里返回结构化的"等待用户"响应。

        Ok(json!({
            "success": true,
            "pending_user_input": true,
            "questions": questions,
            "timeout": timeout,
            "message": "Waiting for user input. In a full runtime, this would block until the user responds.",
        }))
    }

    async fn show_message(&self, args: &Value) -> Result<Value> {
        let message = match args["message"].as_str() {
            Some(m) if !m.is_empty() => m,
            _ => {
                return Err(loom_core::LoomError::Other(
                    "show_message: 'message' is required and must be non-empty".into(),
                ));
            }
        };
        let level = args["level"].as_str().unwrap_or("info");

        // 在实际运行时，这里会通过 UI 层显示消息。
        // 当前实现：输出到日志。
        match level {
            "error" => tracing::error!("[message] {}", message),
            "warning" => tracing::warn!("[message] {}", message),
            "success" => tracing::info!("[message][success] {}", message),
            _ => tracing::info!("[message] {}", message),
        }

        Ok(json!({
            "success": true,
            "displayed": true,
            "level": level,
            "message": message,
        }))
    }
}