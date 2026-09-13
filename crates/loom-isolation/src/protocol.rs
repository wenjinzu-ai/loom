//! 进程 / 容器后端共享的 JSON Line 通信协议
//!
//! 父进程（Host）与子进程 / 容器（Guest）通过 stdin/stdout 交换 JSON 行。
//!
//! ## Host → Guest（stdin，每行一个 JSON 对象）
//! - `{"type":"message","payload":<AgentMessage>}` 发送消息
//! - `{"type":"stop"}` 请求停止
//!
//! ## Guest → Host（stdout，每行一个 JSON 对象）
//! - `{"type":"stream","payload":<Value>}` 流式输出片段
//! - `{"type":"output","payload":<AgentOutput>}` 最终结构化输出（仅一次）
//!
//! ## Guest stderr
//! 纯文本日志行，由 Host 转发到 tracing。
//!
//! Guest 启动参数：
//! - `--agent-config <base64(AgentSpec)>` 命令行参数，或
//! - `LOOM_AGENT_CONFIG` 环境变量（base64 编码的 AgentSpec JSON）

use serde::{Deserialize, Serialize};

use loom_core::{AgentMessage, AgentOutput};

/// Host → Guest 的指令
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostCommand {
    Message { payload: AgentMessage },
    Stop,
}

/// Guest → Host 的事件
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestEvent {
    Stream { payload: serde_json::Value },
    Output { payload: AgentOutput },
}

/// 将 AgentSpec 编码为 base64 JSON，用于通过命令行参数或环境变量传递
pub fn encode_spec(spec: &loom_core::AgentSpec) -> Result<String, loom_core::LoomError> {
    let json = serde_json::to_string(spec)?;
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(json.as_bytes()))
}

/// 从 base64 字符串解码 AgentSpec
pub fn decode_spec(encoded: &str) -> Result<loom_core::AgentSpec, loom_core::LoomError> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| loom_core::LoomError::Other(format!("invalid base64 config: {e}")))?;
    let spec: loom_core::AgentSpec = serde_json::from_slice(&bytes)?;
    Ok(spec)
}

/// 从环境变量或命令行参数读取 AgentSpec
///
/// 优先级：环境变量 `LOOM_AGENT_CONFIG` > 命令行 `--agent-config`
pub fn load_spec_from_env() -> Option<loom_core::AgentSpec> {
    std::env::var("LOOM_AGENT_CONFIG")
        .ok()
        .and_then(|v| decode_spec(&v).ok())
}

/// 解析命令行参数中的 `--agent-config <base64>`
pub fn parse_config_arg() -> Option<loom_core::AgentSpec> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--agent-config" {
            if let Some(val) = args.next() {
                return decode_spec(&val).ok();
            }
        } else if let Some(val) = arg.strip_prefix("--agent-config=") {
            return decode_spec(val).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom_core::{AgentMessage, IsolationLevel, MessageContent};
    use uuid::Uuid;

    fn sample_spec() -> loom_core::AgentSpec {
        loom_core::AgentSpec {
            agent_id: Uuid::nil(),
            capability_id: None,
            goal: "test goal".into(),
            context: "ctx".into(),
            toolsets: vec![],
            isolation: IsolationLevel::Process,
            timeout: None,
            config: serde_json::json!({}),
            parent_agent_id: None,
            delegate_depth: 0,
            scope: None,
            parent_toolsets: vec![],
            session_id: None,
            activity_state: None,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let spec = sample_spec();
        let encoded = encode_spec(&spec).unwrap();
        assert!(!encoded.is_empty());
        let decoded = decode_spec(&encoded).unwrap();
        assert_eq!(decoded.agent_id, spec.agent_id);
        assert_eq!(decoded.goal, spec.goal);
        assert_eq!(decoded.isolation, IsolationLevel::Process);
    }

    #[test]
    fn decode_invalid_base64() {
        assert!(decode_spec("not!base64!").is_err());
    }

    #[test]
    fn host_command_message_serde() {
        let msg = AgentMessage::new(Uuid::nil(), Uuid::nil(), MessageContent::Text("hi".into()));
        let cmd = HostCommand::Message { payload: msg };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"type\":\"message\""));
        let back: HostCommand = serde_json::from_str(&json).unwrap();
        match back {
            HostCommand::Message { .. } => {}
            _ => panic!("expected Message"),
        }
    }

    #[test]
    fn host_command_stop_serde() {
        let cmd = HostCommand::Stop;
        let json = serde_json::to_string(&cmd).unwrap();
        assert_eq!(json, "{\"type\":\"stop\"}");
        let back: HostCommand = serde_json::from_str(&json).unwrap();
        matches!(back, HostCommand::Stop);
    }

    #[test]
    fn guest_event_serde() {
        let event = GuestEvent::Stream {
            payload: serde_json::json!({"step": 1}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"stream\""));
        let back: GuestEvent = serde_json::from_str(&json).unwrap();
        match back {
            GuestEvent::Stream { payload } => assert_eq!(payload["step"], 1),
            _ => panic!("expected Stream"),
        }
    }
}