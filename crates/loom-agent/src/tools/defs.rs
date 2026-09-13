use loom_llm::ToolDefinition;
use serde_json::json;

/// spawn_agent 的工具定义（系统级 inline tool）
pub fn spawn_agent_tool_def() -> ToolDefinition {
    ToolDefinition {
        name: "spawn_agent".to_string(),
        description: "委派子 Agent 执行任务。支持单 goal 或 tasks 批量委派。\
                子 Agent 可并行执行。顶层 Agent 可设 background=true 异步返回。\
                可用 action=list 列出子 Agent，action=stop 停止指定子 Agent，\
                action=steer 向运行中的子 Agent 追加 steering 消息。"
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "goal": {
                    "type": "string",
                    "description": "单个子 Agent 的目标（与 tasks 二选一）"
                },
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "goal": {"type": "string", "description": "子任务目标"},
                            "context": {"type": "string", "description": "子任务上下文"},
                            "output_schema": {"type": "object", "description": "JSON Schema，验证子任务输出（可选）"}
                        },
                        "required": ["goal"]
                    },
                    "description": "批量委派的任务列表，所有子 Agent 并行执行"
                },
                "context": {
                    "type": "string",
                    "description": "上下文信息",
                    "default": ""
                },
                "output_schema": {
                    "type": "object",
                    "description": "JSON Schema，用于验证子 Agent 的最终输出（仅单 goal 时使用；tasks 时在每个 task 内指定）"
                },
                "toolsets": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "子 Agent 可用的工具集（仅能使用父 Agent 也拥有的工具集）"
                },
                "isolation": {
                    "type": "string",
                    "enum": ["coroutine", "process", "container", "wasm"],
                    "description": "隔离级别",
                    "default": "coroutine"
                },
                "background": {
                    "type": "boolean",
                    "description": "仅顶层 Agent 有效：true 时异步返回 handle，不等待结果",
                    "default": false
                },
                "action": {
                    "type": "string",
                    "enum": ["spawn", "list", "stop", "steer"],
                    "description": "操作类型：spawn(默认)委派任务，list列出子Agent，stop停止子Agent，steer向子Agent追加消息",
                    "default": "spawn"
                },
                "subagent_id": {
                    "type": "string",
                    "description": "action=stop 或 action=steer 时指定目标子 Agent ID"
                },
                "message": {
                    "type": "string",
                    "description": "action=steer 时发送给子 Agent 的 steering 消息"
                }
            },
            "required": []
        }),
    }
}

/// interrupt 工具定义（human-in-the-loop）
///
/// Agent 调用此工具暂停执行，将 value 暴露给用户，
/// 等待用户通过 resume 提供输入后继续执行。
pub fn interrupt_tool_def() -> ToolDefinition {
    ToolDefinition {
        name: "interrupt".to_string(),
        description: "Pause execution and wait for human input. \
                Use when you need human approval, clarification, or a decision. \
                The 'value' field is shown to the human. \
                When resumed, the human's response is returned as the tool result."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "object",
                    "description": "Content to show to the human (e.g. question, draft for approval)"
                },
                "prompt": {
                    "type": "string",
                    "description": "A short prompt instructing the human what to do"
                }
            },
            "required": ["value"]
        }),
    }
}