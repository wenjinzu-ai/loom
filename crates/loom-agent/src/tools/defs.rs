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
///
/// value 支持三种结构化格式，前端会渲染为对应的交互卡片：
///
/// 1. 选项按钮（适合确认/选择场景）：
/// ```json
/// {
///   "prompt": "请确认是否删除文件 xxx？",
///   "options": [
///     {"label": "确认", "value": {"approved": true}},
///     {"label": "取消", "value": {"approved": false}}
///   ],
///   "allow_input": false
/// }
/// ```
/// - options: 可点击的选项按钮，点击后 option.value 直接作为 resume_value 返回
/// - allow_input: 是否同时显示自由输入框（默认 false）
///
/// 2. 自由输入（适合开放式回答）：
/// ```json
/// {
///   "prompt": "请输入你希望生成的文件路径：",
///   "allow_input": true
/// }
/// ```
///
/// 3. 多字段表单（适合需要收集多条信息的场景）：
/// ```json
/// {
///   "prompt": "请填写定时任务信息：",
///   "fields": [
///     {"name": "name", "label": "任务名称", "type": "text", "required": true},
///     {"name": "frequency", "label": "执行频率", "type": "select",
///      "options": [{"label": "每天", "value": "daily"}, {"label": "每周", "value": "weekly"}]},
///     {"name": "cron", "label": "Cron 表达式", "type": "text", "placeholder": "0 3 * * *"},
///     {"name": "desc", "label": "描述", "type": "textarea"}
///   ]
/// }
/// ```
/// - fields: 字段数组，支持 type: text / textarea / select / number
/// - 提交时所有字段值组装为对象作为 resume_value 返回
///
/// label 建议使用简短动词（确认/取消/同意/拒绝），前端会自动着色。
pub fn interrupt_tool_def() -> ToolDefinition {
    ToolDefinition {
        name: "interrupt".to_string(),
        description: "Pause execution and wait for human input. \
                Use when you need human approval, clarification, a decision, or to collect multiple fields of information. \
                The 'value' field is shown to the human. When resumed, the human's response is returned as the tool result.\n\n\
                value supports three structured formats:\n\n\
                1) Option buttons (approval/choice): {\"prompt\": \"...\", \"options\": [{\"label\": \"Confirm\", \"value\": {\"approved\": true}}, {\"label\": \"Cancel\", \"value\": {\"approved\": false}}], \"allow_input\": false}. Clicking an option returns its 'value' as resume_value.\n\n\
                2) Free text input: {\"prompt\": \"...\", \"allow_input\": true}.\n\n\
                3) Multi-field form: {\"prompt\": \"...\", \"fields\": [{\"name\": \"...\", \"label\": \"...\", \"type\": \"text|textarea|select|number\", \"required\": true|false, \"placeholder\": \"...\", \"options\": [{\"label\": \"...\", \"value\": \"...\"}]}]}. All field values are submitted as one object as resume_value.\n\n\
                Use short verbs for option labels (Confirm/Cancel/Approve/Reject); the frontend auto-colors them."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "object",
                    "description": "Content shown to the human. Use one of: {prompt, options, allow_input} for option buttons; {prompt, allow_input:true} for free text; {prompt, fields:[...]} for a multi-field form."
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