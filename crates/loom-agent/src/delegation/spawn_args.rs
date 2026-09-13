use loom_core::{IsolationLevel, MemoryScope};
use serde_json::{json, Value};
use uuid::Uuid;

use super::schema::coerce_output_schema;

/// 委派任务描述
pub(crate) struct SpawnTask {
    pub goal: String,
    pub context: String,
    /// 可选的 JSON Schema，用于验证子 Agent 输出
    pub output_schema: Option<serde_json::Map<String, Value>>,
}

/// 子 Agent 启动规格（聚合 launch 相关参数，避免方法参数过多）
pub(crate) struct SubagentJob {
    pub goal: String,
    pub context: String,
    pub toolsets: Vec<String>,
    /// 父 Agent 的工具集（用于子 Agent 工具集交集）
    pub parent_toolsets: Vec<String>,
    pub isolation: IsolationLevel,
    pub parent_agent_id: Uuid,
    pub child_depth: u32,
    pub output_schema: Option<serde_json::Map<String, Value>>,
    /// 记忆作用域（从父 Agent 继承，用于租户隔离）
    pub scope: Option<MemoryScope>,
    /// 会话 ID（用于后台结果跨轮次回注）
    pub session_id: Option<String>,
}

/// spawn_agent 工具的解析后参数（将 JSON 解析与业务逻辑分离）
pub(crate) struct SpawnArgs {
    pub action: String,
    pub toolsets: Vec<String>,
    pub isolation: IsolationLevel,
    pub background: bool,
    pub tasks: Vec<SpawnTask>,
}

impl SpawnArgs {
    /// 从 JSON 参数解析，返回 Ok(parsed) 或 Err(错误 JSON)
    pub(crate) fn from_value(args: &Value, is_top_level: bool) -> std::result::Result<Self, Value> {
        let action = args["action"].as_str().unwrap_or("spawn").to_string();

        // control actions（list/stop/steer）不需要任务参数
        if matches!(action.as_str(), "list" | "stop" | "steer") {
            return Ok(Self {
                action,
                toolsets: vec![],
                isolation: IsolationLevel::Coroutine,
                background: false,
                tasks: vec![],
            });
        }

        let toolsets = match args["toolsets"].as_array() {
            Some(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            None => vec![],
        };

        let isolation = match args["isolation"].as_str() {
            Some("process") => IsolationLevel::Process,
            Some("container") => IsolationLevel::Container,
            Some("wasm") => IsolationLevel::Wasm,
            _ => IsolationLevel::Coroutine,
        };

        // 后台模式仅顶层 Agent 允许，子 Agent 必须同步等待（避免无限后台链）
        let background = is_top_level && args["background"].as_bool().unwrap_or(false);

        let top_schema = match coerce_output_schema(args.get("output_schema")) {
            Ok(s) => s,
            Err(e) => return Err(json!({"error": format!("invalid output_schema: {e}")})),
        };

        // 批量委派：优先解析 tasks 数组；否则退化到单 goal
        let tasks = match args["tasks"].as_array() {
            Some(arr) if !arr.is_empty() => {
                let mut parsed = Vec::with_capacity(arr.len());
                for item in arr {
                    let goal = item["goal"]
                        .as_str()
                        .ok_or_else(|| json!({ "error": "each task requires 'goal'" }))?
                        .to_string();
                    let context = item["context"].as_str().unwrap_or("").to_string();
                    let output_schema =
                        coerce_output_schema(item.get("output_schema")).unwrap_or_default();
                    parsed.push(SpawnTask {
                        goal,
                        context,
                        output_schema,
                    });
                }
                parsed
            }
            _ => {
                let goal = args["goal"]
                    .as_str()
                    .ok_or_else(|| json!({ "error": "'goal' is required for spawn_agent" }))?
                    .to_string();
                let context = args["context"].as_str().unwrap_or("").to_string();
                vec![SpawnTask {
                    goal,
                    context,
                    output_schema: top_schema,
                }]
            }
        };

        Ok(Self {
            action,
            toolsets,
            isolation,
            background,
            tasks,
        })
    }
}