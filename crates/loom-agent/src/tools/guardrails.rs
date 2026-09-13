use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};

use loom_llm::ToolCall;

use crate::config::GuardrailConfig;

/// 工具调用防护裁决（tool_guardrails 的 GuardrailVerdict）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardrailVerdict {
    /// 通过
    Pass,
    /// 警告但继续（返回给模型的提示信息）
    Warn(String),
    /// 终止 turn（guardrail halt）
    Halt(String),
}

/// 单次工具调用指纹 (tool_name, args_hash)
type CallFingerprint = (String, u64);

/// 幂等工具集合（IDEMPOTENT_TOOL_NAMES）
/// 这些工具重复调用不会产生副作用，可启用 no-progress 检测
const IDEMPOTENT_TOOLS: &[&str] = &[
    "read_file",
    "list_dir",
    "search_files",
    "web_search",
    "web_extract",
    "http_get",
    "todo_list",
    "skills_list",
    "skill_view",
    "echo",
];

/// 变更工具集合（MUTATING_TOOL_NAMES）
/// 成功调用后重置所有失败计数，表示取得进展
const PROGRESS_TOOLS: &[&str] = &[
    "write_file",
    "patch",
    "terminal",
    "run_shell",
    "execute_code",
    "process_manage",
    "skill_manage",
];

/// 容错工具集合（FAILURE_TOLERANT_TOOL_NAMES）
/// 不同参数的失败不触发连续失败 halt（如终端命令测试失败属正常输出）
const FAILURE_TOLERANT_TOOLS: &[&str] = &["terminal", "run_shell", "execute_code"];

/// 工具调用防护控制器
///
/// 职责（tool_guardrails）：
/// 1. 同一轮内去重：相同 (name, args) 的调用只保留一次
/// 2. 循环检测：滑动窗口内相同指纹重复次数超限 → Halt
/// 3. 连续失败检测：同一工具连续失败超限 → Halt
/// 4. 单轮硬上限：web_search / spawn_agent 调用次数上限
pub struct ToolGuardrailController {
    config: GuardrailConfig,
    /// 滑动窗口：最近 N 次调用指纹
    call_window: VecDeque<CallFingerprint>,
    /// 每个工具的连续失败次数
    consecutive_failures: HashMap<String, u32>,
    /// 本轮 web_search 调用次数
    web_search_count: u32,
    /// 本轮 spawn_agent 调用次数
    subagent_count: u32,
}

impl ToolGuardrailController {
    pub fn new(config: GuardrailConfig) -> Self {
        Self {
            call_window: VecDeque::with_capacity(config.loop_window),
            consecutive_failures: HashMap::new(),
            config,
            web_search_count: 0,
            subagent_count: 0,
        }
    }

    /// 同一轮内去重：保留首次出现的 (name, args) 调用
    pub fn deduplicate(&self, tool_calls: Vec<ToolCall>) -> Vec<ToolCall> {
        if !self.config.deduplicate {
            return tool_calls;
        }
        let mut seen: std::collections::HashSet<CallFingerprint> =
            std::collections::HashSet::new();
        tool_calls
            .into_iter()
            .filter(|tc| {
                let fp = fingerprint(tc);
                seen.insert(fp)
            })
            .collect()
    }

    /// 校验本轮工具调用是否触发循环检测，并将调用记入窗口
    ///
    /// 返回 GuardrailVerdict：
    /// - Pass：未超限
    /// - Warn：接近上限（返回提示给模型）
    /// - Halt：超过 max_duplicate_calls 或 loop cap
    pub fn check_and_record(&mut self, tool_calls: &[ToolCall]) -> GuardrailVerdict {
        for tc in tool_calls {
            // loop cap 检查
            match tc.name.as_str() {
                "web_search" => {
                    self.web_search_count += 1;
                    if self.web_search_count > self.config.max_web_searches {
                        return GuardrailVerdict::Halt(format!(
                            "Guardrail halt: web_search called {} times this turn (max={}). \
                             You have exhausted your web search budget for this turn.",
                            self.web_search_count, self.config.max_web_searches
                        ));
                    }
                }
                "spawn_agent" => {
                    self.subagent_count += 1;
                    if self.subagent_count > self.config.max_subagents {
                        return GuardrailVerdict::Halt(format!(
                            "Guardrail halt: spawn_agent called {} times this turn (max={}). \
                             You have exhausted your sub-agent budget for this turn.",
                            self.subagent_count, self.config.max_subagents
                        ));
                    }
                }
                _ => {}
            }

            // 仅对幂等工具启用"相同参数重复调用"循环检测。
            // 非幂等工具（如 write_file/terminal）重复相同参数可能是有意行为，不视为循环。
            if IDEMPOTENT_TOOLS.contains(&tc.name.as_str()) {
                let fp = fingerprint(tc);
                self.call_window.push_back(fp.clone());
                while self.call_window.len() > self.config.loop_window {
                    self.call_window.pop_front();
                }
                let count = self.call_window.iter().filter(|f| *f == &fp).count() as u32;
                if count >= self.config.max_duplicate_calls {
                    return GuardrailVerdict::Halt(format!(
                        "Guardrail halt: tool '{}' called with identical arguments {} times within the last {} calls. \
                         This looks like an infinite loop. Try a different approach or stop.",
                        tc.name, count, self.config.loop_window
                    ));
                }
                if count + 1 == self.config.max_duplicate_calls {
                    return GuardrailVerdict::Warn(format!(
                        "Warning: tool '{}' has been called with identical arguments {} times. \
                         Repeating again will halt execution. Consider a different approach.",
                        tc.name, count
                    ));
                }
            }
        }
        GuardrailVerdict::Pass
    }

    /// 记录工具执行结果：成功重置连续失败计数，失败则递增
    ///
    /// 连续失败超过 max_consecutive_failures 时返回 Halt。
    ///
    /// 工具语义分类：
    /// - PROGRESS_TOOLS（write_file/patch/terminal 等）成功 → 清空所有失败计数
    /// - FAILURE_TOLERANT_TOOLS（terminal/execute_code 等）失败不 halt
    pub fn record_results(&mut self, results: &[(String, bool)]) -> GuardrailVerdict {
        for (name, is_error) in results {
            if *is_error {
                // 容错工具的失败不计入连续失败（如终端测试失败属正常输出）
                if FAILURE_TOLERANT_TOOLS.contains(&name.as_str()) {
                    continue;
                }
                let count = self
                    .consecutive_failures
                    .entry(name.clone())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                if *count >= self.config.max_consecutive_failures {
                    return GuardrailVerdict::Halt(format!(
                        "Guardrail halt: tool '{}' failed {} consecutive times. \
                         The tool may be unavailable or your arguments are incorrect. \
                         Stop and try an alternative approach.",
                        name, count
                    ));
                }
            } else {
                self.consecutive_failures.remove(name);
                // 变更工具成功 → 清空所有失败计数（表示取得进展）
                if PROGRESS_TOOLS.contains(&name.as_str()) {
                    self.consecutive_failures.clear();
                }
            }
        }
        GuardrailVerdict::Pass
    }

    /// 检查工具名是否在白名单中
    ///
    /// 返回 (valid_calls, invalid_results)：
    /// - valid_calls：存在的工具调用
    /// - invalid_results：不存在工具的错误结果（直接返回给模型）
    pub fn validate_tool_names(
        &self,
        tool_calls: Vec<ToolCall>,
        valid_names: &std::collections::HashSet<String>,
    ) -> (Vec<ToolCall>, Vec<(String, String)>) {
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        for tc in tool_calls {
            if valid_names.contains(&tc.name) {
                valid.push(tc);
            } else {
                invalid.push((
                    tc.id.clone(),
                    format!(
                        "Tool '{}' not found. Available tools: {}",
                        tc.name,
                        valid_names
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        }
        (valid, invalid)
    }
}

/// 计算工具调用指纹：(name, args_hash)
fn fingerprint(tc: &ToolCall) -> CallFingerprint {
    let args_str = serde_json::to_string(&tc.arguments).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    args_str.hash(&mut hasher);
    (tc.name.clone(), hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: format!("call_{}", name),
            name: name.to_string(),
            arguments: args,
        }
    }

    #[test]
    fn test_deduplicate() {
        let config = GuardrailConfig::default();
        let ctrl = ToolGuardrailController::new(config);
        let calls = vec![
            make_call("read", json!({"path": "a.txt"})),
            make_call("read", json!({"path": "a.txt"})),
            make_call("read", json!({"path": "b.txt"})),
        ];
        let result = ctrl.deduplicate(calls);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].arguments, json!({"path": "a.txt"}));
        assert_eq!(result[1].arguments, json!({"path": "b.txt"}));
    }

    #[test]
    fn test_loop_detection_halt() {
        let config = GuardrailConfig {
            max_duplicate_calls: 3,
            loop_window: 10,
            ..Default::default()
        };
        let mut ctrl = ToolGuardrailController::new(config);
        // 使用幂等工具 read_file（仅幂等工具启用重复调用循环检测）
        let call = make_call("read_file", json!({"path": "a.txt"}));
        // 第 1 次：Pass
        let verdict = ctrl.check_and_record(&[call.clone()]);
        assert_eq!(verdict, GuardrailVerdict::Pass);
        // 第 2 次：接近上限，Warn
        let verdict = ctrl.check_and_record(&[call.clone()]);
        assert!(matches!(verdict, GuardrailVerdict::Warn(_)));
        // 第 3 次：达到上限，Halt
        let verdict = ctrl.check_and_record(&[call.clone()]);
        assert!(matches!(verdict, GuardrailVerdict::Halt(_)));
    }

    #[test]
    fn test_consecutive_failures_halt() {
        let config = GuardrailConfig {
            max_consecutive_failures: 3,
            ..Default::default()
        };
        let mut ctrl = ToolGuardrailController::new(config);
        for _ in 0..2 {
            let verdict = ctrl.record_results(&[("read".to_string(), true)]);
            assert_eq!(verdict, GuardrailVerdict::Pass);
        }
        let verdict = ctrl.record_results(&[("read".to_string(), true)]);
        assert!(matches!(verdict, GuardrailVerdict::Halt(_)));
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = GuardrailConfig {
            max_consecutive_failures: 3,
            ..Default::default()
        };
        let mut ctrl = ToolGuardrailController::new(config);
        ctrl.record_results(&[("read".to_string(), true)]);
        ctrl.record_results(&[("read".to_string(), true)]);
        ctrl.record_results(&[("read".to_string(), false)]);
        ctrl.record_results(&[("read".to_string(), true)]);
        ctrl.record_results(&[("read".to_string(), true)]);
        let verdict = ctrl.record_results(&[("read".to_string(), true)]);
        assert!(matches!(verdict, GuardrailVerdict::Halt(_)));
    }

    #[test]
    fn test_validate_tool_names() {
        let config = GuardrailConfig::default();
        let ctrl = ToolGuardrailController::new(config);
        let calls = vec![
            make_call("read", json!({})),
            make_call("nonexistent", json!({})),
        ];
        let mut valid_names = std::collections::HashSet::new();
        valid_names.insert("read".to_string());
        let (valid, invalid) = ctrl.validate_tool_names(calls, &valid_names);
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].name, "read");
        assert_eq!(invalid.len(), 1);
        assert!(invalid[0].1.contains("nonexistent"));
    }
}