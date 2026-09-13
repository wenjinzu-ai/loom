//! 委派任务的结构化输出 schema 验证
//!
//! - 每个任务可附带 `output_schema`（JSON Schema 对象）
//! - 子 Agent 的 context 追加 OUTPUT CONTRACT 块
//! - 父 Agent 验证子 Agent 最终响应是否符合 schema
//! - 验证失败时进行**单次有界重试**（仅回传错误，不重复粘贴 schema）
//! - `MAX_SCHEMA_RETRIES = 1`

use serde_json::{Map, Value};

/// 最大 schema 验证重试次数（固定为 1：更多重试会让模型丢弃原本正确的字段）
pub const MAX_SCHEMA_RETRIES: usize = 1;

/// 将原始 output_schema 强制转换为可用的 JSON Schema 对象
///
/// 返回 `(schema, None)` 可用，`(None, error)` 不可用；`None` 输入返回 `(None, None)`（无 schema）。
pub fn coerce_output_schema(raw: Option<&Value>) -> Result<Option<Map<String, Value>>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    // 模型有时会把 schema 双重编码为 JSON 字符串
    let mut schema_val = raw.clone();
    if let Some(s) = schema_val.as_str() {
        match serde_json::from_str::<Value>(s) {
            Ok(parsed) => schema_val = parsed,
            Err(_) => {
                return Err(
                    "output_schema must be a JSON Schema object, got a non-JSON string."
                        .to_string(),
                )
            }
        }
    }

    let obj = match schema_val.as_object() {
        Some(o) => o.clone(),
        None => {
            return Err(format!(
                "output_schema must be a JSON Schema object, got {}.",
                schema_val
            ))
        }
    };

    // 元验证：检查 JSON Schema 的基本结构（type/properties 等）
    if !is_valid_json_schema(&obj) {
        return Err("output_schema is not a valid JSON Schema.".to_string());
    }

    Ok(Some(obj))
}

/// 基本的 JSON Schema 结构校验（不依赖外部 jsonschema crate）
fn is_valid_json_schema(schema: &Map<String, Value>) -> bool {
    // 必须有 type 字段或 $ref 或 properties
    if schema.contains_key("type")
        || schema.contains_key("$ref")
        || schema.contains_key("properties")
    {
        return true;
    }
    // 允许 anyOf / allOf / oneOf
    schema.contains_key("anyOf") || schema.contains_key("allOf") || schema.contains_key("oneOf")
}

/// 将 OUTPUT CONTRACT 块追加到子 Agent 的 context
pub fn append_output_contract(context: &str, schema: &Map<String, Value>) -> String {
    let schema_text = serde_json::to_string_pretty(schema)
        .unwrap_or_else(|_| Value::Object(schema.clone()).to_string());
    let block = format!(
        "OUTPUT CONTRACT (machine-validated):\n\
         Your FINAL response must be a single JSON object that validates \
         against this JSON Schema. No prose before or after the JSON; a \
         ```json code fence is acceptable but not required.\n\
         {schema_text}"
    );
    let base = context.trim_end();
    if base.is_empty() {
        block
    } else {
        format!("{base}\n\n{block}")
    }
}

/// 从文本中提取 JSON 候选（去除 markdown 围栏和周围散文）
pub fn extract_json_candidate(text: &str) -> String {
    let raw = text.trim();

    // 处理 ```json ... ``` 或 ``` ... ``` 围栏
    if raw.starts_with("```") {
        let after_first_line = raw.split_once('\n').map(|(_, rest)| rest).unwrap_or(raw);
        let mut inner = after_first_line;
        if inner.trim_end().ends_with("```") {
            inner = inner.trim_end().trim_end_matches("```");
        }
        let inner = inner.trim();
        let inner = if inner.to_lowercase().starts_with("json\n") {
            inner
                .split_once('\n')
                .map(|(_, rest)| rest)
                .unwrap_or(inner)
        } else {
            inner
        };
        return inner.to_string();
    }

    // 提取最外层的 {...} 或 [...]
    for (opener, closer) in [('{', '}'), ('[', ']')] {
        if raw.starts_with(opener) {
            return raw.to_string();
        }
        if let (Some(start), Some(end)) = (raw.find(opener), raw.rfind(closer)) {
            if end > start {
                return raw[start..=end].to_string();
            }
        }
    }
    raw.to_string()
}

/// 验证文本响应是否符合 schema
///
/// 返回 `(valid, errors)`，errors 适合直接用于重试提示。
pub fn validate_output(text: &str, schema: &Map<String, Value>) -> (bool, Vec<String>) {
    let candidate = extract_json_candidate(text);
    if candidate.trim().is_empty() {
        return (
            false,
            vec!["Response was empty — expected a JSON object matching the schema.".to_string()],
        );
    }

    let parsed: Value = match serde_json::from_str(&candidate) {
        Ok(v) => v,
        Err(e) => {
            return (false, vec![format!("Response is not valid JSON: {e}")]);
        }
    };

    let errors = validate_against_schema(&parsed, schema, "$");
    (errors.is_empty(), errors)
}

/// 递归验证 JSON 值是否符合 schema（轻量级实现，支持常见关键字）
fn validate_against_schema(value: &Value, schema: &Map<String, Value>, path: &str) -> Vec<String> {
    let mut errors = Vec::new();

    // type 校验
    if let Some(type_val) = schema.get("type") {
        let expected_types: Vec<&str> = match type_val {
            Value::String(s) => vec![s.as_str()],
            Value::Array(arr) => arr.iter().filter_map(|v| v.as_str()).collect(),
            _ => vec![],
        };
        if !expected_types.is_empty() && !matches_type(value, &expected_types) {
            errors.push(format!(
                "{path}: expected type {}, got {}",
                expected_types.join(" or "),
                json_type_name(value)
            ));
        }
    }

    // enum 校验
    if let Some(Value::Array(enum_vals)) = schema.get("enum") {
        if !enum_vals.iter().any(|v| v == value) {
            errors.push(format!(
                "{path}: value is not one of the allowed enum values"
            ));
        }
    }

    // 对象 properties 校验
    if let (Some(Value::Object(props)), Some(obj)) = (schema.get("properties"), value.as_object()) {
        // required 校验
        if let Some(Value::Array(required)) = schema.get("required") {
            for req in required {
                if let Some(req_str) = req.as_str() {
                    if !obj.contains_key(req_str) {
                        errors.push(format!("{path}.{req_str}: is required"));
                    }
                }
            }
        }
        // 递归校验每个属性
        for (key, prop_schema) in props {
            if let Some(prop_value) = obj.get(key) {
                if let Some(prop_obj) = prop_schema.as_object() {
                    let child_path = format!("{path}.{key}");
                    errors.extend(validate_against_schema(prop_value, prop_obj, &child_path));
                }
            }
        }
    }

    // 数组 items 校验
    if let (Some(items_schema), Some(arr)) = (schema.get("items"), value.as_array()) {
        if let Some(items_obj) = items_schema.as_object() {
            for (i, item) in arr.iter().enumerate() {
                let child_path = format!("{path}[{i}]");
                errors.extend(validate_against_schema(item, items_obj, &child_path));
            }
        }
    }

    errors
}

/// 判断值是否匹配期望的类型集合
fn matches_type(value: &Value, types: &[&str]) -> bool {
    types.iter().any(|t| match *t {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.is_i64() || value.is_u64(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    })
}

/// 获取 JSON 值的类型名称
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// 构建单次有界重试消息（仅包含错误，不重复粘贴 schema）
///
/// schema deliberately NOT re-pasted，避免模型在重试时丢弃原本正确的字段。
pub fn build_retry_message(errors: &[String]) -> String {
    let error_block = errors
        .iter()
        .map(|e| format!("- {e}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Your previous final response was rejected by the output contract \
         validator. Validation errors:\n{error_block}\n\n\
         Reply with ONLY the corrected JSON object matching the OUTPUT \
         CONTRACT schema from your task context. No prose, no explanations."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_coerce_none() {
        assert!(coerce_output_schema(None).unwrap().is_none());
    }

    #[test]
    fn test_coerce_valid_object() {
        let raw = json!({"type": "object", "properties": {"name": {"type": "string"}}});
        let result = coerce_output_schema(Some(&raw)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_coerce_string_encoded() {
        let raw = Value::String(r#"{"type":"object"}"#.to_string());
        let result = coerce_output_schema(Some(&raw)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_coerce_invalid_type() {
        let raw = json!(123);
        assert!(coerce_output_schema(Some(&raw)).is_err());
    }

    #[test]
    fn test_extract_json_direct() {
        let text = r#"{"name": "test"}"#;
        assert_eq!(extract_json_candidate(text), r#"{"name": "test"}"#);
    }

    #[test]
    fn test_extract_json_code_fence() {
        let text = "Here is the result:\n```json\n{\"status\": \"ok\"}\n```";
        assert_eq!(extract_json_candidate(text), r#"{"status": "ok"}"#);
    }

    #[test]
    fn test_validate_output_valid() {
        let schema = json!({"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]});
        let schema_obj = schema.as_object().unwrap().clone();
        let text = r#"{"name": "test"}"#;
        let (valid, errors) = validate_output(text, &schema_obj);
        assert!(valid, "errors: {errors:?}");
    }

    #[test]
    fn test_validate_output_missing_required() {
        let schema = json!({"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]});
        let schema_obj = schema.as_object().unwrap().clone();
        let text = r#"{"age": 30}"#;
        let (valid, errors) = validate_output(text, &schema_obj);
        assert!(!valid);
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_validate_output_wrong_type() {
        let schema = json!({"type": "object", "properties": {"age": {"type": "integer"}}});
        let schema_obj = schema.as_object().unwrap().clone();
        let text = r#"{"age": "thirty"}"#;
        let (valid, _errors) = validate_output(text, &schema_obj);
        assert!(!valid);
    }

    #[test]
    fn test_build_retry_message() {
        let errors = vec!["$.name: is required".to_string()];
        let msg = build_retry_message(&errors);
        assert!(msg.contains("$.name: is required"));
        assert!(msg.contains("OUTPUT CONTRACT"));
    }
}