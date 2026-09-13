//! 工具规格与 ToolSet trait

use async_trait::async_trait;
use futures::stream::BoxStream;
use loom_core::{Result, ToolContext};
use serde_json::Value;

/// 单个工具的规格描述
#[derive(Debug, Clone)]
pub struct ToolSpec {
    /// 工具名（在 ToolSet 内唯一）
    pub name: String,
    /// 人类可读描述
    pub description: String,
    /// JSON Schema 输入
    pub input_schema: Value,
    /// JSON Schema 输出
    pub output_schema: Value,
    /// 是否支持流式输出
    pub streaming: bool,
    /// 标签（分组用）
    pub tags: Vec<String>,
}

/// 工具集：一组相关工具的集合
///
/// 例如 FilesystemToolSet 包含 read_file / write_file / list_dir 等。
#[async_trait]
pub trait ToolSet: Send + Sync {
    /// 工具集名
    fn name(&self) -> &str;

    /// 该工具集提供的所有工具规格
    fn tools(&self) -> Vec<ToolSpec>;

    /// 执行某个工具（同步）
    async fn execute(&self, tool_name: &str, args: Value, ctx: &ToolContext) -> Result<Value>;

    /// 流式执行某个工具
    async fn execute_stream(
        &self,
        tool_name: &str,
        args: Value,
        ctx: &ToolContext,
    ) -> Result<BoxStream<'static, Result<Value>>>;
}