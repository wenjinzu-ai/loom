//! 上下文压缩配置（动态参数）
//!
//! 核心参数：
//! - 阈值基于模型 context_length 动态计算，而非固定硬编码
//! - 尾部预算 = max(floor, min(cap, ctx_len * 0.025))
//! - 摘要目标 = summary_target_ratio * context_length
//! - 支持 lean / legacy 两种尾部模式

/// 尾部保护模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TailMode {
    /// 小尾部 + 摘要中包含会话日志（默认）
    #[default]
    Lean,
    /// 传统大尾部（0.20 * context_length）
    Legacy,
}

/// 上下文压缩配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressionConfig {
    /// 触发压缩的阈值占 context_length 的百分比（0.0 - 1.0）
    #[serde(default = "default_threshold_percent")]
    pub threshold_percent: f32,

    /// 头部保护的消息数（system + 前 N 条非 system 消息）
    #[serde(default = "default_protect_first_n")]
    pub protect_first_n: usize,

    /// 尾部保护的消息数（legacy 模式下的固定尾部）
    #[serde(default = "default_protect_last_n")]
    pub protect_last_n: usize,

    /// 摘要目标占 context_length 的比例
    #[serde(default = "default_summary_target_ratio")]
    pub summary_target_ratio: f32,

    /// 尾部模式
    #[serde(default)]
    pub tail_mode: TailMode,

    /// Lean 尾部预算下限（tokens）
    #[serde(default = "default_lean_tail_floor")]
    pub lean_tail_floor: usize,

    /// Lean 尾部预算上限（tokens）
    #[serde(default = "default_lean_tail_cap")]
    pub lean_tail_cap: usize,

    /// 模型的 context_length（0 表示未知，使用 fallback_threshold）
    #[serde(default)]
    pub context_length: usize,

    /// context_length 未知时使用的阈值（tokens），None 时用 12000
    #[serde(default)]
    pub fallback_threshold_tokens: Option<usize>,

    /// 摘要失败冷却时间（秒）
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,

    /// 单条工具输出最大字符数（超出截断）
    #[serde(default = "default_max_tool_output_chars")]
    pub max_tool_output_chars: usize,

    /// 主动修剪工具输出的 token 阈值（0 表示禁用）
    #[serde(default)]
    pub proactive_prune_tokens: usize,

    /// 主动修剪的最小工具输出字符数
    #[serde(default = "default_proactive_prune_min_chars")]
    pub proactive_prune_min_result_chars: usize,

    /// 是否在摘要失败时中止（true=保留原文，false=用 fallback 摘要）
    #[serde(default)]
    pub abort_on_summary_failure: bool,

    /// 最小尾部用户消息数（压缩必须至少保留这么多 user 消息在尾部）
    #[serde(default = "default_min_tail_user_messages")]
    pub min_tail_user_messages: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            threshold_percent: default_threshold_percent(),
            protect_first_n: default_protect_first_n(),
            protect_last_n: default_protect_last_n(),
            summary_target_ratio: default_summary_target_ratio(),
            tail_mode: TailMode::default(),
            lean_tail_floor: default_lean_tail_floor(),
            lean_tail_cap: default_lean_tail_cap(),
            context_length: 0,
            fallback_threshold_tokens: None,
            cooldown_seconds: default_cooldown_seconds(),
            max_tool_output_chars: default_max_tool_output_chars(),
            proactive_prune_tokens: 0,
            proactive_prune_min_result_chars: default_proactive_prune_min_chars(),
            abort_on_summary_failure: false,
            min_tail_user_messages: default_min_tail_user_messages(),
        }
    }
}

fn default_threshold_percent() -> f32 {
    0.50
}
fn default_protect_first_n() -> usize {
    3
}
fn default_protect_last_n() -> usize {
    20
}
fn default_summary_target_ratio() -> f32 {
    0.20
}
fn default_lean_tail_floor() -> usize {
    500
}
fn default_lean_tail_cap() -> usize {
    4000
}
fn default_cooldown_seconds() -> u64 {
    600
}
fn default_max_tool_output_chars() -> usize {
    4000
}
fn default_proactive_prune_min_chars() -> usize {
    8000
}
fn default_min_tail_user_messages() -> usize {
    1
}

impl CompressionConfig {
    /// 校验配置参数的合理性
    pub fn validate(&self) -> Result<(), String> {
        if self.threshold_percent <= 0.0 || self.threshold_percent > 1.0 {
            return Err(format!(
                "threshold_percent must be in (0.0, 1.0], got {}",
                self.threshold_percent
            ));
        }
        if self.summary_target_ratio <= 0.0 || self.summary_target_ratio > 0.5 {
            return Err(format!(
                "summary_target_ratio must be in (0.0, 0.5], got {}",
                self.summary_target_ratio
            ));
        }
        if self.lean_tail_floor > self.lean_tail_cap {
            return Err(format!(
                "lean_tail_floor ({}) must be <= lean_tail_cap ({})",
                self.lean_tail_floor, self.lean_tail_cap
            ));
        }
        if self.context_length > 0 && self.threshold_tokens() >= self.context_length {
            return Err(format!(
                "threshold ({}) must be < context_length ({})",
                self.threshold_tokens(),
                self.context_length
            ));
        }
        Ok(())
    }

    /// 计算压缩阈值 tokens
    ///
    /// threshold = context_length * threshold_percent
    pub fn threshold_tokens(&self) -> usize {
        if self.context_length == 0 {
            return self.fallback_threshold_tokens.unwrap_or(12_000);
        }
        let computed = (self.context_length as f32 * self.threshold_percent).round() as usize;
        computed.max(1)
    }

    /// 计算尾部 token 预算
    ///
    /// - Lean: max(floor, min(cap, ctx_len * 0.025))
    /// - Legacy: ctx_len * 0.20
    pub fn tail_token_budget(&self) -> usize {
        match self.tail_mode {
            TailMode::Lean => {
                if self.context_length == 0 {
                    return self.lean_tail_floor;
                }
                let pct = (self.context_length as f32 * 0.025).round() as usize;
                self.lean_tail_floor.max(self.lean_tail_cap.min(pct))
            }
            TailMode::Legacy => {
                if self.context_length == 0 {
                    return 6000;
                }
                (self.context_length as f32 * 0.20).round() as usize
            }
        }
    }

    /// 计算摘要最大 token 数
    pub fn max_summary_tokens(&self) -> usize {
        if self.context_length == 0 {
            return 2000;
        }
        (self.context_length as f32 * self.summary_target_ratio).round() as usize
    }

    /// 计算摘要输入最大字符数
    ///
    /// 根据 context_length 动态调整，避免小模型放不下或大模型浪费容量。
    pub fn summary_input_max_chars(&self) -> usize {
        super::summary::summary_input_max_chars(self.context_length)
    }
}