use loom_core::ActivitySummary;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

use crate::result::SubagentResult;

/// 后台任务持久化 namespace
pub const PERSISTENCE_NAMESPACE: &str = "async_delegations";

/// 单次投递的最大重试次数（参考 _MAX_DELIVERY_ATTEMPTS）
pub const MAX_DELIVERY_ATTEMPTS: u32 = 8;

/// 投递认领的 TTL（秒），参考 `delivery_claimed_at < now - 300`。
/// 超过此时间未确认的 claim 视为失效，恢复时重置为 Pending。
pub(crate) const CLAIM_TTL_SECS: f64 = 300.0;

/// 心跳采样间隔（秒），参考 _HEARTBEAT_INTERVAL
pub(crate) const HEARTBEAT_INTERVAL_SECS: u64 = 30;
/// 空闲态（未执行工具）陈旧阈值：连续 N 次采样无进展则判定卡死
/// 参考 _HEARTBEAT_STALE_CYCLES_IDLE（默认 15，15×30s=450s）
pub(crate) const STALE_CYCLES_IDLE: u32 = 15;
/// 工具执行中陈旧阈值：工具执行允许更长时间无输出
/// 参考 _HEARTBEAT_STALE_CYCLES_IN_TOOL（默认 40，40×30s=1200s）
pub(crate) const STALE_CYCLES_IN_TOOL: u32 = 40;

/// 后台任务状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundTaskStatus {
    /// 运行中
    Running,
    /// 正在完成（已收到结果，正在持久化；崩溃恢复时视为 Unknown）
    ///
    /// 参考 `finalizing` 状态：防止完成事件已广播但持久化未写入时进程崩溃导致结果丢失。
    Finalizing,
    /// 已完成
    Completed,
    /// 失败
    Failed,
    /// 卡死（被陈旧检测终止）
    Stale,
    /// 未知（进程重启后未确认）
    Unknown,
}

impl BackgroundTaskStatus {
    /// 判断是否可转换到目标状态
    ///
    /// 合法转换：
    /// - Running → Finalizing（开始完成流程）
    /// - Running → Stale（陈旧检测终止）
    /// - Finalizing → Completed / Failed（写入最终结果）
    /// - Running / Finalizing → Unknown（崩溃恢复时标记）
    pub fn can_transition_to(&self, next: &BackgroundTaskStatus) -> bool {
        use BackgroundTaskStatus::*;
        matches!(
            (self, next),
            (Running, Finalizing)
                | (Running, Stale)
                | (Running, Unknown)
                | (Finalizing, Completed)
                | (Finalizing, Failed)
                | (Finalizing, Unknown)
        )
    }

    /// 判断是否为终态（不再变化）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            BackgroundTaskStatus::Completed
                | BackgroundTaskStatus::Failed
                | BackgroundTaskStatus::Stale
                | BackgroundTaskStatus::Unknown
        )
    }
}

/// 投递状态（exactly-once 投递状态机，参考 delivery_state）
///
/// Pending → Claimed → Delivered（成功）/ Dropped（重试耗尽）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    /// 待投递（已完成，等待被会话认领）
    #[default]
    Pending,
    /// 已被会话认领（正在注入历史）
    Claimed,
    /// 已成功投递
    Delivered,
    /// 投递失败（重试耗尽，放弃）
    Dropped,
}

/// 会话唤醒器（推模式投递的回调）
///
/// 当后台任务完成且需要唤醒父会话处理结果时调用。
/// HTTP 模式下实现为 self-POST `/chat`，CLI 模式下可为 no-op。
pub trait SessionWaker: Send + Sync {
    /// 唤醒指定会话（异步触发，不阻塞）
    fn wake(&self, session_id: &str);
}

/// 后台子 Agent 任务记录
///
/// 参考 async_delegations 表结构：
/// - delegation_id: 批次 ID，同一批 spawn_background 的多个任务共享
/// - child_agent_id: 子 Agent ID
/// - status: 任务状态
/// - result: 完成后的结果
/// - live_transcript: 运行时活动快照（P2 实时转录）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundTask {
    /// 批次委派 ID（同一 spawn_background 调用的多个任务共享）
    pub delegation_id: Uuid,
    /// 子 Agent ID
    pub child_agent_id: Uuid,
    /// 任务目标
    pub goal: String,
    /// 任务状态
    pub status: BackgroundTaskStatus,
    /// 父 Agent ID
    pub parent_agent_id: Uuid,
    /// 会话 ID（用于跨轮次回注）
    pub session_id: Option<String>,
    /// 原始会话 ID（结果应投递到的会话，优先于 session_id 路由）
    /// 参考 origin_session_id：即使子 Agent 在子会话中运行，结果也回到发起委派的会话
    #[serde(default)]
    pub origin_session_id: Option<String>,
    /// 派发时间戳（秒）
    pub dispatched_at: f64,
    /// 完成时间戳（秒）
    pub completed_at: Option<f64>,
    /// 完成后的结果
    pub result: Option<SubagentResult>,
    /// 运行时活动快照列表（P2 实时转录，按时间顺序）
    #[serde(default)]
    pub live_transcript: Vec<ActivitySnapshot>,
    /// 投递状态（exactly-once 投递）
    #[serde(default)]
    pub delivery_state: DeliveryState,
    /// 投递尝试次数（超过 _MAX_DELIVERY_ATTEMPTS 则标记 Dropped）
    #[serde(default)]
    pub delivery_attempts: u32,
    /// 投递认领令牌（consumer:pid:uuid，跨进程互斥 exactly-once 投递）
    ///
    /// 参考 `delivery_claim`：claim 时写入，complete/release 时清除。
    #[serde(default)]
    pub delivery_claim: Option<String>,
    /// 投递认领时间戳（秒），用于 TTL 超时判断
    #[serde(default)]
    pub delivery_claimed_at: Option<f64>,
    /// 派发该任务的进程实例 ID（UUID，崩溃恢复时判断归属）
    ///
    /// 参考 `owner_pid` + `owner_started_at`：恢复时仅当 owner_instance_id
    /// 与当前进程不同时，才将 Running 任务标记为 Unknown。
    #[serde(default)]
    pub owner_instance_id: Option<String>,
    /// 租户 ID（多租户隔离，持久化到 delegation_tasks 表）
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（多租户隔离，持久化到 delegation_tasks 表）
    #[serde(default)]
    pub user_id: Option<String>,
}

/// 子 Agent 活动快照（用于实时转录）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivitySnapshot {
    /// 采样时间戳（秒）
    pub timestamp: f64,
    /// 已完成迭代数
    pub iterations: u64,
    /// API 调用次数
    pub api_call_count: u64,
    /// 当前正在执行的工具
    pub current_tool: Option<String>,
    /// 最后活动时间
    pub last_activity_ts: Option<String>,
}

impl ActivitySnapshot {
    pub(crate) fn from_summary(summary: &ActivitySummary) -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            iterations: summary.iterations,
            api_call_count: summary.api_call_count,
            current_tool: summary.current_tool.clone(),
            last_activity_ts: summary.last_activity_ts.map(|t| t.to_rfc3339()),
        }
    }
}