//! HTTP 处理器：chat 流、Agent 生命周期、能力列表

use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use futures::Stream;
use loom_agent::hitl::ResumeCommand;
use loom_agent::result::ProgressEvent;
use loom_core::{
    AgentHandle, AgentLaunchRequest, AgentMessage, CapabilitySpec, IsolationLevel, MessageContent,
    MemoryScope,
};
use loom_llm::{ChatMessage, Role};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// 父中断传播守护：stream 被 drop（客户端断开）时取消令牌，
/// 触发正在执行的同步子 Agent 停止。
struct CancelGuard(Option<tokio_util::sync::CancellationToken>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Some(token) = self.0.take() {
            token.cancel();
        }
    }
}

// ---------- 请求/响应类型 ----------

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub message: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub isolation: Option<String>,
    /// 租户/组织/工作空间 ID（可选），用于记忆隔离
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（可选），用于记忆隔离
    #[serde(default)]
    pub user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LaunchRequest {
    pub goal: String,
    #[serde(default)]
    pub context: String,
    #[serde(default)]
    pub isolation: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct AgentView {
    pub agent_id: Uuid,
    pub state: String,
    pub isolation: String,
    pub goal: String,
    pub parent_agent_id: Option<Uuid>,
    pub created_at: String,
}

impl From<AgentHandle> for AgentView {
    fn from(h: AgentHandle) -> Self {
        Self {
            agent_id: h.agent_id,
            state: h.state.to_string(),
            isolation: h.isolation.to_string(),
            goal: h.goal,
            parent_agent_id: h.parent_agent_id,
            created_at: h.created_at.to_rfc3339(),
        }
    }
}

// ---------- Chat 流式接口 ----------

/// POST /chat — 运行 Agent 对话循环并以 SSE 流式返回事件
///
/// 事件类型：
/// - `thinking`  Agent 开始思考（调用 LLM）
/// - `message`   Agent 产生的文本
/// - `done`      对话结束（成功或失败都会发送）
///
/// 支持多轮会话：若请求携带 `session_id`，则加载该会话历史并继续对话；
/// 否则创建新会话并在响应中返回 `session_id`。
pub async fn chat_stream(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatRequest>,
) -> impl IntoResponse {
    let agent_loop = state.agent_loop.clone();
    let sessions = state.sessions.clone();
    let goal = req.message.clone();

    // 确定 session_id：使用请求中的，或生成新的
    let session_id = req
        .session_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // 构建记忆作用域（tenant_id + user_id），用于记忆隔离
    let scope = MemoryScope::new(req.tenant_id.clone(), req.user_id.clone());
    let session_scope = scope.clone();

    let agent_id = Uuid::new_v4();

    // 创建父中断传播取消令牌：当 chat 请求被取消时触发同步子 Agent 停止
    let cancel_token = tokio_util::sync::CancellationToken::new();

    // 创建进度事件 channel，用于实时推送工具调用过程
    let (progress_tx, mut progress_rx) =
        tokio::sync::mpsc::unbounded_channel::<loom_agent::result::ProgressEvent>();
    // 克隆 AgentLoop 并设置进度发送器 + 记忆作用域 + 会话 ID + 取消令牌（不影响共享实例）
    let agent_loop_with_progress = (*agent_loop)
        .clone()
        .with_progress_sender(progress_tx)
        .with_scope(scope)
        .with_session_id(session_id.clone())
        .with_cancel_token(cancel_token.clone());

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, axum::BoxError>> + Send>> =
        Box::pin(async_stream::try_stream! {
            // 父中断传播：stream 被 drop 时取消令牌，停止同步子 Agent
            let _cancel_guard = CancelGuard(Some(cancel_token.clone()));

            tracing::debug!(
                "[chat_stream] start: session_id={}, agent_id={}, goal_len={}",
                session_id,
                agent_id,
                goal.len()
            );

            yield Event::default()
                .event("thinking")
                .json_data(serde_json::json!({
                    "goal": goal,
                    "agent_id": agent_id.to_string(),
                    "session_id": session_id,
                }))?;

            // 加载会话历史（若存在）
            let mut history = sessions.get(&session_scope, &session_id).await;
            tracing::debug!(
                "[chat_stream] loaded history: session_id={}, messages={}",
                session_id,
                history.as_ref().map(|h| h.len()).unwrap_or(0)
            );

            // P0-1: 回注已完成的后台子 Agent 结果到会话历史
            // 上一轮后台启动的子 Agent 在此期间完成，结果已存入 BackgroundTaskRegistry。
            // 将其作为 user 消息注入历史，让父 Agent 在本轮感知到后台结果。
            //
            // exactly-once 投递：先 claim（Pending -> Claimed），
            // 成功注入后 complete（删除），失败则 release（回退 Pending 重试）。
            let claimed = agent_loop_with_progress.claim_completed_tasks(&session_id);
            let claimed_ids: Vec<Uuid> = claimed.iter().map(|t| t.child_agent_id).collect();
            let claimed_claim_ids: Vec<Option<String>> =
                claimed.iter().map(|t| t.delivery_claim.clone()).collect();
            let bg_results: Vec<(Uuid, String, loom_agent::result::SubagentResult)> = claimed
                .iter()
                .filter_map(|t| t.result.as_ref().map(|r| (t.child_agent_id, t.goal.clone(), r.clone())))
                .collect();
            if !bg_results.is_empty() {
                tracing::info!(
                    "[chat_stream] injecting {} background result(s) into session {} (claimed {:?})",
                    bg_results.len(),
                    session_id,
                    claimed_ids
                );
                let total_duration_ms: u64 = bg_results.iter().map(|(_, _, r)| r.duration_ms).sum();
                let total_iterations: usize = bg_results.iter().map(|(_, _, r)| r.usage.iterations).sum();
                let total_tool_calls: usize = bg_results.iter().map(|(_, _, r)| r.usage.tool_calls).sum();

                let injection_lines: Vec<String> = bg_results
                    .iter()
                    .map(|(child_id, child_goal, result)| {
                        let status = if result.success { "completed" } else { "failed" };
                        let detail = if result.success {
                            result.summary.clone()
                        } else {
                            format!("error: {}", result.error_message.clone().unwrap_or_default())
                        };
                        format!(
                            "[background sub-agent {} ({}) {}]\n{}",
                            child_id, child_goal, status, detail
                        )
                    })
                    .collect();
                let injection_text = format!(
                    "{}\n\n---\nAggregated cost: {}ms duration, {} iterations, {} tool calls across {} sub-agent(s).",
                    injection_lines.join("\n\n"),
                    total_duration_ms,
                    total_iterations,
                    total_tool_calls,
                    bg_results.len()
                );
                yield Event::default()
                    .event("background_results")
                    .json_data(serde_json::json!({
                        "count": bg_results.len(),
                        "results": bg_results.iter().map(|(id, g, r)| {
                            serde_json::json!({
                                "agent_id": id.to_string(),
                                "goal": g,
                                "success": r.success,
                            })
                        }).collect::<Vec<_>>(),
                        "cost_aggregation": {
                            "total_duration_ms": total_duration_ms,
                            "total_iterations": total_iterations,
                            "total_tool_calls": total_tool_calls,
                        }
                    }))?;

                let injection_msg = ChatMessage::user(format!(
                    "Previously dispatched background sub-agent(s) have finished. Here are their results:\n\n{}",
                    injection_text
                ));
                match history.as_mut() {
                    Some(h) => h.push(injection_msg),
                    None => history = Some(vec![injection_msg]),
                }
            }

            // Wake-only 模式：message 为空时（如 SelfPostWaker 触发的 self-POST），
            // 不调用 LLM。若有后台结果则保存历史并确认投递，直接返回。
            if goal.is_empty() {
                let has_bg = !bg_results.is_empty();
                if has_bg {
                    let saved = if let Some(h) = &history {
                        if let Err(e) = sessions.save(&session_scope, &session_id, h).await {
                            tracing::error!("[chat_stream] wake-only: failed to persist session {}: {}", session_id, e);
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    if saved {
                        agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                    } else {
                        let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                    }
                }
                yield Event::default().event("done");
                return;
            }

            // 并发执行：等待 run_goal 完成，同时转发 progress 事件
            let run_fut = agent_loop_with_progress.run_goal_with_history(
                &goal, "", agent_id, None, 0, history, &[], &[], None,
            );
            tokio::pin!(run_fut);

            loop {
                tokio::select! {
                    // 优先处理进度事件
                    Some(progress) = progress_rx.recv() => {
                        tracing::debug!("[chat_stream] progress event: {:?}", progress);
                        yield Event::default()
                            .event("progress")
                            .json_data(serde_json::to_value(&progress)?)?;
                    }
                    // run_goal 完成
                    result = &mut run_fut => {
                        match result {
                            Ok(outcome) => {
                                match outcome {
                                    loom_agent::AgentRunOutcome::Finished(result) => {
                                        tracing::info!(
                                            "chat finished: iterations={}, tool_calls={}",
                                            result.iterations,
                                            result.tool_calls_made
                                        );
                                        tracing::debug!(
                                            "[chat_stream] finished detail: session_id={}, response_len={}, history_len={}, tool_names={:?}",
                                            session_id,
                                            result.final_response.len(),
                                            result.history.len(),
                                            result.tool_names
                                        );
                                        // 保存更新后的对话历史到后端存储
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &result.history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            tracing::debug!("[chat_stream] session persisted: session_id={}", session_id);
                                            true
                                        };
                                        // exactly-once 投递确认：历史持久化成功后删除后台任务，
                                        // 失败则 release 回退重试，避免结果丢失。
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                                tracing::debug!("[chat_stream] completed background delivery for {:?}", claimed_ids);
                                            } else {
                                                let dropped = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                                tracing::warn!("[chat_stream] released background delivery (history save failed), dropped={:?}", dropped);
                                            }
                                        }
                                        yield Event::default()
                                            .event("message")
                                            .json_data(serde_json::json!({
                                                "text": result.final_response,
                                                "iterations": result.iterations,
                                                "tool_calls_made": result.tool_calls_made,
                                                "tool_trace": result.tool_trace,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                    loom_agent::AgentRunOutcome::Interrupt {
                                        value,
                                        checkpoint_id,
                                        thread_id,
                                        history,
                                    } => {
                                        tracing::info!(
                                            "chat interrupted: checkpoint={}, thread={}",
                                            checkpoint_id,
                                            thread_id
                                        );
                                        tracing::debug!(
                                            "[chat_stream] interrupt detail: session_id={}, value={}, history_len={}",
                                            session_id,
                                            value,
                                            history.len()
                                        );
                                        // 保存中断时的消息历史
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            tracing::debug!("[chat_stream] interrupted session persisted: session_id={}", session_id);
                                            true
                                        };
                                        // exactly-once 投递确认：中断时历史已包含注入结果
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            } else {
                                                let dropped = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                                tracing::warn!("[chat_stream] released background delivery on interrupt (history save failed), dropped={:?}", dropped);
                                            }
                                        }
                                        // 发送 interrupt 事件，携带恢复所需信息
                                        yield Event::default()
                                            .event("interrupt")
                                            .json_data(serde_json::json!({
                                                "value": value,
                                                "checkpoint_id": checkpoint_id,
                                                "thread_id": thread_id,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("chat failed: {e}");
                                tracing::debug!("[chat_stream] error detail: session_id={}, error={:?}", session_id, e);
                                // exactly-once 投递回退：run 失败时 release 允许重试
                                if !claimed_ids.is_empty() {
                                    let dropped = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                    tracing::warn!("[chat_stream] released background delivery on run error, dropped={:?}", dropped);
                                }
                                yield Event::default()
                                    .event("error")
                                    .json_data(serde_json::json!({
                                        "error": e.to_string(),
                                        "session_id": session_id,
                                    }))?;
                                // 错误分支也发送 done，确保客户端能正常结束流
                                yield Event::default().event("done");
                            }
                        }
                        break;
                    }
                }
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ---------- Agent 生命周期接口 ----------

pub async fn list_agents(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tracing::debug!("[list_agents] start");
    match state.lifecycle.list().await {
        Ok(agents) => {
            tracing::debug!("[list_agents] found {} agents", agents.len());
            let views: Vec<AgentView> = agents.into_iter().map(AgentView::from).collect();
            Json(views).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn launch_agent(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LaunchRequest>,
) -> impl IntoResponse {
    tracing::debug!("[launch_agent] goal_len={}", req.goal.len());
    let isolation = req
        .isolation
        .as_deref()
        .and_then(|s| IsolationLevel::from_str(s).ok())
        .unwrap_or_default();

    let launch_req = AgentLaunchRequest {
        goal: req.goal,
        context: req.context,
        isolation,
        ..Default::default()
    };

    match state.lifecycle.launch(launch_req).await {
        Ok(handle) => {
            tracing::debug!("[launch_agent] ok: agent_id={}", handle.agent_id);
            (StatusCode::CREATED, Json(AgentView::from(handle))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn get_agent(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    tracing::debug!("[get_agent] agent_id={}", id);
    match state.lifecycle.get_status(&id).await {
        Ok(handle) => Json(AgentView::from(handle)).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "agent not found").into_response(),
    }
}

pub async fn destroy_agent(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    tracing::debug!("[destroy_agent] agent_id={}", id);
    match state.lifecycle.destroy(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn send_message(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<SendMessageRequest>,
) -> impl IntoResponse {
    tracing::debug!("[send_message] agent_id={}, content_len={}", id, req.content.len());
    // sender 为 Uuid::nil() 表示消息来自系统/用户（非子 Agent）
    let msg = AgentMessage::new(Uuid::nil(), id, MessageContent::Text(req.content));
    match state.lifecycle.send_message(&id, msg).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------- 能力列表 ----------

pub async fn list_capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tracing::debug!("[list_capabilities] start");
    match state.registry.list().await {
        Ok(specs) => {
            tracing::debug!("[list_capabilities] found {} capabilities", specs.len());
            let views: Vec<CapabilityView> = specs.into_iter().map(CapabilityView::from).collect();
            Json(views).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Serialize)]
pub struct CapabilityView {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub description: String,
    pub source: String,
}

impl From<CapabilitySpec> for CapabilityView {
    fn from(s: CapabilitySpec) -> Self {
        Self {
            id: s.id,
            name: s.name,
            kind: format!("{:?}", s.kind),
            description: s.description,
            source: format!("{:?}", s.source),
        }
    }
}

// ---------- 会话管理接口 ----------

/// 会话详情（含消息历史）
#[derive(Debug, Serialize)]
pub struct SessionDetail {
    pub session_id: String,
    pub title: String,
    pub updated_at: u64,
    pub messages: Vec<MessageView>,
    /// 工具调用轨迹（最近 N 条，含工具名、参数、结果预览）
    pub tool_trace: loom_agent::result::ToolTrace,
}

/// 前端友好的消息视图
#[derive(Debug, Serialize)]
pub struct MessageView {
    pub role: String,
    pub content: String,
    /// 工具调用列表（仅 assistant 消息有值）
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCallView>,
}

/// 工具调用视图
#[derive(Debug, Serialize)]
pub struct ToolCallView {
    pub id: String,
    pub name: String,
    pub arguments: Option<Value>,
}

impl From<&ChatMessage> for MessageView {
    fn from(m: &ChatMessage) -> Self {
        let role = match m.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        }
        .to_string();
        let content = m.content.clone().unwrap_or_default();
        let tool_calls = m
            .tool_calls
            .as_ref()
            .map(|tcs| {
                tcs.iter()
                    .map(|tc| ToolCallView {
                        id: tc.id.clone(),
                        name: tc.name.clone(),
                        arguments: if tc.arguments.is_null() {
                            None
                        } else {
                            Some(tc.arguments.clone())
                        },
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            role,
            content,
            tool_calls,
        }
    }
}

/// 重命名请求
#[derive(Debug, Deserialize)]
pub struct RenameRequest {
    pub title: String,
}

/// 会话列表查询参数（租户/用户隔离）
#[derive(Debug, Deserialize)]
pub struct SessionQuery {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
}

impl SessionQuery {
    fn scope(&self) -> MemoryScope {
        MemoryScope::new(self.tenant_id.clone(), self.user_id.clone())
    }
}

/// 当前时间戳（秒）
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// GET /sessions — 列出所有会话（按更新时间倒序）
pub async fn list_sessions(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<SessionQuery>,
) -> impl IntoResponse {
    tracing::debug!("[list_sessions] start");
    let scope = query.scope();
    match state.sessions.list(&scope).await {
        Ok(summaries) => {
            tracing::debug!("[list_sessions] found {} sessions", summaries.len());
            Json(summaries).into_response()
        }
        Err(e) => {
            tracing::error!("list sessions failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// GET /sessions/:id — 获取单个会话的消息历史
pub async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<SessionQuery>,
) -> impl IntoResponse {
    tracing::debug!("[get_session] session_id={}", id);
    let scope = query.scope();
    match state.sessions.get(&scope, &id).await {
        Some(msgs) => {
            tracing::debug!("[get_session] found: session_id={}, messages={}", id, msgs.len());
            let messages: Vec<MessageView> = msgs.iter().map(MessageView::from).collect();
            let tool_trace = loom_agent::result::ToolTrace::from_messages(&msgs, 50);
            let detail = SessionDetail {
                session_id: id,
                title: loom_agent::derive_title(&msgs),
                updated_at: now_secs(),
                messages,
                tool_trace,
            };
            Json(detail).into_response()
        }
        None => (StatusCode::NOT_FOUND, "session not found").into_response(),
    }
}

/// DELETE /sessions/:id — 删除会话
pub async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<SessionQuery>,
) -> impl IntoResponse {
    tracing::debug!("[delete_session] session_id={}", id);
    let scope = query.scope();
    match state.sessions.delete(&scope, &id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "session not found").into_response(),
        Err(e) => {
            tracing::error!("delete session failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// PATCH /sessions/:id — 重命名会话
pub async fn rename_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<SessionQuery>,
    Json(req): Json<RenameRequest>,
) -> impl IntoResponse {
    tracing::debug!("[rename_session] session_id={}, title={}", id, req.title);
    let scope = query.scope();
    match state.sessions.get(&scope, &id).await {
        Some(mut msgs) => {
            // 在消息列表头部插入一条 system 消息作为标题标记
            let title_msg = ChatMessage::system(format!("[session-title] {}", req.title.trim()));
            msgs.insert(0, title_msg);
            if let Err(e) = state.sessions.save(&scope, &id, &msgs).await {
                tracing::error!("rename session failed: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
            }
            StatusCode::OK.into_response()
        }
        None => (StatusCode::NOT_FOUND, "session not found").into_response(),
    }
}

// ---------- Human-in-the-Loop 恢复接口 ----------

/// 恢复执行请求
#[derive(Debug, Deserialize)]
pub struct ResumeRequest {
    /// 检查点 ID（从 interrupt 事件中获取）
    pub checkpoint_id: String,
    /// 线程 ID（从 interrupt 事件中获取）
    pub thread_id: String,
    /// 用户提供的恢复值（成为 interrupt 的返回值）
    pub resume_value: Value,
    /// 租户 ID（多租户隔离）
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（多租户隔离）
    #[serde(default)]
    pub user_id: Option<String>,
}

/// 通用断点续执行请求（不限于 interrupt 场景）
///
/// 与 `ResumeRequest` 的区别：不需要 `resume_value`，直接从 checkpoint 恢复继续执行。
/// 适用于崩溃恢复、手动暂停后继续等场景。
#[derive(Debug, Deserialize)]
pub struct ResumeFromCheckpointRequest {
    /// 线程 ID（会话 ID）
    pub thread_id: String,
    /// 检查点 ID（可选，不传则恢复最新的 checkpoint）
    #[serde(default)]
    pub checkpoint_id: Option<String>,
    /// 租户 ID（多租户隔离）
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 用户 ID（多租户隔离）
    #[serde(default)]
    pub user_id: Option<String>,
}

/// POST /sessions/:id/resume — 从 checkpoint 恢复 Agent 执行
///
/// 当前端收到 `interrupt` 事件后，用户提供输入，调用此接口恢复执行。
/// 响应同样以 SSE 流返回，事件类型与 `/chat` 一致（message / interrupt / done）。
pub async fn resume_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(req): Json<ResumeRequest>,
) -> impl IntoResponse {
    let agent_loop = state.agent_loop.clone();
    let sessions = state.sessions.clone();

    // 构建记忆作用域（tenant_id + user_id），用于记忆/会话隔离
    let scope = MemoryScope::new(req.tenant_id.clone(), req.user_id.clone());
    let session_scope = scope.clone();

    // 创建进度事件 channel
    let (progress_tx, mut progress_rx) =
        tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
    let agent_loop_with_progress = (*agent_loop)
        .clone()
        .with_progress_sender(progress_tx)
        .with_scope(scope)
        .with_session_id(session_id.clone());

    let cmd = ResumeCommand::new(&req.thread_id, &req.checkpoint_id, req.resume_value);

    tracing::debug!(
        "[resume_session] start: session_id={}, thread_id={}, checkpoint_id={}",
        session_id,
        req.thread_id,
        req.checkpoint_id
    );

    // P0: resume 前先认领后台结果，避免在 interrupt 期间完成的子 Agent 结果丢失
    let claimed = agent_loop_with_progress.claim_completed_tasks(&session_id);
    let claimed_ids: Vec<Uuid> = claimed.iter().map(|t| t.child_agent_id).collect();
    let claimed_claim_ids: Vec<Option<String>> =
        claimed.iter().map(|t| t.delivery_claim.clone()).collect();
    let bg_results: Vec<(Uuid, String, loom_agent::result::SubagentResult)> = claimed
        .iter()
        .filter_map(|t| t.result.as_ref().map(|r| (t.child_agent_id, t.goal.clone(), r.clone())))
        .collect();

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, axum::BoxError>> + Send>> =
        Box::pin(async_stream::try_stream! {
            yield Event::default()
                .event("resuming")
                .json_data(serde_json::json!({
                    "checkpoint_id": req.checkpoint_id,
                    "thread_id": req.thread_id,
                    "session_id": session_id,
                }))?;

            if !bg_results.is_empty() {
                yield Event::default()
                    .event("background_results")
                    .json_data(serde_json::json!({
                        "count": bg_results.len(),
                        "results": bg_results.iter().map(|(id, g, r)| {
                            serde_json::json!({
                                "agent_id": id.to_string(),
                                "goal": g,
                                "success": r.success,
                            })
                        }).collect::<Vec<_>>(),
                    }))?;
            }

            let resume_fut = agent_loop_with_progress.resume(cmd);
            tokio::pin!(resume_fut);

            loop {
                tokio::select! {
                    Some(progress) = progress_rx.recv() => {
                        tracing::debug!("[resume_session] progress event: {:?}", progress);
                        yield Event::default()
                            .event("progress")
                            .json_data(serde_json::to_value(&progress)?)?;
                    }
                    result = &mut resume_fut => {
                        match result {
                            Ok(outcome) => {
                                match outcome {
                                    loom_agent::AgentRunOutcome::Finished(result) => {
                                        tracing::info!(
                                            "resume finished: iterations={}, tool_calls={}",
                                            result.iterations,
                                            result.tool_calls_made
                                        );
                                        tracing::debug!(
                                            "[resume_session] finished detail: session_id={}, response_len={}, history_len={}, tool_names={:?}",
                                            session_id,
                                            result.final_response.len(),
                                            result.history.len(),
                                            result.tool_names
                                        );
                                        // P0: 将后台结果注入到恢复后的历史中，避免丢失
                                        let mut history = result.history.clone();
                                        if !bg_results.is_empty() {
                                            let injection_lines: Vec<String> = bg_results
                                                .iter()
                                                .map(|(child_id, child_goal, result)| {
                                                    let status = if result.success { "completed" } else { "failed" };
                                                    let detail = if result.success {
                                                        result.summary.clone()
                                                    } else {
                                                        format!("error: {}", result.error_message.clone().unwrap_or_default())
                                                    };
                                                    format!(
                                                        "[background sub-agent {} ({}) {}]\n{}",
                                                        child_id, child_goal, status, detail
                                                    )
                                                })
                                                .collect();
                                            history.push(ChatMessage::user(format!(
                                                "Previously dispatched background sub-agent(s) have finished. Here are their results:\n\n{}",
                                                injection_lines.join("\n\n")
                                            )));
                                        }
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            tracing::debug!("[resume_session] session persisted after resume: session_id={}", session_id);
                                            true
                                        };
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            } else {
                                                let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            }
                                        }
                                        yield Event::default()
                                            .event("message")
                                            .json_data(serde_json::json!({
                                                "text": result.final_response,
                                                "iterations": result.iterations,
                                                "tool_calls_made": result.tool_calls_made,
                                                "tool_trace": result.tool_trace,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                    loom_agent::AgentRunOutcome::Interrupt {
                                        value,
                                        checkpoint_id,
                                        thread_id,
                                        history,
                                    } => {
                                        tracing::info!(
                                            "resume interrupted again: checkpoint={}",
                                            checkpoint_id
                                        );
                                        tracing::debug!(
                                            "[resume_session] re-interrupt detail: session_id={}, value={}, history_len={}",
                                            session_id,
                                            value,
                                            history.len()
                                        );
                                        // P0: 中断时也注入后台结果到历史
                                        let mut history = history.clone();
                                        if !bg_results.is_empty() {
                                            let injection_lines: Vec<String> = bg_results
                                                .iter()
                                                .map(|(child_id, child_goal, result)| {
                                                    let status = if result.success { "completed" } else { "failed" };
                                                    let detail = if result.success {
                                                        result.summary.clone()
                                                    } else {
                                                        format!("error: {}", result.error_message.clone().unwrap_or_default())
                                                    };
                                                    format!(
                                                        "[background sub-agent {} ({}) {}]\n{}",
                                                        child_id, child_goal, status, detail
                                                    )
                                                })
                                                .collect();
                                            history.push(ChatMessage::user(format!(
                                                "Previously dispatched background sub-agent(s) have finished. Here are their results:\n\n{}",
                                                injection_lines.join("\n\n")
                                            )));
                                        }
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            tracing::debug!("[resume_session] re-interrupted session persisted: session_id={}", session_id);
                                            true
                                        };
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            } else {
                                                let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            }
                                        }
                                        yield Event::default()
                                            .event("interrupt")
                                            .json_data(serde_json::json!({
                                                "value": value,
                                                "checkpoint_id": checkpoint_id,
                                                "thread_id": thread_id,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("resume failed: {e}");
                                tracing::debug!("[resume_session] error detail: session_id={}, error={:?}", session_id, e);
                                // run 失败时 release 后台任务，允许重试
                                if !claimed_ids.is_empty() {
                                    let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                }
                                yield Event::default()
                                    .event("error")
                                    .json_data(serde_json::json!({
                                        "error": e.to_string(),
                                        "session_id": session_id,
                                    }))?;
                                yield Event::default().event("done");
                            }
                        }
                        break;
                    }
                }
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// POST /sessions/:id/resume-from-checkpoint — 通用断点续执行
///
/// 从指定 checkpoint（或最新 checkpoint）恢复 Agent 执行，无需提供 resume_value。
/// 适用于崩溃恢复、手动暂停后继续等场景。
/// 响应以 SSE 流返回，事件类型与 `/chat` 一致（message / interrupt / done）。
pub async fn resume_from_checkpoint_session(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(req): Json<ResumeFromCheckpointRequest>,
) -> impl IntoResponse {
    let agent_loop = state.agent_loop.clone();
    let sessions = state.sessions.clone();

    let scope = MemoryScope::new(req.tenant_id.clone(), req.user_id.clone());
    let session_scope = scope.clone();

    let (progress_tx, mut progress_rx) =
        tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
    let agent_loop_with_progress = (*agent_loop)
        .clone()
        .with_progress_sender(progress_tx)
        .with_scope(scope)
        .with_session_id(session_id.clone());

    tracing::debug!(
        "[resume_from_checkpoint_session] start: session_id={}, thread_id={}, checkpoint_id={:?}",
        session_id,
        req.thread_id,
        req.checkpoint_id
    );

    // P0: resume 前先认领后台结果，避免在 interrupt 期间完成的子 Agent 结果丢失
    let claimed = agent_loop_with_progress.claim_completed_tasks(&session_id);
    let claimed_ids: Vec<Uuid> = claimed.iter().map(|t| t.child_agent_id).collect();
    let claimed_claim_ids: Vec<Option<String>> =
        claimed.iter().map(|t| t.delivery_claim.clone()).collect();
    let bg_results: Vec<(Uuid, String, loom_agent::result::SubagentResult)> = claimed
        .iter()
        .filter_map(|t| t.result.as_ref().map(|r| (t.child_agent_id, t.goal.clone(), r.clone())))
        .collect();

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, axum::BoxError>> + Send>> =
        Box::pin(async_stream::try_stream! {
            yield Event::default()
                .event("resuming")
                .json_data(serde_json::json!({
                    "checkpoint_id": req.checkpoint_id,
                    "thread_id": req.thread_id,
                    "session_id": session_id,
                }))?;

            if !bg_results.is_empty() {
                yield Event::default()
                    .event("background_results")
                    .json_data(serde_json::json!({
                        "count": bg_results.len(),
                        "results": bg_results.iter().map(|(id, g, r)| {
                            serde_json::json!({
                                "agent_id": id.to_string(),
                                "goal": g,
                                "success": r.success,
                            })
                        }).collect::<Vec<_>>(),
                    }))?;
            }

            let resume_fut = agent_loop_with_progress
                .resume_from_checkpoint(&req.thread_id, req.checkpoint_id.as_deref());
            tokio::pin!(resume_fut);

            loop {
                tokio::select! {
                    Some(progress) = progress_rx.recv() => {
                        tracing::debug!("[resume_from_checkpoint_session] progress event: {:?}", progress);
                        yield Event::default()
                            .event("progress")
                            .json_data(serde_json::to_value(&progress)?)?;
                    }
                    result = &mut resume_fut => {
                        match result {
                            Ok(outcome) => {
                                match outcome {
                                    loom_agent::AgentRunOutcome::Finished(result) => {
                                        tracing::info!(
                                            "[resume_from_checkpoint_session] finished: iterations={}, tool_calls={}",
                                            result.iterations,
                                            result.tool_calls_made
                                        );
                                        // P0: 将后台结果注入到恢复后的历史中，避免丢失
                                        let mut history = result.history.clone();
                                        if !bg_results.is_empty() {
                                            let injection_lines: Vec<String> = bg_results
                                                .iter()
                                                .map(|(child_id, child_goal, result)| {
                                                    let status = if result.success { "completed" } else { "failed" };
                                                    let detail = if result.success {
                                                        result.summary.clone()
                                                    } else {
                                                        format!("error: {}", result.error_message.clone().unwrap_or_default())
                                                    };
                                                    format!(
                                                        "[background sub-agent {} ({}) {}]\n{}",
                                                        child_id, child_goal, status, detail
                                                    )
                                                })
                                                .collect();
                                            history.push(ChatMessage::user(format!(
                                                "Previously dispatched background sub-agent(s) have finished. Here are their results:\n\n{}",
                                                injection_lines.join("\n\n")
                                            )));
                                        }
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            true
                                        };
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            } else {
                                                let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            }
                                        }
                                        yield Event::default()
                                            .event("message")
                                            .json_data(serde_json::json!({
                                                "text": result.final_response,
                                                "iterations": result.iterations,
                                                "tool_calls_made": result.tool_calls_made,
                                                "tool_trace": result.tool_trace,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                    loom_agent::AgentRunOutcome::Interrupt {
                                        value,
                                        checkpoint_id,
                                        thread_id,
                                        history,
                                    } => {
                                        tracing::info!(
                                            "[resume_from_checkpoint_session] interrupted: checkpoint={}",
                                            checkpoint_id
                                        );
                                        // P0: 中断时也注入后台结果到历史
                                        let mut history = history.clone();
                                        if !bg_results.is_empty() {
                                            let injection_lines: Vec<String> = bg_results
                                                .iter()
                                                .map(|(child_id, child_goal, result)| {
                                                    let status = if result.success { "completed" } else { "failed" };
                                                    let detail = if result.success {
                                                        result.summary.clone()
                                                    } else {
                                                        format!("error: {}", result.error_message.clone().unwrap_or_default())
                                                    };
                                                    format!(
                                                        "[background sub-agent {} ({}) {}]\n{}",
                                                        child_id, child_goal, status, detail
                                                    )
                                                })
                                                .collect();
                                            history.push(ChatMessage::user(format!(
                                                "Previously dispatched background sub-agent(s) have finished. Here are their results:\n\n{}",
                                                injection_lines.join("\n\n")
                                            )));
                                        }
                                        let saved = if let Err(e) = sessions.save(&session_scope, &session_id, &history).await {
                                            tracing::error!("failed to persist session {}: {}", session_id, e);
                                            false
                                        } else {
                                            true
                                        };
                                        if !claimed_ids.is_empty() {
                                            if saved {
                                                agent_loop_with_progress.complete_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            } else {
                                                let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                            }
                                        }
                                        yield Event::default()
                                            .event("interrupt")
                                            .json_data(serde_json::json!({
                                                "value": value,
                                                "checkpoint_id": checkpoint_id,
                                                "thread_id": thread_id,
                                                "session_id": session_id,
                                            }))?;
                                        yield Event::default().event("done");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("[resume_from_checkpoint_session] failed: {e}");
                                // run 失败时 release 后台任务，允许重试
                                if !claimed_ids.is_empty() {
                                    let _ = agent_loop_with_progress.release_background_delivery(&claimed_ids, &claimed_claim_ids);
                                }
                                yield Event::default()
                                    .event("error")
                                    .json_data(serde_json::json!({
                                        "error": e.to_string(),
                                        "session_id": session_id,
                                    }))?;
                                yield Event::default().event("done");
                            }
                        }
                        break;
                    }
                }
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}