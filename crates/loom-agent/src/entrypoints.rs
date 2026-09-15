use loom_core::{CheckpointConfig, MemoryScope, Result};
use loom_llm::{ChatMessage, Role};
use std::sync::Arc;
use uuid::Uuid;

use crate::hitl::{AgentEvent, ResumeCommand};
use crate::loop_types::{
    AgentRunOutcome, AgentRunResult, AgentRunResultWithHistory, AgentState, LoopContext,
};
use crate::prompt::{build_system_prompt, PromptParts};
use crate::tools::ToolResolver;

use super::AgentLoop;

impl AgentLoop {
    pub async fn run_goal(
        &self,
        goal: &str,
        context: &str,
        agent_id: Uuid,
        parent_agent_id: Option<Uuid>,
        delegate_depth: u32,
        toolsets: &[String],
        parent_toolsets: &[String],
        activity_state: Option<Arc<std::sync::Mutex<loom_core::ActivitySummary>>>,
    ) -> Result<AgentRunResult> {
        match self
            .run_goal_with_event(
                goal,
                context,
                agent_id,
                parent_agent_id,
                delegate_depth,
                toolsets,
                parent_toolsets,
                activity_state,
            )
            .await?
        {
            AgentEvent::Finished {
                response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
            } => Ok(AgentRunResult {
                final_response: response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
            }),
            AgentEvent::Interrupt { .. } => Err(loom_core::LoomError::AgentExecution(
                "agent returned interrupt; use run_goal_with_event + resume for HITL flow"
                    .to_string(),
            )),
        }
    }

    /// 运行 Agent 循环，支持 human-in-the-loop
    ///
    /// 返回 `AgentEvent`：
    /// - `Finished`: 正常完成
    /// - `Interrupt`: 执行暂停，保存了 checkpoint，调用者可通过 resume 恢复
    ///
    /// `toolsets`：工具集白名单（空表示全部可用）
    /// `parent_toolsets`：父 Agent 的工具集（用于子 Agent 工具集交集，顶层传空）
    ///
    /// 记忆策略：
    /// - 顶层 Agent（delegate_depth == 0）：加载会话记忆快照
    /// - 子 Agent（delegate_depth > 0）：不加载记忆（skip_memory），避免父会话记忆干扰子 Agent 独立推理
    pub async fn run_goal_with_event(
        &self,
        goal: &str,
        context: &str,
        agent_id: Uuid,
        parent_agent_id: Option<Uuid>,
        delegate_depth: u32,
        toolsets: &[String],
        parent_toolsets: &[String],
        activity_state: Option<Arc<std::sync::Mutex<loom_core::ActivitySummary>>>,
    ) -> Result<AgentEvent> {
        let tools = ToolResolver::new(self.registry.as_ref(), &self.config)
            .available_tools(toolsets, parent_toolsets, delegate_depth)
            .await;

        let stable = self.get_stable_prompt().await;
        let context_part = build_system_prompt(
            &self.config,
            goal,
            context,
            &tools,
            self.workspace_dir.as_deref(),
        );
        let parts = PromptParts {
            stable,
            context: context_part.context,
            volatile: context_part.volatile,
        };
        let system_prompt = parts.joined();

        let mut messages: Vec<ChatMessage> = vec![ChatMessage::system(system_prompt)];
        let thread_id = agent_id.to_string();
        // 记忆策略：子 Agent 不加载记忆（skip_memory=True），仅顶层 Agent 加载会话记忆快照
        if delegate_depth == 0 {
            if let Some(memory) = &self.memory {
                let scope = self.memory_scope();
                memory.load_session(&scope, &thread_id).await;
                let block = memory.build_system_prompt(&scope, &thread_id).await;
                if !block.is_empty() {
                    messages.push(ChatMessage::system(block));
                }
            }
        }
        messages.push(ChatMessage::user(format!("Goal: {goal}")));

        let ctx = LoopContext {
            messages,
            iterations: 0,
            tool_calls_made: 0,
            tool_names: Vec::new(),
            thread_id,
            last_checkpoint_id: None,
            agent_id,
            parent_agent_id,
            delegate_depth,
            toolsets: toolsets.to_vec(),
            parent_toolsets: parent_toolsets.to_vec(),

            activity_state,
        };

        let (event, _history) = self.run_loop(ctx).await?;
        Ok(event)
    }

    /// 运行 Agent 循环，支持携带已有对话历史（用于多轮会话）
    ///
    /// - 若 `history` 为 `None`，构建全新的 system + goal 消息
    /// - 若 `history` 为 `Some(messages)`，在其末尾追加用户消息并继续
    ///
    /// 返回更新后的完整消息历史（供调用方保存到会话存储）。
    pub async fn run_goal_with_history(
        &self,
        goal: &str,
        context: &str,
        agent_id: Uuid,
        parent_agent_id: Option<Uuid>,
        delegate_depth: u32,
        history: Option<Vec<ChatMessage>>,
        toolsets: &[String],
        parent_toolsets: &[String],
        activity_state: Option<Arc<std::sync::Mutex<loom_core::ActivitySummary>>>,
    ) -> Result<AgentRunOutcome> {
        let tools = ToolResolver::new(self.registry.as_ref(), &self.config)
            .available_tools(toolsets, parent_toolsets, delegate_depth)
            .await;
        let thread_id = agent_id.to_string();
        // 子 Agent 不加载记忆（skip_memory=True）
        let load_memory = delegate_depth == 0;

        let messages = match history {
            Some(mut existing) => {
                // 已有会话：直接追加用户消息，不重建 system prompt
                if load_memory {
                    if let Some(memory) = &self.memory {
                        let scope = self.memory_scope();
                        memory.load_session(&scope, &thread_id).await;
                    }
                }
                existing.push(ChatMessage::user(goal.to_string()));
                existing
            }
            None => {
                // 新会话：构建 system prompt + goal
                let stable = self.get_stable_prompt().await;
                let context_part = build_system_prompt(
                    &self.config,
                    goal,
                    context,
                    &tools,
                    self.workspace_dir.as_deref(),
                );
                let parts = PromptParts {
                    stable,
                    context: context_part.context,
                    volatile: context_part.volatile,
                };
                let system_prompt = parts.joined();

                let mut msgs = vec![ChatMessage::system(system_prompt)];
                // 注入会话级记忆快照（仅顶层 Agent）
                if load_memory {
                    if let Some(memory) = &self.memory {
                        let scope = self.memory_scope();
                        memory.load_session(&scope, &thread_id).await;
                        let block = memory.build_system_prompt(&scope, &thread_id).await;
                        if !block.is_empty() {
                            msgs.push(ChatMessage::system(block));
                        }
                    }
                }
                msgs.push(ChatMessage::user(format!("Goal: {goal}")));
                msgs
            }
        };

        let ctx = LoopContext {
            messages,
            iterations: 0,
            tool_calls_made: 0,
            tool_names: Vec::new(),
            thread_id,
            last_checkpoint_id: None,
            agent_id,
            parent_agent_id,
            delegate_depth,
            toolsets: toolsets.to_vec(),
            parent_toolsets: parent_toolsets.to_vec(),

            activity_state,
        };

        let (event, final_messages) = self.run_loop(ctx).await?;

        let outcome = match event {
            AgentEvent::Finished {
                response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
            } => AgentRunOutcome::Finished(AgentRunResultWithHistory {
                final_response: response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
                history: final_messages,
            }),
            AgentEvent::Interrupt {
                value,
                checkpoint_id,
                thread_id,
            } => AgentRunOutcome::Interrupt {
                value,
                checkpoint_id,
                thread_id,
                history: final_messages,
            },
        };

        Ok(outcome)
    }

    /// 从 checkpoint 恢复执行（human-in-the-loop resume）
    ///
    /// resume 机制：
    /// 1. 从 checkpoint_saver 读取检查点状态
    /// 2. 将 resume_value 作为 interrupt 工具的返回值注入消息历史
    /// 3. 从断点继续执行循环
    pub async fn resume(&self, cmd: ResumeCommand) -> Result<AgentRunOutcome> {
        tracing::debug!(
            "[resume] start: thread_id={}, checkpoint_id={}, resume_value_len={}",
            cmd.thread_id,
            cmd.checkpoint_id,
            cmd.resume_value.to_string().len()
        );

        let scope = self.scope.clone().unwrap_or_default();
        let config = CheckpointConfig::new(&cmd.thread_id)
            .with_checkpoint(&cmd.checkpoint_id)
            .with_tenant(scope.tenant_id, scope.user_id);

        let tuple = self.checkpoint_saver.get_tuple(&config).await?;
        let Some(tuple) = tuple else {
            tracing::debug!(
                "[resume] checkpoint NOT found: thread={}, checkpoint={}",
                cmd.thread_id,
                cmd.checkpoint_id
            );
            return Err(loom_core::LoomError::Other(format!(
                "checkpoint not found: thread={}, checkpoint={}",
                cmd.thread_id, cmd.checkpoint_id
            )));
        };
        tracing::debug!(
            "[resume] checkpoint loaded: thread={}, checkpoint={}",
            cmd.thread_id,
            cmd.checkpoint_id
        );

        // 从 checkpoint 反序列化状态
        let state: AgentState = serde_json::from_value(tuple.checkpoint.channel_values.clone())?;
        tracing::debug!(
            "[resume] state restored: iterations={}, messages={}, tool_calls_made={}",
            state.iterations,
            state.messages.len(),
            state.tool_calls_made
        );

        // 恢复压缩状态（previous_summary / 失败计数 / terminal_failure），
        // 保证迭代式摘要链在 resume 后不断裂
        if let Some(snapshot) = state.compression_state {
            self.compressor.restore_state(snapshot);
        }

        // 恢复作用域（租户/用户隔离）
        let mut scoped_self = self.clone();
        if let Some(scope_json) = &state.scope_json {
            if let Ok(parsed_scope) = serde_json::from_str::<MemoryScope>(scope_json) {
                scoped_self = scoped_self.with_scope(parsed_scope);
            }
        }

        // 找到 interrupt 工具调用，将 resume_value 作为其结果
        let mut messages = state.messages;
        let interrupt_tc_id = messages
            .last()
            .filter(|m| m.role == Role::Assistant)
            .and_then(|m| m.tool_calls.as_ref())
            .and_then(|tcs| tcs.iter().find(|tc| tc.name == "interrupt"))
            .map(|tc| tc.id.clone());

        tracing::debug!(
            "[resume] interrupt tool_call_id found: {:?}",
            interrupt_tc_id
        );

        if let Some(tc_id) = interrupt_tc_id {
            messages.push(ChatMessage::tool(&tc_id, cmd.resume_value.to_string()));
            tracing::debug!(
                "[resume] injected resume_value as tool result: tc_id={}, messages_len={}",
                tc_id,
                messages.len()
            );
        }

        let agent_id = match Uuid::parse_str(&cmd.thread_id) {
            Ok(id) => id,
            Err(_) => {
                tracing::warn!(
                    "thread_id {} is not a valid UUID, generating new agent_id for resume",
                    cmd.thread_id
                );
                Uuid::new_v4()
            }
        };

        tracing::info!(
            "resuming agent from checkpoint {} (iteration {})",
            cmd.checkpoint_id,
            state.iterations
        );

        // 恢复会话记忆（外部后端会重新建立会话上下文）
        if let Some(memory) = &scoped_self.memory {
            let scope = scoped_self.memory_scope();
            memory.load_session(&scope, &cmd.thread_id).await;
        }

        // 处理 max_iterations 中断的恢复选项
        let max_iter_action = cmd
            .resume_value
            .get("action")
            .and_then(|v| v.as_str());
        if max_iter_action == Some("summarize") {
            // 用户选择"停止并总结"：发起一次不带工具的 LLM 调用获取总结
            tracing::info!(
                "[resume] max_iterations summarize: requesting final summary without tools"
            );
            messages.push(ChatMessage::user(
                "你已达到最大工具调用迭代次数。请提供最终回复，总结目前已发现和完成的内容，不要再调用任何工具。"
                    .to_string(),
            ));
            let summary_resp = crate::api_retry::chat_with_retry(
                &*scoped_self.llm,
                messages.clone(),
                vec![],
                &scoped_self.config.api_retry,
            )
            .await?;
            let summary = summary_resp
                .content
                .unwrap_or_else(|| "(empty summary)".to_string());
            messages.push(ChatMessage::assistant(summary.clone()));
            return Ok(AgentRunOutcome::Finished(AgentRunResultWithHistory {
                final_response: summary,
                iterations: state.iterations,
                tool_calls_made: state.tool_calls_made,
                tool_names: state.tool_names,
                duration_ms: 0,
                tool_trace: None,
                history: messages,
            }));
        }

        // 用户选择"继续执行"或其他情况：continue 时重置迭代计数
        let iterations = if max_iter_action == Some("continue") {
            tracing::info!(
                "[resume] max_iterations continue: resetting iteration count from {} to 0",
                state.iterations
            );
            0
        } else {
            state.iterations
        };

        let ctx = LoopContext {
            messages,
            iterations,
            tool_calls_made: state.tool_calls_made,
            tool_names: state.tool_names,
            thread_id: cmd.thread_id,
            last_checkpoint_id: Some(cmd.checkpoint_id),
            agent_id,
            parent_agent_id: state.parent_agent_id,
            delegate_depth: state.delegate_depth,
            toolsets: state.toolsets,
            parent_toolsets: state.parent_toolsets,

            activity_state: None,
        };

        let (event, history) = scoped_self.run_loop(ctx).await?;
        let outcome = match event {
            AgentEvent::Finished {
                response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
            } => {
                tracing::debug!(
                    "[resume] finished: iterations={}, tool_calls={}, response_len={}",
                    iterations,
                    tool_calls_made,
                    response.len()
                );
                AgentRunOutcome::Finished(AgentRunResultWithHistory {
                    final_response: response,
                    iterations,
                    tool_calls_made,
                    tool_names,
                    duration_ms,
                    tool_trace,
                    history,
                })
            }
            AgentEvent::Interrupt {
                value,
                checkpoint_id,
                thread_id,
            } => {
                tracing::debug!(
                    "[resume] re-interrupted: checkpoint={}, value={}",
                    checkpoint_id,
                    value
                );
                AgentRunOutcome::Interrupt {
                    value,
                    checkpoint_id,
                    thread_id,
                    history,
                }
            }
        };

        Ok(outcome)
    }

    /// 从 checkpoint 断点续执行（通用恢复，不限于 interrupt 场景）
    ///
    /// 与 `resume` 的区别：
    /// - `resume` 仅支持 interrupt 恢复：将 `resume_value` 注入为 interrupt 工具的结果
    /// - `resume_from_checkpoint` 支持从任意 checkpoint（step / pre_tool_exec / interrupt）恢复：
    ///   无需额外输入，直接从断点继续执行
    ///
    /// 恢复策略（根据消息历史最后一条消息的角色决定）：
    /// - 最后一条是 **user**：直接继续循环，LLM 会响应这条用户消息
    /// - 最后一条是 **tool**：工具结果已返回，继续循环让 LLM 处理结果
    /// - 最后一条是 **assistant（带 tool_calls）**：工具可能未执行完，注入"工具执行中断"提示，
    ///   让 LLM 重新决策（避免重复执行已完成的工具或挂起未完成的工具）
    /// - 最后一条是 **assistant（纯文本）**：Agent 已给出最终响应，无需继续，返回 Finished
    ///
    /// # 参数
    /// - `thread_id`：会话线程 ID
    /// - `checkpoint_id`：指定从哪个 checkpoint 恢复；`None` 表示恢复最新的 checkpoint
    pub async fn resume_from_checkpoint(
        &self,
        thread_id: &str,
        checkpoint_id: Option<&str>,
    ) -> Result<AgentRunOutcome> {
        tracing::info!(
            "[resume_from_checkpoint] start: thread_id={}, checkpoint_id={:?}",
            thread_id,
            checkpoint_id
        );

        let scope = self.scope.clone().unwrap_or_default();
        let mut config = CheckpointConfig::new(thread_id).with_tenant(scope.tenant_id, scope.user_id);
        if let Some(cp_id) = checkpoint_id {
            config = config.with_checkpoint(cp_id);
        }

        let tuple = self.checkpoint_saver.get_tuple(&config).await?;
        let Some(tuple) = tuple else {
            return Err(loom_core::LoomError::Other(format!(
                "checkpoint not found: thread={}, checkpoint={:?}",
                thread_id, checkpoint_id
            )));
        };

        let cp = &tuple.checkpoint;
        tracing::info!(
            "[resume_from_checkpoint] loaded checkpoint: id={}, source={}, step={}",
            cp.id,
            cp.metadata.source,
            cp.metadata.step
        );

        // 反序列化状态（兼容旧 checkpoint：缺失字段使用默认值）
        let state: AgentState = serde_json::from_value(cp.channel_values.clone())?;
        let mut messages = state.messages;

        // 恢复压缩状态，保证迭代式摘要链不断裂
        if let Some(snapshot) = state.compression_state {
            self.compressor.restore_state(snapshot);
        }

        // 根据最后一条消息决定恢复策略
        let needs_continuation = match messages.last() {
            Some(last) => match last.role {
                Role::User => false,
                Role::Tool => false,
                Role::Assistant => {
                    if last.tool_calls.as_ref().is_some_and(|tcs| !tcs.is_empty()) {
                        // assistant 有未完成的 tool_calls：注入中断提示，让 LLM 重新决策
                        let tool_call_ids: Vec<String> = last
                            .tool_calls
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|tc| tc.id.clone())
                            .collect();
                        for tc_id in &tool_call_ids {
                            messages.push(ChatMessage::tool(
                                tc_id,
                                "[execution interrupted: tool result unknown, please re-evaluate and decide whether to retry or continue]",
                            ));
                        }
                        false
                    } else {
                        // assistant 纯文本响应：已完成，无需继续
                        tracing::info!(
                            "[resume_from_checkpoint] checkpoint at final assistant response, no continuation needed"
                        );
                        let final_text = last.content.clone().unwrap_or_default();
                        return Ok(AgentRunOutcome::Finished(AgentRunResultWithHistory {
                            final_response: final_text,
                            iterations: state.iterations,
                            tool_calls_made: state.tool_calls_made,
                            tool_names: state.tool_names,
                            duration_ms: 0,
                            tool_trace: None,
                            history: messages,
                        }));
                    }
                }
                Role::System => false,
            },
            None => {
                // 空消息历史：不应该发生
                return Err(loom_core::LoomError::Other(
                    "checkpoint has empty message history".to_string(),
                ));
            }
        };

        // 如果最后一条不是 user/tool，注入 continuation 提示保持对话格式
        if needs_continuation {
            messages.push(ChatMessage::user(
                "[resumed from checkpoint: please continue from where you left off]".to_string(),
            ));
        }

        // 恢复作用域
        let mut scoped_self = self.clone();
        if let Some(scope_json) = &state.scope_json {
            if let Ok(parsed_scope) = serde_json::from_str::<MemoryScope>(scope_json) {
                scoped_self = scoped_self.with_scope(parsed_scope);
            }
        }

        let agent_id = match Uuid::parse_str(thread_id) {
            Ok(id) => id,
            Err(_) => Uuid::new_v4(),
        };

        // 恢复会话记忆
        if let Some(memory) = &scoped_self.memory {
            let mem_scope = scoped_self.memory_scope();
            memory.load_session(&mem_scope, thread_id).await;
        }

        tracing::info!(
            "[resume_from_checkpoint] resuming loop: iterations={}, messages={}, delegate_depth={}",
            state.iterations,
            messages.len(),
            state.delegate_depth
        );

        let ctx = LoopContext {
            messages,
            iterations: state.iterations,
            tool_calls_made: state.tool_calls_made,
            tool_names: state.tool_names,
            thread_id: thread_id.to_string(),
            last_checkpoint_id: Some(cp.id.clone()),
            agent_id,
            parent_agent_id: state.parent_agent_id,
            delegate_depth: state.delegate_depth,
            toolsets: state.toolsets,
            parent_toolsets: state.parent_toolsets,
            activity_state: None,
        };

        let (event, history) = scoped_self.run_loop(ctx).await?;
        let outcome = match event {
            AgentEvent::Finished {
                response,
                iterations,
                tool_calls_made,
                tool_names,
                duration_ms,
                tool_trace,
            } => {
                tracing::info!(
                    "[resume_from_checkpoint] finished: iterations={}, tool_calls={}",
                    iterations,
                    tool_calls_made
                );
                AgentRunOutcome::Finished(AgentRunResultWithHistory {
                    final_response: response,
                    iterations,
                    tool_calls_made,
                    tool_names,
                    duration_ms,
                    tool_trace,
                    history,
                })
            }
            AgentEvent::Interrupt {
                value,
                checkpoint_id,
                thread_id,
            } => {
                tracing::info!(
                    "[resume_from_checkpoint] re-interrupted: checkpoint={}",
                    checkpoint_id
                );
                AgentRunOutcome::Interrupt {
                    value,
                    checkpoint_id,
                    thread_id,
                    history,
                }
            }
        };

        Ok(outcome)
    }
}