import { useState, useCallback, useRef, useEffect } from 'react'
import Sidebar from './components/Sidebar.jsx'
import ChatArea from './components/ChatArea.jsx'
import Login, { getCredentials, clearCredentials } from './components/Login.jsx'
import { chatStream, resumeStream, getSessions, getSession, deleteSession } from './api.js'

const CURRENT_SESSION_KEY = 'loom-current-session'

export default function App() {
  const [sessions, setSessions] = useState([])
  const [currentSessionId, setCurrentSessionId] = useState(null)
  const [messages, setMessages] = useState([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [interruptInfo, setInterruptInfo] = useState(null)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [credentials, setCredentials] = useState(() => getCredentials())
  const abortRef = useRef(null)

  const loggedIn = Boolean(credentials.tenantId && credentials.userId)

  const handleLogin = useCallback((creds) => {
    setCredentials(creds)
    loadSessions()
  }, [])

  const handleLogout = useCallback(() => {
    if (abortRef.current) {
      abortRef.current()
      abortRef.current = null
    }
    clearCredentials()
    setCredentials({ tenantId: '', userId: '' })
    setSessions([])
    setCurrentSessionId(null)
    setMessages([])
    setIsStreaming(false)
    setInterruptInfo(null)
    localStorage.removeItem(CURRENT_SESSION_KEY)
  }, [])

  // 初始化：从后端加载会话列表
  useEffect(() => {
    if (loggedIn) {
      loadSessions()
    }
  }, [loggedIn])

  const loadSessions = useCallback(async () => {
    try {
      const list = await getSessions()
      setSessions(list || [])
    } catch (e) {
      console.error('failed to load sessions', e)
    }
  }, [])

  const currentSession = sessions.find((s) => s.session_id === currentSessionId)

  const handleNewSession = useCallback(() => {
    if (abortRef.current) {
      abortRef.current()
      abortRef.current = null
    }
    setCurrentSessionId(null)
    setMessages([])
    setIsStreaming(false)
    setInterruptInfo(null)
    localStorage.removeItem(CURRENT_SESSION_KEY)
  }, [])

  const handleSelectSession = useCallback(
    async (id) => {
      if (abortRef.current) {
        abortRef.current()
        abortRef.current = null
      }
      setCurrentSessionId(id)
      setInterruptInfo(null)
      localStorage.setItem(CURRENT_SESSION_KEY, id)
      try {
        const detail = await getSession(id)
        // 过滤掉 system 消息（如 [session-title] 标记）和 tool 消息，只展示 user/assistant
        // 但保留 assistant 消息中的 tool_calls，用于在思考过程中展示工具调用详情
        const allMsgs = detail.messages || []
        const visible = []
        let lastAssistant = null
        for (const m of allMsgs) {
          if (m.role === 'user' || m.role === 'assistant') {
            const msg = { role: m.role, content: m.content }
            if (m.role === 'assistant') {
              // 过滤掉 content 为空的 assistant 消息（纯工具调用的中间轮次）
              if (!m.content) continue
              msg.thinking = ''
              msg.isLoading = false
              lastAssistant = msg
            }
            visible.push(msg)
          }
        }
        // tool_trace 汇总到最后一条 assistant 消息
        if (lastAssistant && detail.tool_trace?.entries?.length) {
          lastAssistant.toolTrace = detail.tool_trace.entries
          lastAssistant.iterations = detail.tool_trace.entries.length
          lastAssistant.toolCalls = detail.tool_trace.entries.length
        }
        setMessages(visible)
      } catch (e) {
        console.error('failed to load session', e)
        setMessages([])
      }
      setIsStreaming(false)
    },
    [],
  )

  const handleDeleteSession = useCallback(
    async (id) => {
      try {
        await deleteSession(id)
        if (currentSessionId === id) {
          setCurrentSessionId(null)
          setMessages([])
          localStorage.removeItem(CURRENT_SESSION_KEY)
        }
        loadSessions()
      } catch (e) {
        console.error('failed to delete session', e)
      }
    },
    [currentSessionId, loadSessions],
  )

  const handleSend = useCallback(
    (text) => {
      if (isStreaming) return

      setInterruptInfo(null)
      const userMsg = { role: 'user', content: text }
      const assistantMsg = { role: 'assistant', content: '', thinking: '' }

      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setIsStreaming(true)

      abortRef.current = chatStream(
        { message: text, sessionId: currentSessionId || undefined },
        {
          onThinking: (data) => {
            if (data.session_id && !currentSessionId) {
              setCurrentSessionId(data.session_id)
              localStorage.setItem(CURRENT_SESSION_KEY, data.session_id)
            }
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (last && last.role === 'assistant') {
                copy[copy.length - 1] = {
                  ...last,
                  thinking: data.goal || last.thinking || '正在分析你的问题...',
                }
              }
              return copy
            })
          },
          onMessage: (data) => {
            if (data.session_id) {
              setCurrentSessionId(data.session_id)
              localStorage.setItem(CURRENT_SESSION_KEY, data.session_id)
            }
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (last && last.role === 'assistant') {
                const finalTrace = data.tool_trace?.entries || []
                copy[copy.length - 1] = {
                  ...last,
                  content: data.text || '',
                  thinking: last.thinking || '思考完成',
                  isLoading: false,
                  iterations: data.iterations,
                  toolCalls: data.tool_calls_made,
                  toolTrace: finalTrace,
                }
              }
              return copy
            })
          },
          onInterrupt: (data) => {
            setInterruptInfo({
              value: data.value,
              checkpoint_id: data.checkpoint_id,
              thread_id: data.thread_id,
              session_id: data.session_id,
            })
          },
          onProgress: (data) => {
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (!last || last.role !== 'assistant') return prev

              const trace = Array.isArray(last.toolTrace) ? [...last.toolTrace] : []

              if (data.type === 'iteration') {
                copy[copy.length - 1] = {
                  ...last,
                  iterations: data.iteration,
                  toolTrace: trace,
                }
                return copy
              }

              if (data.type === 'tool_start') {
                trace.push({
                  tool: data.tool,
                  arguments: data.arguments,
                  result: '',
                  is_error: false,
                  running: true,
                })
                copy[copy.length - 1] = { ...last, toolTrace: trace }
                return copy
              }

              if (data.type === 'tool_end') {
                // 找到第一个 running 的同工具条目，填入结果
                const idx = trace.findIndex((t) => t.running && t.tool === data.tool)
                if (idx >= 0) {
                  trace[idx] = {
                    ...trace[idx],
                    result: data.result,
                    is_error: data.is_error,
                    running: false,
                  }
                } else {
                  // 兜底：直接追加
                  trace.push({
                    tool: data.tool,
                    arguments: undefined,
                    result: data.result,
                    is_error: data.is_error,
                    running: false,
                  })
                }
                copy[copy.length - 1] = { ...last, toolTrace: trace }
                return copy
              }

              return prev
            })
          },
          onError: (data) => {
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (last && last.role === 'assistant') {
                copy[copy.length - 1] = {
                  ...last,
                  content: `❌ 出错了：${data.error || '未知错误'}`,
                  thinking: last.thinking || '发生错误',
                }
              }
              return copy
            })
          },
          onDone: () => {
            setIsStreaming(false)
            abortRef.current = null
            loadSessions()
          },
        },
      )
    },
    [isStreaming, currentSessionId, loadSessions],
  )

  const handleStop = useCallback(() => {
    if (abortRef.current) {
      abortRef.current()
      abortRef.current = null
    }
    setIsStreaming(false)
  }, [])

  // Human-in-the-Loop 恢复：将用户输入作为 resume_value 提交，继续 Agent 执行
  const handleResume = useCallback(
    (text) => {
      if (isStreaming || !interruptInfo) return

      const { session_id, checkpoint_id, thread_id } = interruptInfo
      const resumeValue = text

      // 将用户回复作为一条消息追加展示
      const userMsg = { role: 'user', content: text }
      const assistantMsg = { role: 'assistant', content: '', thinking: '' }
      setMessages((prev) => [...prev, userMsg, assistantMsg])
      setInterruptInfo(null)
      setIsStreaming(true)

      abortRef.current = resumeStream(
        {
          sessionId: session_id,
          checkpointId: checkpoint_id,
          threadId: thread_id,
          resumeValue,
        },
        {
          onMessage: (data) => {
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (last && last.role === 'assistant') {
                const finalTrace = data.tool_trace?.entries || []
                copy[copy.length - 1] = {
                  ...last,
                  content: data.text || '',
                  thinking: last.thinking || '思考完成',
                  isLoading: false,
                  iterations: data.iterations,
                  toolCalls: data.tool_calls_made,
                  toolTrace: finalTrace,
                }
              }
              return copy
            })
          },
          onInterrupt: (data) => {
            setInterruptInfo({
              value: data.value,
              checkpoint_id: data.checkpoint_id,
              thread_id: data.thread_id,
              session_id: data.session_id,
            })
          },
          onProgress: (data) => {
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (!last || last.role !== 'assistant') return prev

              const trace = Array.isArray(last.toolTrace) ? [...last.toolTrace] : []

              if (data.type === 'iteration') {
                copy[copy.length - 1] = { ...last, iterations: data.iteration, toolTrace: trace }
                return copy
              }
              if (data.type === 'tool_start') {
                trace.push({
                  tool: data.tool,
                  arguments: data.arguments,
                  result: '',
                  is_error: false,
                  running: true,
                })
                copy[copy.length - 1] = { ...last, toolTrace: trace }
                return copy
              }
              if (data.type === 'tool_end') {
                const idx = trace.findIndex((t) => t.running && t.tool === data.tool)
                if (idx >= 0) {
                  trace[idx] = {
                    ...trace[idx],
                    result: data.result,
                    is_error: data.is_error,
                    running: false,
                  }
                } else {
                  trace.push({
                    tool: data.tool,
                    arguments: undefined,
                    result: data.result,
                    is_error: data.is_error,
                    running: false,
                  })
                }
                copy[copy.length - 1] = { ...last, toolTrace: trace }
                return copy
              }
              return prev
            })
          },
          onError: (data) => {
            setMessages((prev) => {
              const copy = [...prev]
              const last = copy[copy.length - 1]
              if (last && last.role === 'assistant') {
                copy[copy.length - 1] = {
                  ...last,
                  content: `❌ 恢复出错：${data.error || '未知错误'}`,
                  thinking: last.thinking || '发生错误',
                }
              }
              return copy
            })
          },
          onDone: () => {
            setIsStreaming(false)
            abortRef.current = null
            loadSessions()
          },
        },
      )
    },
    [isStreaming, interruptInfo, loadSessions],
  )

  if (!loggedIn) {
    return <Login onLogin={handleLogin} />
  }

  return (
    <div className="flex h-full w-full bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100">
      <Sidebar
        sessions={sessions}
        currentSessionId={currentSessionId}
        onSelect={handleSelectSession}
        onNew={handleNewSession}
        onDelete={handleDeleteSession}
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((v) => !v)}
        onLogout={handleLogout}
        credentials={credentials}
      />
      <ChatArea
        messages={messages}
        isStreaming={isStreaming}
        onSend={handleSend}
        onStop={handleStop}
        onResume={handleResume}
        interruptInfo={interruptInfo}
        currentSessionTitle={currentSession?.title}
      />
    </div>
  )
}