import { useEffect, useRef } from 'react'
import MessageItem from './MessageItem.jsx'
import ChatInput from './ChatInput.jsx'
import { Sparkles, AlertCircle } from 'lucide-react'

const SUGGESTIONS = [
  '帮我写一个 Python 脚本，批量重命名当前目录下所有 .txt 文件，加上今天的日期前缀',
  '搜索一下 2026 年最值得关注的 AI 编程工具有哪些，帮我整理一份对比清单',
  '读取当前目录下的 README.md，总结一下这个项目的主要功能和使用方式',
  '帮我在看板上创建三个任务：写需求文档、实现核心功能、测试上线',
  '帮我创建一个每天凌晨 3 点自动备份项目目录到 backup 文件夹的定时任务',
  '用 todo_list 帮我规划今天要完成的三件事，并按优先级排序',
]

export default function ChatArea({
  messages,
  isStreaming,
  onSend,
  onStop,
  onResume,
  interruptInfo,
  currentSessionTitle,
}) {
  const scrollRef = useRef(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages, interruptInfo])

  const isEmpty = messages.length === 0

  // 格式化 interrupt value 为可展示文本
  const formatInterruptValue = (value) => {
    if (value == null) return ''
    if (typeof value === 'string') return value
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }

  return (
    <div className="flex-1 flex flex-col h-full min-w-0">
      {/* 顶部标题栏 */}
      <header className="h-14 shrink-0 flex items-center px-4 border-b border-slate-200 dark:border-slate-700 bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm">
        <h2 className="font-medium text-slate-800 dark:text-slate-100 truncate">
          {currentSessionTitle || '新对话'}
        </h2>
        {isStreaming && (
          <span className="ml-3 flex items-center gap-1.5 text-xs text-brand-600 dark:text-brand-400">
            <span className="w-1.5 h-1.5 rounded-full bg-brand-500 animate-pulse" />
            正在生成
          </span>
        )}
      </header>

      {/* 消息列表 */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {isEmpty ? (
          <div className="h-full flex flex-col items-center justify-center px-6 text-center">
            <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-brand-500 to-purple-600 flex items-center justify-center mb-6 shadow-lg shadow-brand-500/30">
              <Sparkles size={32} className="text-white" />
            </div>
            <h1 className="text-2xl font-bold text-slate-800 dark:text-slate-100 mb-2">
              你好，我是 Loom AI
            </h1>
            <p className="text-slate-500 dark:text-slate-400 mb-8 max-w-md">
              我是一个具备工具调用能力的 AI Agent，可以帮你阅读代码、执行任务、分析问题。
              试试下面的建议，或直接输入你的问题。
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 w-full max-w-xl">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => onSend(s)}
                  className="px-4 py-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 hover:border-brand-400 hover:bg-brand-50 dark:hover:bg-brand-900/20 text-sm text-slate-700 dark:text-slate-300 transition-all text-left"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="max-w-3xl mx-auto py-4">
            {messages.map((m, i) => (
              <MessageItem key={i} message={m} />
            ))}
            {/* HITL 中断提示：Agent 暂停等待人工输入 */}
            {interruptInfo && (
              <div className="my-4 mx-auto max-w-3xl">
                <div className="rounded-2xl border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-4 shadow-sm">
                  <div className="flex items-start gap-3">
                    <div className="shrink-0 w-8 h-8 rounded-full bg-amber-500 flex items-center justify-center text-white">
                      <AlertCircle size={18} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-amber-800 dark:text-amber-200 mb-1">
                        Agent 暂停，需要你的输入
                      </div>
                      <p className="text-sm text-amber-700 dark:text-amber-300 mb-2">
                        Agent 在执行过程中调用了 interrupt 工具，正在等待你的回复以继续。
                      </p>
                      <div className="bg-white/60 dark:bg-slate-900/60 rounded-lg p-3 text-sm text-slate-700 dark:text-slate-200 whitespace-pre-wrap break-words">
                        {formatInterruptValue(interruptInfo.value)}
                      </div>
                      <p className="text-xs text-amber-600 dark:text-amber-400 mt-2">
                        请在下方输入框中输入你的回复，将作为 interrupt 的返回值。
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* 输入框 */}
      <ChatInput
        onSend={onSend}
        disabled={isStreaming}
        isStreaming={isStreaming}
        onStop={onStop}
        isInterrupt={!!interruptInfo}
        onResume={onResume}
      />
    </div>
  )
}