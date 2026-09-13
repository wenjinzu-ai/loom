import { useState, useRef, useEffect } from 'react'
import { Send, Square, StopCircle } from 'lucide-react'

export default function ChatInput({ onSend, disabled, isStreaming, onStop, isInterrupt, onResume }) {
  const [text, setText] = useState('')
  const textareaRef = useRef(null)

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 200) + 'px'
    }
  }, [text])

  const handleSubmit = (e) => {
    e?.preventDefault()
    const trimmed = text.trim()
    if (!trimmed || disabled) return
    if (isInterrupt && onResume) {
      onResume(trimmed)
    } else {
      onSend(trimmed)
    }
    setText('')
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  const placeholder = isStreaming
    ? 'AI 正在回复中...'
    : isInterrupt
      ? '输入回复以继续 Agent 执行，Enter 发送'
      : '输入消息，Shift+Enter 换行，Enter 发送'

  return (
    <div className="px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm">
      <form
        onSubmit={handleSubmit}
        className={`max-w-3xl mx-auto flex items-end gap-2 rounded-2xl border focus-within:ring-2 transition-all p-2 ${
          isInterrupt
            ? 'bg-amber-50 dark:bg-amber-900/10 border-amber-300 dark:border-amber-700 focus-within:border-amber-400 focus-within:ring-amber-100 dark:focus-within:ring-amber-900/50'
            : 'bg-slate-50 dark:bg-slate-800 border-slate-200 dark:border-slate-700 focus-within:border-brand-400 focus-within:ring-brand-100 dark:focus-within:ring-brand-900/50'
        }`}
      >
        <textarea
          ref={textareaRef}
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
          rows={1}
          className="flex-1 resize-none bg-transparent outline-none px-3 py-2 text-sm text-slate-800 dark:text-slate-100 placeholder:text-slate-400 disabled:opacity-50 max-h-[200px]"
        />
        {isStreaming ? (
          <button
            type="button"
            onClick={onStop}
            className="shrink-0 w-9 h-9 rounded-xl bg-red-500 hover:bg-red-600 text-white flex items-center justify-center transition-colors"
            title="停止生成"
          >
            <StopCircle size={18} />
          </button>
        ) : (
          <button
            type="submit"
            disabled={disabled || !text.trim()}
            className={`shrink-0 w-9 h-9 rounded-xl text-white flex items-center justify-center transition-colors ${
              isInterrupt
                ? 'bg-amber-600 hover:bg-amber-700 disabled:bg-amber-300 dark:disabled:bg-amber-800'
                : 'bg-brand-600 hover:bg-brand-700 disabled:bg-slate-300 dark:disabled:bg-slate-700'
            }`}
            title={isInterrupt ? '发送回复' : '发送'}
          >
            <Send size={16} />
          </button>
        )}
      </form>
      <p className="text-center text-xs text-slate-400 dark:text-slate-500 mt-2">
        Loom Chat · AI Agent 助手 · 由本地模型驱动
      </p>
    </div>
  )
}