import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { User, Bot, Copy, Check } from 'lucide-react'
import { useState } from 'react'
import ThinkingBlock from './ThinkingBlock.jsx'

export default function MessageItem({ message }) {
  const [copied, setCopied] = useState(false)
  const isUser = message.role === 'user'

  const copyText = () => {
    navigator.clipboard.writeText(message.content)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div
      className={`group flex gap-3 px-4 py-4 animate-slide-up ${
        isUser ? 'flex-row-reverse' : ''
      }`}
    >
      {/* 头像 */}
      <div
        className={`shrink-0 w-8 h-8 rounded-lg flex items-center justify-center ${
          isUser
            ? 'bg-brand-600 text-white'
            : 'bg-gradient-to-br from-brand-500 to-purple-600 text-white'
        }`}
      >
        {isUser ? <User size={16} /> : <Bot size={16} />}
      </div>

      {/* 消息内容 */}
      <div className={`flex flex-col max-w-[88%] ${isUser ? 'items-end' : 'items-start'}`}>
        {!isUser && (
          <ThinkingBlock
            thinking={message.thinking}
            isLoading={message.isLoading ?? (message.thinking && !message.content)}
            iterations={message.iterations}
            toolCalls={message.toolCalls}
            toolTrace={message.toolTrace}
          />
        )}

        {(message.content || message.isLoading || message.thinking) && (
          <div
            className={`relative px-4 py-2.5 rounded-2xl ${
              isUser
                ? 'bg-brand-600 text-white rounded-tr-sm'
                : 'bg-slate-100 dark:bg-slate-800 text-slate-800 dark:text-slate-100 rounded-tl-sm'
            }`}
          >
            {message.content ? (
              <div className="prose-chat">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
              </div>
            ) : message.isLoading ? (
              <span className="inline-block w-2 h-4 bg-brand-500 animate-pulse-slow rounded-sm" />
            ) : (
              <span className="text-slate-400 dark:text-slate-500 italic">思考中...</span>
            )}
          </div>
        )}

        {/* 复制按钮 */}
        {message.content && !isUser && (
          <button
            onClick={copyText}
            className="mt-1 opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-1 text-xs text-slate-400 hover:text-slate-600 dark:hover:text-slate-300"
          >
            {copied ? <Check size={12} /> : <Copy size={12} />}
            {copied ? '已复制' : '复制'}
          </button>
        )}
      </div>
    </div>
  )
}