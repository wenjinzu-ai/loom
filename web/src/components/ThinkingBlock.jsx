import { useState } from 'react'
import { ChevronDown, ChevronRight, Brain, Loader2, Wrench, AlertTriangle } from 'lucide-react'

function ToolCallItem({ entry, index }) {
  const [expanded, setExpanded] = useState(false)
  const hasArgs = entry.arguments != null && entry.arguments !== undefined
  const argsText = hasArgs ? JSON.stringify(entry.arguments, null, 2) : ''
  const isDelegate = /delegate|spawn|agent/i.test(entry.tool)
  const resultText = entry.result || entry.preview || ''
  const hasResult = resultText.length > 0

  return (
    <div className="mt-2 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full flex items-center gap-2 px-2.5 py-1.5 bg-slate-50 dark:bg-slate-800/50 hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors text-left"
      >
        {expanded ? (
          <ChevronDown size={12} className="shrink-0 text-slate-400" />
        ) : (
          <ChevronRight size={12} className="shrink-0 text-slate-400" />
        )}
        {entry.running ? (
          <Loader2 size={12} className="animate-spin text-brand-500 shrink-0" />
        ) : (
          <Wrench size={12} className={isDelegate ? 'text-purple-500' : 'text-brand-500 shrink-0'} />
        )}
        <span className="text-xs font-mono font-medium text-slate-700 dark:text-slate-300 truncate">
          {entry.tool}
        </span>
        {entry.running && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-brand-100 dark:bg-brand-900/40 text-brand-700 dark:text-brand-300">
            执行中
          </span>
        )}
        {isDelegate && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300">
            委托
          </span>
        )}
        {entry.is_error && (
          <AlertTriangle size={12} className="text-red-500 ml-auto shrink-0" />
        )}
        {!entry.running && !entry.is_error && hasResult && (
          <span className="text-[10px] text-green-600 dark:text-green-400 ml-auto shrink-0">✓</span>
        )}
        <span className="text-[10px] text-slate-400 ml-auto shrink-0">#{index + 1}</span>
      </button>

      {expanded && (
        <div className="px-2.5 py-2 space-y-2 animate-fade-in">
          {hasArgs && (
            <div>
              <div className="text-[10px] uppercase tracking-wide text-slate-400 mb-1">入参</div>
              <pre className="text-[11px] font-mono bg-slate-100 dark:bg-slate-900 rounded p-2 overflow-x-auto text-slate-700 dark:text-slate-300 max-h-60 overflow-y-auto">
                {argsText}
              </pre>
            </div>
          )}
          {entry.running ? (
            <div className="flex items-center gap-1.5 text-[11px] text-slate-400">
              <Loader2 size={11} className="animate-spin" />
              等待工具返回结果...
            </div>
          ) : hasResult ? (
            <div>
              <div className="text-[10px] uppercase tracking-wide text-slate-400 mb-1">
                {isDelegate ? '委托结果' : '出参'}
              </div>
              <pre className="text-[11px] font-mono bg-slate-100 dark:bg-slate-900 rounded p-2 overflow-x-auto text-slate-700 dark:text-slate-300 max-h-60 overflow-y-auto whitespace-pre-wrap break-all">
                {resultText}
              </pre>
            </div>
          ) : null}
        </div>
      )}
    </div>
  )
}

export default function ThinkingBlock({ thinking, isLoading, iterations, toolCalls, toolTrace }) {
  const [expanded, setExpanded] = useState(false)

  const hasToolTrace = toolTrace && toolTrace.length > 0
  const hasContent = thinking || isLoading || iterations != null || toolCalls != null || hasToolTrace

  if (!hasContent) return null

  const summary = isLoading
    ? '正在思考中...'
    : thinking
      ? thinking.length > 60
        ? thinking.slice(0, 60) + '...'
        : thinking
      : hasToolTrace
        ? `调用 ${toolTrace.length} 个工具`
        : iterations != null
          ? `迭代 ${iterations} 轮，调用 ${toolCalls ?? 0} 个工具`
          : '已完成思考'

  return (
    <div className="mb-2">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-1.5 text-xs text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-300 transition-colors group"
      >
        {expanded ? (
          <ChevronDown size={14} className="shrink-0" />
        ) : (
          <ChevronRight size={14} className="shrink-0" />
        )}
        <Brain size={13} className="shrink-0 text-brand-500" />
        <span className="font-medium">思考过程</span>
        {isLoading && <Loader2 size={12} className="animate-spin ml-1" />}
        <span className="text-slate-400 dark:text-slate-500 truncate">— {summary}</span>
      </button>

      {expanded && (
        <div className="mt-1.5 ml-5 pl-3 border-l-2 border-brand-200 dark:border-brand-800 animate-fade-in">
          {thinking && (
            <p className="text-xs text-slate-600 dark:text-slate-400 leading-relaxed whitespace-pre-wrap">
              {thinking}
            </p>
          )}
          {(iterations != null || toolCalls != null) && (
            <div className="mt-2 flex flex-wrap gap-2">
              {iterations != null && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-slate-100 dark:bg-slate-800 text-xs text-slate-600 dark:text-slate-400">
                  迭代 {iterations} 轮
                </span>
              )}
              {toolCalls != null && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-slate-100 dark:bg-slate-800 text-xs text-slate-600 dark:text-slate-400">
                  工具调用 {toolCalls} 次
                </span>
              )}
            </div>
          )}
          {hasToolTrace && (
            <div className="mt-2">
              <div className="text-[10px] uppercase tracking-wide text-slate-400 mb-1.5">
                工具调用详情
              </div>
              {toolTrace.map((entry, i) => (
                <ToolCallItem key={i} entry={entry} index={i} />
              ))}
            </div>
          )}
          {!thinking && !iterations && !toolCalls && !hasToolTrace && isLoading && (
            <p className="text-xs text-slate-400 dark:text-slate-500 italic">思考中...</p>
          )}
        </div>
      )}
    </div>
  )
}