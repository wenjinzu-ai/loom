import { useEffect, useRef, useState } from 'react'
import MessageItem from './MessageItem.jsx'
import ChatInput from './ChatInput.jsx'
import { Sparkles, AlertCircle, Check, X } from 'lucide-react'

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

  // 判断 interrupt 是否为结构化审批卡片（用于决定是否禁用底部输入框）
  const interruptParsed = interruptInfo ? parseInterruptValue(interruptInfo.value) : null
  const isStructuredInterrupt = interruptParsed?.isStructured === true

  // 解析 interrupt value，判断结构化类型
  // 支持三种格式：
  // 1. 选项按钮: { prompt, options: [{label, value}], allow_input? }
  // 2. 自由输入: { prompt, allow_input: true }
  // 3. 多字段表单: { prompt, fields: [{name, label, type, required?, placeholder?, options?}] }
  function parseInterruptValue(value) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const hasOptions = Array.isArray(value.options) && value.options.length > 0
      const hasFields = Array.isArray(value.fields) && value.fields.length > 0
      if (value.prompt || hasOptions || value.allow_input === true || hasFields) {
        const base = {
          isStructured: true,
          prompt: typeof value.prompt === 'string' ? value.prompt : '',
        }
        if (hasFields) {
          // 表单模式
          return {
            ...base,
            mode: 'form',
            fields: value.fields
              .filter((f) => f && typeof f.name === 'string' && typeof f.label === 'string')
              .map((f) => ({
                name: f.name,
                label: f.label,
                type: ['text', 'textarea', 'select', 'number'].includes(f.type)
                  ? f.type
                  : 'text',
                required: f.required === true,
                placeholder: typeof f.placeholder === 'string' ? f.placeholder : '',
                options: Array.isArray(f.options)
                  ? f.options
                      .filter((o) => o && typeof o.label === 'string')
                      .map((o) => ({ label: o.label, value: o.value }))
                  : [],
                default: f.default,
              })),
          }
        }
        // 选项按钮 / 自由输入模式
        return {
          ...base,
          mode: hasOptions ? 'options' : 'input',
          options: hasOptions
            ? value.options
                .filter((o) => o && typeof o.label === 'string')
                .map((o) => ({ label: o.label, value: o.value }))
            : [],
          allowInput: value.allow_input === true,
        }
      }
    }
    return { isStructured: false }
  }

  // 格式化 interrupt value 为可展示文本（非结构化时使用）
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
          <div className="max-w-7xl mx-auto py-4">
            {messages.map((m, i) => (
              <MessageItem key={i} message={m} />
            ))}
            {/* HITL 中断提示：Agent 暂停等待人工输入 */}
            {interruptInfo && (
              <InterruptCard
                value={interruptInfo.value}
                onResume={onResume}
                formatValue={formatInterruptValue}
                parseValue={parseInterruptValue}
              />
            )}
          </div>
        )}
      </div>

      {/* 输入框 */}
      <ChatInput
        onSend={onSend}
        disabled={isStreaming || isStructuredInterrupt}
        isStreaming={isStreaming}
        onStop={onStop}
        isInterrupt={!!interruptInfo && !isStructuredInterrupt}
        onResume={onResume}
      />
    </div>
  )
}

// 人工审批卡片：支持选项按钮、自由输入、多字段表单三种模式
function InterruptCard({ value, onResume, formatValue, parseValue }) {
  const parsed = parseValue(value)
  const [inputText, setInputText] = useState('')
  // 表单字段值
  const [formValues, setFormValues] = useState({})

  if (!parsed.isStructured) {
    // 非结构化：展示原始 value，用户在底部输入框回复
    return (
      <div className="my-4 mx-auto max-w-7xl">
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
                {formatValue(value)}
              </div>
              <p className="text-xs text-amber-600 dark:text-amber-400 mt-2">
                请在下方输入框中输入你的回复，将作为 interrupt 的返回值。
              </p>
            </div>
          </div>
        </div>
      </div>
    )
  }

  const { prompt, mode } = parsed

  // 判断选项按钮颜色（仅用通用确认/取消类动词，不绑定具体场景）
  const optionStyle = (label) => {
    if (/确认|同意|是|Yes|Approve|OK|Submit|提交|继续|执行|允许/i.test(label)) {
      return { cls: 'bg-green-600 hover:bg-green-700 border-green-600', icon: <Check size={14} /> }
    }
    if (/取消|拒绝|否|No|Cancel|Abort|Stop|停止|撤销/i.test(label)) {
      return { cls: 'bg-red-500 hover:bg-red-600 border-red-500', icon: <X size={14} /> }
    }
    return { cls: 'bg-amber-600 hover:bg-amber-700 border-amber-600', icon: null }
  }

  // 选项按钮模式：点击即提交
  if (mode === 'options') {
    const { options, allowInput } = parsed
    return (
      <div className="my-4 mx-auto max-w-7xl">
        <div className="rounded-2xl border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-4 shadow-sm">
          <div className="flex items-start gap-3">
            <div className="shrink-0 w-8 h-8 rounded-full bg-amber-500 flex items-center justify-center text-white">
              <AlertCircle size={18} />
            </div>
            <div className="flex-1 min-w-0">
              <div className="font-medium text-amber-800 dark:text-amber-200 mb-1">
                需要你的确认
              </div>
              {prompt && (
                <p className="text-sm text-amber-700 dark:text-amber-300 mb-3 whitespace-pre-wrap">
                  {prompt}
                </p>
              )}
              {options.length > 0 && (
                <div className="flex flex-wrap gap-2">
                  {options.map((opt, idx) => {
                    const style = optionStyle(opt.label)
                    return (
                      <button
                        key={idx}
                        onClick={() => onResume(opt.value)}
                        className={`px-4 py-2 rounded-lg text-sm font-medium border text-white shadow-sm transition-all flex items-center gap-1.5 ${style.cls}`}
                      >
                        {style.icon}
                        {opt.label}
                      </button>
                    )
                  })}
                </div>
              )}
              {allowInput && (
                <div className="flex gap-2 mt-3">
                  <textarea
                    value={inputText}
                    onChange={(e) => setInputText(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault()
                        if (inputText.trim()) onResume(inputText.trim())
                      }
                    }}
                    placeholder={options.length > 0 ? '或输入其他回复...' : '输入你的回复...'}
                    rows={1}
                    className="flex-1 rounded-lg border border-amber-300 dark:border-amber-700 bg-white/70 dark:bg-slate-900/60 px-3 py-2 text-sm text-slate-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-amber-300 dark:focus:ring-amber-700 resize-none"
                  />
                  <button
                    onClick={() => inputText.trim() && onResume(inputText.trim())}
                    disabled={!inputText.trim()}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-all shrink-0 ${
                      inputText.trim()
                        ? 'bg-amber-600 hover:bg-amber-700 text-white shadow-sm'
                        : 'bg-slate-300 dark:bg-slate-700 text-slate-500 dark:text-slate-400 cursor-not-allowed'
                    }`}
                  >
                    发送
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    )
  }

  // 自由输入模式
  if (mode === 'input') {
    return (
      <div className="my-4 mx-auto max-w-7xl">
        <div className="rounded-2xl border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-4 shadow-sm">
          <div className="flex items-start gap-3">
            <div className="shrink-0 w-8 h-8 rounded-full bg-amber-500 flex items-center justify-center text-white">
              <AlertCircle size={18} />
            </div>
            <div className="flex-1 min-w-0">
              <div className="font-medium text-amber-800 dark:text-amber-200 mb-1">
                需要你的输入
              </div>
              {prompt && (
                <p className="text-sm text-amber-700 dark:text-amber-300 mb-3 whitespace-pre-wrap">
                  {prompt}
                </p>
              )}
              <div className="flex gap-2">
                <textarea
                  value={inputText}
                  onChange={(e) => setInputText(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault()
                      if (inputText.trim()) onResume(inputText.trim())
                    }
                  }}
                  placeholder="输入你的回复..."
                  rows={2}
                  className="flex-1 rounded-lg border border-amber-300 dark:border-amber-700 bg-white/70 dark:bg-slate-900/60 px-3 py-2 text-sm text-slate-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-amber-300 dark:focus:ring-amber-700 resize-none"
                />
                <button
                  onClick={() => inputText.trim() && onResume(inputText.trim())}
                  disabled={!inputText.trim()}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-all shrink-0 self-end ${
                    inputText.trim()
                      ? 'bg-amber-600 hover:bg-amber-700 text-white shadow-sm'
                      : 'bg-slate-300 dark:bg-slate-700 text-slate-500 dark:text-slate-400 cursor-not-allowed'
                  }`}
                >
                  发送
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  // 多字段表单模式
  if (mode === 'form') {
    const { fields } = parsed

    const setField = (name, val) => {
      setFormValues((prev) => ({ ...prev, [name]: val }))
    }

    // 初始化默认值
    const ensureDefaults = () => {
      const defaults = {}
      let changed = false
      for (const f of fields) {
        if (!(f.name in formValues) && f.default !== undefined) {
          defaults[f.name] = f.default
          changed = true
        }
      }
      if (changed) setFormValues((prev) => ({ ...prev, ...defaults }))
    }
    ensureDefaults()

    const allRequiredFilled = fields
      .filter((f) => f.required)
      .every((f) => {
        const v = formValues[f.name]
        return v !== undefined && v !== null && String(v).trim() !== ''
      })

    const handleSubmit = () => {
      const result = {}
      for (const f of fields) {
        const v = formValues[f.name]
        result[f.name] = v === undefined ? '' : v
      }
      onResume(result)
    }

    return (
      <div className="my-4 mx-auto max-w-7xl">
        <div className="rounded-2xl border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-4 shadow-sm">
          <div className="flex items-start gap-3">
            <div className="shrink-0 w-8 h-8 rounded-full bg-amber-500 flex items-center justify-center text-white">
              <AlertCircle size={18} />
            </div>
            <div className="flex-1 min-w-0">
              <div className="font-medium text-amber-800 dark:text-amber-200 mb-1">
                需要你补充信息
              </div>
              {prompt && (
                <p className="text-sm text-amber-700 dark:text-amber-300 mb-3 whitespace-pre-wrap">
                  {prompt}
                </p>
              )}

              <div className="space-y-3">
                {fields.map((f) => (
                  <div key={f.name}>
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-1">
                      {f.label}
                      {f.required && <span className="text-red-500 ml-0.5">*</span>}
                    </label>
                    {f.type === 'textarea' ? (
                      <textarea
                        value={formValues[f.name] ?? ''}
                        onChange={(e) => setField(f.name, e.target.value)}
                        placeholder={f.placeholder}
                        rows={3}
                        className="w-full rounded-lg border border-amber-300 dark:border-amber-700 bg-white/70 dark:bg-slate-900/60 px-3 py-2 text-sm text-slate-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-amber-300 dark:focus:ring-amber-700 resize-y"
                      />
                    ) : f.type === 'select' ? (
                      <select
                        value={formValues[f.name] ?? ''}
                        onChange={(e) => setField(f.name, e.target.value)}
                        className="w-full rounded-lg border border-amber-300 dark:border-amber-700 bg-white/70 dark:bg-slate-900/60 px-3 py-2 text-sm text-slate-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-amber-300 dark:focus:ring-amber-700"
                      >
                        <option value="">{f.placeholder || '请选择...'}</option>
                        {f.options.map((o, i) => (
                          <option key={i} value={o.value}>
                            {o.label}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type={f.type === 'number' ? 'number' : 'text'}
                        value={formValues[f.name] ?? ''}
                        onChange={(e) =>
                          setField(
                            f.name,
                            f.type === 'number' ? e.target.value : e.target.value,
                          )
                        }
                        placeholder={f.placeholder}
                        className="w-full rounded-lg border border-amber-300 dark:border-amber-700 bg-white/70 dark:bg-slate-900/60 px-3 py-2 text-sm text-slate-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-amber-300 dark:focus:ring-amber-700"
                      />
                    )}
                  </div>
                ))}
              </div>

              <button
                onClick={handleSubmit}
                disabled={!allRequiredFilled}
                className={`mt-4 px-5 py-2 rounded-lg text-sm font-medium transition-all flex items-center gap-1.5 ${
                  allRequiredFilled
                    ? 'bg-amber-600 hover:bg-amber-700 text-white shadow-sm'
                    : 'bg-slate-300 dark:bg-slate-700 text-slate-500 dark:text-slate-400 cursor-not-allowed'
                }`}
              >
                <Check size={15} />
                提交
              </button>
            </div>
          </div>
        </div>
      </div>
    )
  }

  return null
}