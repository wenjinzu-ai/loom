import { MessageSquare, Plus, Trash2, PanelLeftClose, PanelLeftOpen, LogOut } from 'lucide-react'

export default function Sidebar({ sessions, currentSessionId, onSelect, onNew, onDelete, collapsed, onToggle, onLogout, credentials }) {
  return (
    <>
      {/* 侧边栏主体 */}
      <aside
        className={`${
          collapsed ? 'w-0 overflow-hidden' : 'w-72'
        } shrink-0 h-full bg-white dark:bg-slate-900 border-r border-slate-200 dark:border-slate-700 flex flex-col transition-all duration-300`}
      >
        {/* 头部 */}
        <div className="flex items-center justify-between px-4 h-14 border-b border-slate-200 dark:border-slate-700">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-brand-500 to-purple-600 flex items-center justify-center">
              <MessageSquare size={16} className="text-white" />
            </div>
            <span className="font-semibold text-slate-800 dark:text-slate-100">Loom Chat</span>
          </div>
          <button
            onClick={onToggle}
            className="p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500 transition-colors"
            title="收起侧边栏"
          >
            <PanelLeftClose size={18} />
          </button>
        </div>

        {/* 新建会话按钮 */}
        <div className="p-3">
          <button
            onClick={onNew}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl bg-brand-600 hover:bg-brand-700 text-white text-sm font-medium transition-colors shadow-sm hover:shadow"
          >
            <Plus size={16} />
            新建对话
          </button>
        </div>

        {/* 会话列表 */}
        <div className="flex-1 overflow-y-auto px-2 pb-4">
          <div className="px-2 py-1.5 text-xs font-medium text-slate-400 uppercase tracking-wider">
            历史对话
          </div>
          {sessions.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-slate-400">
              暂无历史对话
            </div>
          ) : (
            <div className="space-y-1">
              {sessions.map((s) => (
                <div
                  key={s.session_id}
                  onClick={() => onSelect(s.session_id)}
                  className={`group flex items-center gap-2 px-3 py-2.5 rounded-xl cursor-pointer transition-colors ${
                    s.session_id === currentSessionId
                      ? 'bg-brand-50 dark:bg-brand-900/30 text-brand-700 dark:text-brand-300'
                      : 'hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-700 dark:text-slate-300'
                  }`}
                >
                  <MessageSquare
                    size={16}
                    className={`shrink-0 ${
                      s.session_id === currentSessionId ? 'text-brand-500' : 'text-slate-400'
                    }`}
                  />
                  <span className="flex-1 truncate text-sm">{s.title}</span>
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      onDelete(s.session_id)
                    }}
                    className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-slate-400 hover:text-red-500 transition-all"
                    title="删除会话"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* 底部：用户信息 + 登出 */}
        <div className="border-t border-slate-200 dark:border-slate-700 p-3">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <div className="w-8 h-8 rounded-full bg-brand-100 dark:bg-brand-900/40 flex items-center justify-center shrink-0">
                <span className="text-xs font-semibold text-brand-700 dark:text-brand-300">
                  {credentials?.userId?.charAt(0)?.toUpperCase() || '?'}
                </span>
              </div>
              <div className="min-w-0">
                <div className="text-sm font-medium text-slate-700 dark:text-slate-200 truncate">
                  {credentials?.userId || '未登录'}
                </div>
                <div className="text-xs text-slate-400 truncate">
                  租户: {credentials?.tenantId || '-'}
                </div>
              </div>
            </div>
            <button
              onClick={onLogout}
              className="p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-800 text-slate-500 hover:text-red-500 transition-colors shrink-0"
              title="退出登录"
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </aside>

      {/* 收起后的展开按钮 */}
      {collapsed && (
        <button
          onClick={onToggle}
          className="absolute top-3 left-3 z-20 p-2 rounded-lg bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700 shadow-sm transition-colors"
          title="展开侧边栏"
        >
          <PanelLeftOpen size={18} />
        </button>
      )}
    </>
  )
}