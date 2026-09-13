import { useState } from 'react'
import { MessageSquare } from 'lucide-react'

const TENANT_KEY = 'loom-tenant-id'
const USER_KEY = 'loom-user-id'

export function getCredentials() {
  return {
    tenantId: localStorage.getItem(TENANT_KEY) || '',
    userId: localStorage.getItem(USER_KEY) || '',
  }
}

export function isLoggedIn() {
  const { tenantId, userId } = getCredentials()
  return Boolean(tenantId && userId)
}

export function clearCredentials() {
  localStorage.removeItem(TENANT_KEY)
  localStorage.removeItem(USER_KEY)
}

export default function Login({ onLogin }) {
  const [tenantId, setTenantId] = useState('')
  const [userId, setUserId] = useState('')
  const [error, setError] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    const t = tenantId.trim()
    const u = userId.trim()
    if (!t || !u) {
      setError('请输入租户 ID 和用户 ID')
      return
    }
    localStorage.setItem(TENANT_KEY, t)
    localStorage.setItem(USER_KEY, u)
    onLogin({ tenantId: t, userId: u })
  }

  return (
    <div className="min-h-full w-full flex items-center justify-center bg-gradient-to-br from-slate-100 via-brand-50 to-purple-50 dark:from-slate-950 dark:via-slate-900 dark:to-slate-950 p-4">
      <div className="w-full max-w-md">
        <div className="flex flex-col items-center mb-8">
          <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-brand-500 to-purple-600 flex items-center justify-center shadow-lg shadow-brand-500/30 mb-4">
            <MessageSquare size={28} className="text-white" />
          </div>
          <h1 className="text-2xl font-bold text-slate-800 dark:text-slate-100">Loom Chat</h1>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">登录以开始对话</p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="bg-white dark:bg-slate-900 rounded-2xl shadow-xl border border-slate-200 dark:border-slate-700 p-6 space-y-5"
        >
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
              租户 ID
            </label>
            <input
              type="text"
              value={tenantId}
              onChange={(e) => {
                setTenantId(e.target.value)
                setError('')
              }}
              placeholder="例如: tenant-001"
              className="w-full px-3.5 py-2.5 rounded-xl border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 text-slate-900 dark:text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent transition"
              autoComplete="off"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1.5">
              用户 ID
            </label>
            <input
              type="text"
              value={userId}
              onChange={(e) => {
                setUserId(e.target.value)
                setError('')
              }}
              placeholder="例如: user-001"
              className="w-full px-3.5 py-2.5 rounded-xl border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 text-slate-900 dark:text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent transition"
              autoComplete="off"
            />
          </div>

          {error && (
            <div className="text-sm text-red-500 bg-red-50 dark:bg-red-900/20 px-3 py-2 rounded-lg">
              {error}
            </div>
          )}

          <button
            type="submit"
            className="w-full py-2.5 rounded-xl bg-brand-600 hover:bg-brand-700 text-white font-medium transition-colors shadow-sm hover:shadow"
          >
            登录
          </button>

          <p className="text-xs text-center text-slate-400 dark:text-slate-500">
            租户 ID 和用户 ID 用于数据隔离，相同组合将共享会话与记忆
          </p>
        </form>
      </div>
    </div>
  )
}