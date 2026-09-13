// SSE 流式聊天 API 客户端

const BASE_URL = ''

const TENANT_KEY = 'loom-tenant-id'
const USER_KEY = 'loom-user-id'

function getCredentials() {
  return {
    tenant_id: localStorage.getItem(TENANT_KEY) || undefined,
    user_id: localStorage.getItem(USER_KEY) || undefined,
  }
}

function withQuery(url) {
  const { tenant_id, user_id } = getCredentials()
  const params = new URLSearchParams()
  if (tenant_id) params.set('tenant_id', tenant_id)
  if (user_id) params.set('user_id', user_id)
  const qs = params.toString()
  return qs ? `${url}?${qs}` : url
}

/**
 * 发送聊天消息，通过 SSE 流式接收响应
 * @param {Object} params
 * @param {string} params.message - 用户消息
 * @param {string} [params.sessionId] - 会话 ID（多轮对话）
 * @param {Object} callbacks - 事件回调
 * @param {Function} callbacks.onThinking - thinking 事件回调
 * @param {Function} callbacks.onMessage - message 事件回调
 * @param {Function} callbacks.onInterrupt - interrupt 事件回调（HITL 暂停）
 * @param {Function} callbacks.onDone - done 事件回调
 * @param {Function} callbacks.onError - error 事件回调
 * @returns {Function} 取消函数
 */
export function chatStream({ message, sessionId }, callbacks) {
  const controller = new AbortController()
  const { onThinking, onMessage, onInterrupt, onDone, onError, onProgress } = callbacks
  const creds = getCredentials()

  ;(async () => {
    try {
      const res = await fetch(`${BASE_URL}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message,
          session_id: sessionId || undefined,
          tenant_id: creds.tenant_id,
          user_id: creds.user_id,
        }),
        signal: controller.signal,
      })

      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })

        // SSE 事件以 \n\n 分隔
        const events = buffer.split('\n\n')
        buffer = events.pop() || ''

        for (const raw of events) {
          const parsed = parseSseEvent(raw)
          if (!parsed) continue

          const { event, data } = parsed
          switch (event) {
            case 'thinking':
              onThinking?.(data)
              break
            case 'progress':
              onProgress?.(data)
              break
            case 'message':
              onMessage?.(data)
              break
            case 'interrupt':
              onInterrupt?.(data)
              break
            case 'error':
              onError?.(data)
              break
            case 'done':
              onDone?.(data)
              break
          }
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        onError?.({ error: err.message })
      }
    }
  })()

  return () => controller.abort()
}

/**
 * 恢复被 interrupt 暂停的 Agent 执行（HITL）
 * @param {Object} params
 * @param {string} params.sessionId - 会话 ID
 * @param {string} params.checkpointId - 检查点 ID
 * @param {string} params.threadId - 线程 ID
 * @param {*} params.resumeValue - 用户提供的恢复值
 * @param {Object} callbacks - 事件回调（同 chatStream）
 * @returns {Function} 取消函数
 */
export function resumeStream({ sessionId, checkpointId, threadId, resumeValue }, callbacks) {
  const controller = new AbortController()
  const { onMessage, onInterrupt, onDone, onError, onProgress } = callbacks
  const creds = getCredentials()

  ;(async () => {
    try {
      const res = await fetch(`${BASE_URL}/sessions/${encodeURIComponent(sessionId)}/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          checkpoint_id: checkpointId,
          thread_id: threadId,
          resume_value: resumeValue,
          tenant_id: creds.tenant_id,
          user_id: creds.user_id,
        }),
        signal: controller.signal,
      })

      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`)
      }

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })

        const events = buffer.split('\n\n')
        buffer = events.pop() || ''

        for (const raw of events) {
          const parsed = parseSseEvent(raw)
          if (!parsed) continue

          const { event, data } = parsed
          switch (event) {
            case 'progress':
              onProgress?.(data)
              break
            case 'message':
              onMessage?.(data)
              break
            case 'interrupt':
              onInterrupt?.(data)
              break
            case 'error':
              onError?.(data)
              break
            case 'done':
              onDone?.(data)
              break
          }
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        onError?.({ error: err.message })
      }
    }
  })()

  return () => controller.abort()
}

// ---------- 会话管理 API ----------

/** 获取会话列表 */
export async function getSessions() {
  const res = await fetch(withQuery(`${BASE_URL}/sessions`))
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/** 获取单个会话详情（含消息历史） */
export async function getSession(sessionId) {
  const res = await fetch(withQuery(`${BASE_URL}/sessions/${encodeURIComponent(sessionId)}`))
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/** 删除会话 */
export async function deleteSession(sessionId) {
  const res = await fetch(withQuery(`${BASE_URL}/sessions/${encodeURIComponent(sessionId)}`), {
    method: 'DELETE',
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
}

/** 重命名会话 */
export async function renameSession(sessionId, title) {
  const res = await fetch(withQuery(`${BASE_URL}/sessions/${encodeURIComponent(sessionId)}`), {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title }),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
}

function parseSseEvent(raw) {
  const lines = raw.split('\n')
  let event = 'message'
  let data = ''
  let hasEvent = false

  for (const line of lines) {
    if (line.startsWith('event:')) {
      event = line.slice(6).trim()
      hasEvent = true
    } else if (line.startsWith('data:')) {
      data += line.slice(5).trim()
    }
  }

  // done 事件没有 data，不能跳过
  if (!hasEvent && !data) return null

  if (!data) return { event, data: null }

  try {
    return { event, data: JSON.parse(data) }
  } catch {
    return { event, data: { text: data } }
  }
}