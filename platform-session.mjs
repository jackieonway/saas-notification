const MAX_RESPONSE_BYTES = 4096
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export class SessionValidationError extends Error {
  constructor(kind, status = 0) {
    super('Platform session validation failed')
    this.name = 'SessionValidationError'
    this.kind = kind
    this.status = status
  }
}

export const validId = (value) => typeof value === 'string' && UUID.test(value)

async function readJson(response, signal) {
  const length = Number(response.headers.get('content-length'))
  if (length > MAX_RESPONSE_BYTES || !/^application\/json(?:;|$)/i.test(response.headers.get('content-type') ?? '')) {
    await response.body?.cancel()
    throw new SessionValidationError('CONTRACT')
  }
  const reader = response.body?.getReader()
  if (!reader) throw new SessionValidationError('CONTRACT')
  const parts = []
  let bytes = 0
  try {
    while (true) {
      signal.throwIfAborted()
      const { done, value } = await reader.read()
      if (done) break
      bytes += value.byteLength
      if (bytes > MAX_RESPONSE_BYTES) {
        await reader.cancel()
        throw new SessionValidationError('CONTRACT')
      }
      parts.push(value)
    }
    const body = Buffer.concat(parts).toString('utf8')
    try { return JSON.parse(body) } catch { throw new SessionValidationError('CONTRACT') }
  } finally { reader.releaseLock() }
}

export async function validateSession(config, token, notificationId, parentSignal) {
  const timeout = AbortSignal.timeout(config.authTimeoutMs)
  const signal = parentSignal ? AbortSignal.any([timeout, parentSignal]) : timeout
  const url = new URL('/api/notifications/socket-context', config.platformApiBaseUrl)
  if (notificationId) url.searchParams.set('notificationId', notificationId)
  try {
    const response = await fetch(url, {
      method: 'GET', redirect: 'error', signal,
      headers: { Accept: 'application/json', Authorization: `Bearer ${token}` },
    })
    if (response.status !== 200) {
      await response.body?.cancel()
      const kind = notificationId && [403, 404].includes(response.status) ? 'DELIVERY_DENIED'
        : [401, 403].includes(response.status) ? 'SESSION_INVALID' : 'UNAVAILABLE'
      throw new SessionValidationError(kind, response.status)
    }
    const data = await readJson(response, signal)
    if (!data || data.contractVersion !== '1.0' ||
        !['userId', 'tenantId', 'organizationId', 'sessionId'].every((key) => validId(data[key])) ||
        typeof data.expiresAt !== 'string' || !Number.isFinite(Date.parse(data.expiresAt))) throw new SessionValidationError('CONTRACT')
    if (Date.parse(data.expiresAt) <= Date.now()) throw new SessionValidationError('SESSION_INVALID')
    return {
      contractVersion: '1.0',
      userId: data.userId.toLowerCase(), tenantId: data.tenantId.toLowerCase(),
      organizationId: data.organizationId.toLowerCase(), sessionId: data.sessionId.toLowerCase(),
      expiresAt: data.expiresAt,
    }
  } catch (error) {
    if (error instanceof SessionValidationError) throw error
    throw new SessionValidationError('UNAVAILABLE')
  }
}

export async function probePlatform(config, parentSignal) {
  const signal = parentSignal ? AbortSignal.any([AbortSignal.timeout(config.authTimeoutMs), parentSignal]) : AbortSignal.timeout(config.authTimeoutMs)
  const url = new URL('/api/notifications/socket-context', config.platformApiBaseUrl)
  const response = await fetch(url, { method: 'GET', redirect: 'error', signal, headers: { Accept: 'application/json' } })
  await response.body?.cancel()
  // This confirms the protected route is reachable, never that a user is valid.
  if (response.status !== 401) throw new Error('Platform probe failed')
}
