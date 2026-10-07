import { createServer } from 'node:http'
import { Server } from 'socket.io'
import { loadConfig } from './config.mjs'
import { createPostgresStore } from './postgres-store.mjs'
import { probePlatform, validateSession, validId } from './platform-session.mjs'
import { createTelemetry, metricsAllowed, requestId } from './observability.mjs'

const CONTRACT = '1.0'
const TYPES = new Set(['INFO', 'SUCCESS', 'WARNING', 'SECURITY'])
const MAX_PENDING_HINTS = 1000
const roomName = (context) => `tenant:${context.tenantId}:org:${context.organizationId}:user:${context.userId}`
const sameIdentity = (left, right) => ['userId', 'tenantId', 'organizationId', 'sessionId'].every((key) => left[key] === right[key])

function requestPath(request) {
  try { return new URL(request.url ?? '/', 'http://notification.invalid').pathname } catch { return null }
}

function parseHint(raw) {
  if (typeof raw !== 'string' || Buffer.byteLength(raw) > 4096) return null
  let hint
  try { hint = JSON.parse(raw) } catch { return null }
  if (!hint || Array.isArray(hint) || Object.keys(hint).length !== 4 ||
      !['id', 'tenantId', 'organizationId', 'userId'].every((key) => validId(hint[key]))) return null
  return Object.fromEntries(Object.entries(hint).map(([key, value]) => [key, value.toLowerCase()]))
}

function eventFromRow(row, hint) {
  if (!row || !['APP', 'SYSTEM'].includes(row.sourceKind) ||
      !['id', 'tenantId', 'organizationId', 'userId'].every((key) => validId(row[key]) && row[key].toLowerCase() === hint[key]) ||
      !TYPES.has(row.type) || typeof row.title !== 'string' || !row.title ||
      typeof row.createdAt !== 'string' || !Number.isFinite(Date.parse(row.createdAt))) return null
  if (row.sourceKind === 'APP' && (!validId(row.applicationId) || !validId(row.publicationId))) return null
  if (row.sourceKind === 'SYSTEM' && (row.applicationId != null || row.publicationId != null)) return null
  if ((row.content != null && typeof row.content !== 'string') || (row.link != null && typeof row.link !== 'string')) return null
  return {
    contractVersion: CONTRACT, id: hint.id, userId: hint.userId, tenantId: hint.tenantId, organizationId: hint.organizationId,
    applicationId: row.applicationId?.toLowerCase() ?? null, publicationId: row.publicationId?.toLowerCase() ?? null,
    type: row.type, title: row.title, content: row.content ?? '', link: row.link ?? '', createdAt: row.createdAt,
  }
}

function authenticationToken(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data) || Object.keys(data).length !== 1 || typeof data.token !== 'string') return null
  const token = data.token.trim()
  if (!token || Buffer.byteLength(token) > 4096 || /[\s\x00-\x1f\x7f]/.test(token)) return null
  return token
}

export function createNotificationService(config, { store = createPostgresStore(config), logger = console } = {}) {
  const allowedOrigins = new Set(config.allowedOrigins)
  const states = new Map()
  const requestTraces = new WeakMap()
  const traceFor = (request) => {
    if (!requestTraces.has(request)) requestTraces.set(request, requestId(request.headers['x-request-id']))
    return requestTraces.get(request)
  }
  const lifetime = new AbortController()
  const checks = { database: 'UNKNOWN', listen: 'DOWN', platform: 'UNKNOWN' }
  let stopping = false
  let started = false
  let stopPromise
  let healthTimer
  let recheckTimer
  let probePromise
  let hintChain = Promise.resolve()
  let pendingHints = 0
  const snapshot = () => ({ status: Object.values(checks).every((value) => value === 'UP') ? 'UP' : 'DOWN', checks: { ...checks } })
  const telemetry = createTelemetry({ service: 'notification-ws', logger, routeFor: (request) => {
    const path = requestPath(request)
    return ['/health', '/health/live', '/metrics'].includes(path) ? path : path?.startsWith(config.socketPath) ? '/socket.io/' : 'OTHER'
  }, gauges: () => ({ saas_notification_clients: states.size,
    saas_notification_authenticated_clients: [...states.values()].filter((state) => state.context && !state.closed).length,
    saas_notification_pending_hints: pendingHints, saas_notification_listen_up: Number(checks.listen === 'UP'),
    saas_database_up: Number(checks.database === 'UP'), saas_platform_up: Number(checks.platform === 'UP'),
    saas_health_up: Number(snapshot().status === 'UP') }) })

  async function probeHealth() {
    if (probePromise) return probePromise
    probePromise = (async () => {
      await Promise.all([
        Promise.resolve().then(() => store.ping()).then(() => { checks.database = 'UP' }, () => { checks.database = 'DOWN' }),
        probePlatform(config, lifetime.signal).then(() => { checks.platform = 'UP' }, () => { checks.platform = 'DOWN' }),
      ])
      return snapshot()
    })().finally(() => { probePromise = null })
    return probePromise
  }

  const httpServer = createServer((request, response) => {
    const path = requestPath(request)
    const send = (status, body) => {
      if (response.destroyed) return
      response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' })
      response.end(JSON.stringify(body))
    }
    if (!path) { send(400, { message: 'Invalid request path' }); return }
    if (path === '/metrics' && request.method === 'GET') {
      if (!metricsAllowed(request.headers.authorization, config.metricsToken)) { send(401, { message: 'Metrics authentication required' }); return }
      response.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8', 'Cache-Control': 'no-store' })
      response.end(telemetry.render()); return
    }
    if (request.method === 'GET' && path === '/health/live') { send(200, { status: 'UP' }); return }
    if (request.method === 'GET' && path === '/health') {
      void probeHealth().then((health) => send(health.status === 'UP' ? 200 : 503, health), () => send(503, snapshot()))
      return
    }
    send(404, { message: 'Not found' })
  })

  const io = new Server(httpServer, {
    path: config.socketPath, serveClient: false, maxHttpBufferSize: 8192,
    cors: { origin: [...allowedOrigins], methods: ['GET', 'POST'], credentials: false },
    allowRequest: (request, callback) => callback(null, typeof request.headers.origin === 'string' && allowedOrigins.has(request.headers.origin)),
  })
  httpServer.prependListener('request', (request, response) => { telemetry.observe(request, response, { requestId: traceFor(request) }) })
  io.engine.on('headers', (headers, request) => { headers['X-Request-Id'] = traceFor(request) })
  httpServer.prependListener('upgrade', (request, socket) => {
    const path = requestPath(request)
    if (!path || (path.startsWith(config.socketPath) && !allowedOrigins.has(request.headers.origin))) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n')
      socket.destroy()
    }
  })

  function reject(socket, state) {
    if (state.closed) return
    state.closed = true
    if (!stopping) telemetry.count('saas_notification_auth_denials_total')
    state.abort.abort()
    clearTimeout(state.waitTimer)
    clearTimeout(state.expiryTimer)
    if (socket.connected) {
      socket.emit('unauthorized', { code: 'SESSION_INVALID', message: '实时通知会话已失效' })
      socket.disconnect(true)
    }
  }

  function armExpiry(socket, state, context) {
    state.expiresAt = Math.min(state.expiresAt ?? Infinity, Date.parse(context.expiresAt))
    clearTimeout(state.expiryTimer)
    if (state.expiresAt <= Date.now()) { reject(socket, state); return false }
    state.expiryTimer = setTimeout(() => reject(socket, state), Math.min(2 ** 31 - 1, state.expiresAt - Date.now()))
    state.expiryTimer.unref?.()
    return true
  }

  async function validateBound(socket, state, notificationId) {
    if (stopping || state.closed || !socket.connected || !state.context || state.expiresAt <= Date.now()) {
      if (!state.closed && state.expiresAt <= Date.now()) reject(socket, state)
      return null
    }
    try {
      const context = await validateSession(config, state.token, notificationId, AbortSignal.any([lifetime.signal, state.abort.signal]), state)
      if (stopping || state.closed || !socket.connected) return null
      if (!sameIdentity(context, state.context)) { reject(socket, state); return null }
      return armExpiry(socket, state, context) ? context : null
    } catch (error) {
      if (state.closed || stopping) return null
      // App-specific rejection stops this hint. Other allowed app hints can
      // continue on the valid session; idle validation has no such exemption.
      if (notificationId && error.kind !== 'SESSION_INVALID') return null
      reject(socket, state)
      return null
    }
  }

  io.on('connection', (socket) => {
    const state = { attempted: false, closed: false, context: null, token: null, abort: new AbortController(), rechecking: false,
      requestId: traceFor(socket.request) }
    states.set(socket.id, state)
    state.waitTimer = setTimeout(() => reject(socket, state), config.authWaitMs)
    state.waitTimer.unref?.()
    socket.on('authenticate', async (data) => {
      if (state.attempted || state.closed) { reject(socket, state); return }
      state.attempted = true
      const token = authenticationToken(data)
      if (!token) { reject(socket, state); return }
      state.token = token
      try {
        const context = await validateSession(config, token, undefined, AbortSignal.any([lifetime.signal, state.abort.signal]), state)
        if (stopping || state.closed || !socket.connected) return
        state.context = context
        if (!armExpiry(socket, state, context)) return
        await socket.join(roomName(context))
        if (stopping || state.closed || !socket.connected) return
        clearTimeout(state.waitTimer)
        const { sessionId: _sessionId, ...publicContext } = context
        socket.emit('authenticated', publicContext)
      } catch { reject(socket, state) }
    })
    socket.on('disconnect', () => {
      state.closed = true
      state.token = null
      state.abort.abort()
      clearTimeout(state.waitTimer)
      clearTimeout(state.expiryTimer)
      states.delete(socket.id)
    })
  })

  async function deliverHint(hint) {
    if (stopping) return false
    let row
    try { row = await store.getNotification(hint) } catch {
      checks.database = 'DOWN'
      telemetry.count('saas_notification_hint_errors_total')
      logger.warn('[notification-ws] notification lookup unavailable; hint dropped')
      return false
    }
    const notification = eventFromRow(row, hint)
    if (!notification) return false
    const sockets = [...(io.sockets.adapter.rooms.get(roomName(hint)) ?? [])]
    await Promise.all(sockets.map(async (socketId) => {
      const socket = io.sockets.sockets.get(socketId)
      const state = states.get(socketId)
      if (!socket || !state || !await validateBound(socket, state, notification.id)) return
      if (!stopping && !state.closed && socket.connected && state.expiresAt > Date.now()) socket.emit('notification', notification)
    }))
    return true
  }

  function handleHint(raw) {
    const hint = parseHint(raw)
    if (!hint || stopping) return Promise.resolve(false)
    if (pendingHints >= MAX_PENDING_HINTS) { telemetry.count('saas_notification_hint_drops_total'); logger.warn('[notification-ws] realtime hint backlog full; hint dropped'); return Promise.resolve(false) }
    pendingHints++
    const pending = hintChain.then(() => deliverHint(hint)).finally(() => { pendingHints-- })
    hintChain = pending.catch(() => logger.warn('[notification-ws] realtime hint handling failed'))
    return pending
  }

  return {
    io, httpServer, handleHint, probeHealth, status: snapshot,
    async start() {
      if (started) return httpServer.address()
      started = true
      await new Promise((resolve, reject) => {
        httpServer.once('error', reject)
        httpServer.listen(config.port, config.host, () => { httpServer.removeListener('error', reject); resolve() })
      })
      void store.start(handleHint, (listening) => { checks.listen = listening ? 'UP' : 'DOWN' }).catch(() => { checks.listen = 'DOWN' })
      void probeHealth()
      healthTimer = setInterval(() => { void probeHealth() }, config.healthProbeMs)
      recheckTimer = setInterval(() => {
        for (const [socketId, state] of states) {
          const socket = io.sockets.sockets.get(socketId)
          if (!socket || !state.context || state.closed || state.rechecking) continue
          state.rechecking = true
          void validateBound(socket, state).finally(() => { state.rechecking = false })
        }
      }, config.recheckMs)
      healthTimer.unref?.()
      recheckTimer.unref?.()
      logger.info('[notification-ws] Socket.IO service listening')
      return httpServer.address()
    },
    stop() {
      if (stopPromise) return stopPromise
      stopping = true
      lifetime.abort()
      clearInterval(healthTimer)
      clearInterval(recheckTimer)
      for (const [socketId, state] of states) {
        const socket = io.sockets.sockets.get(socketId)
        if (socket) reject(socket, state)
      }
      stopPromise = Promise.all([
        new Promise((resolve) => io.close(resolve)),
        store.stop(),
      ]).then(() => {})
      return stopPromise
    },
  }
}

export async function runFromEnvironment() {
  let service
  try {
    service = createNotificationService(loadConfig())
    await service.start()
  } catch (error) {
    const configurationError = ['NOTIFICATION_', 'DATABASE_URL', 'PLATFORM_API_BASE_URL'].some((name) => error.message?.startsWith(name))
    console.error(`[notification-ws] startup failed: ${error.code ?? (configurationError ? error.message : 'invalid configuration or unavailable listener')}`)
    await service?.stop().catch(() => {})
    process.exitCode = 1
    return
  }
  const shutdown = () => { void service.stop().catch(() => { process.exitCode = 1 }) }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}
