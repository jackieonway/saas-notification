import { metricsToken } from './observability.mjs'

function integer(env, name, fallback, minimum, maximum) {
  const raw = env[name] ?? String(fallback)
  if (!/^\d+$/.test(raw) || Number(raw) < minimum || Number(raw) > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`)
  }
  return Number(raw)
}

function origin(value, name, production) {
  try {
    const url = new URL(value)
    if (url.username || url.password || url.origin !== value || (url.protocol !== 'https:' &&
        !(url.protocol === 'http:' && !production && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)))) throw new Error()
    return url.origin
  } catch { throw new Error(`${name} must contain exact HTTPS origins (development permits loopback HTTP) without paths or credentials`) }
}

export function loadConfig(env = process.env) {
  const production = env.NODE_ENV === 'production'
  if (production && env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('NODE_TLS_REJECT_UNAUTHORIZED cannot disable production TLS verification')
  const databaseUrl = env.DATABASE_URL?.trim()
  const platformApiBaseUrl = env.PLATFORM_API_BASE_URL?.trim()
  const allowed = env.NOTIFICATION_ALLOWED_ORIGINS?.trim()
  if (!databaseUrl) throw new Error('DATABASE_URL is required')
  try {
    if (!['postgres:', 'postgresql:'].includes(new URL(databaseUrl).protocol)) throw new Error()
  } catch { throw new Error('DATABASE_URL must be a PostgreSQL connection URL') }
  if (!platformApiBaseUrl) throw new Error('PLATFORM_API_BASE_URL is required')
  origin(platformApiBaseUrl, 'PLATFORM_API_BASE_URL', production)
  if (!allowed) throw new Error('NOTIFICATION_ALLOWED_ORIGINS is required')
  const allowedOrigins = [...new Set(allowed.split(',').map((value) => origin(value.trim(), 'NOTIFICATION_ALLOWED_ORIGINS', production)))]
  const socketPath = env.NOTIFICATION_SOCKET_PATH ?? '/socket.io/'
  if (!/^\/[a-zA-Z0-9/._-]+\/$/.test(socketPath) || socketPath.includes('//') || socketPath === '/' ||
      socketPath.split('/').some((part) => part === '.' || part === '..')) {
    throw new Error('NOTIFICATION_SOCKET_PATH must be an explicit absolute Socket.IO path ending with /')
  }
  const host = env.NOTIFICATION_HOST?.trim() || '0.0.0.0'
  if (/[\s/\\]/.test(host)) throw new Error('NOTIFICATION_HOST must be a bind address')
  return Object.freeze({
    production, metricsToken: metricsToken(env, production),
    databaseUrl,
    platformApiBaseUrl,
    allowedOrigins: Object.freeze(allowedOrigins),
    port: integer(env, 'NOTIFICATION_PORT', 3003, 1, 65535),
    host,
    socketPath,
    authTimeoutMs: integer(env, 'NOTIFICATION_AUTH_TIMEOUT_MS', 2000, 100, 10000),
    recheckMs: integer(env, 'NOTIFICATION_AUTH_RECHECK_MS', 30000, 1000, 300000),
    authWaitMs: integer(env, 'NOTIFICATION_AUTH_WAIT_MS', 10000, 100, 60000),
    healthProbeMs: integer(env, 'NOTIFICATION_HEALTH_PROBE_MS', 5000, 1000, 60000),
    dbRetryMs: integer(env, 'NOTIFICATION_DATABASE_RETRY_MS', 1000, 100, 30000),
  })
}
