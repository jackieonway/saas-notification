import { randomUUID, timingSafeEqual } from 'node:crypto'
import { performance } from 'node:perf_hooks'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const METHODS = new Set(['GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD', 'PUT'])
const STATUSES = new Set([200, 201, 204, 400, 401, 403, 404, 405, 409, 413, 429, 500, 502, 503])
const BUCKETS = [.005, .025, .1, .5, 1, 5, 15]

export function metricsToken(env, production) {
  const token = env.METRICS_TOKEN
  if (token == null && !production) return null
  if (typeof token !== 'string' || token.length < 32 || token.length > 256 ||
      !/^[A-Za-z0-9_-]+$/.test(token) || new Set(token).size < 8 ||
      /change-?me|replace-?me|dev-?secret|your-?secret|placeholder/i.test(token)) {
    throw new Error('METRICS_TOKEN requires 32–256 random URL-safe characters with at least 8 distinct characters; placeholders are rejected')
  }
  return token
}

export function requestId(value) { return typeof value === 'string' && UUID.test(value) ? value.toLowerCase() : randomUUID() }
export function metricsAllowed(header, token) {
  if (!token || typeof header !== 'string') return false
  const actual = Buffer.from(header), expected = Buffer.from('Bearer ' + token)
  return actual.length === expected.length && timingSafeEqual(actual, expected)
}

/** Service-local copy: each independent release carries its own telemetry runtime. */
export function createTelemetry({ service, routeFor, logger = console, gauges = () => ({}) }) {
  const series = new Map()
  const counters = new Map()
  function observe(request, response, metadata = {}) {
    metadata.requestId = requestId(metadata.requestId || request.headers['x-request-id'])
    const route = routeFor(request), method = METHODS.has(request.method) ? request.method : 'OTHER'
    const begin = performance.now()
    response.setHeader('x-request-id', metadata.requestId)
    let recorded = false
    const finish = () => {
      if (recorded) return
      recorded = true
      const seconds = Math.max(0, (performance.now() - begin) / 1000)
      const status = response.writableFinished ? (STATUSES.has(response.statusCode) ? String(response.statusCode) : 'OTHER') : 'ABORTED'
      const key = `${route}|${method}|${status}`
      const item = series.get(key) || { route, method, status, count: 0, sum: 0, buckets: BUCKETS.map(() => 0) }
      item.count++; item.sum += seconds
      for (let index = 0; index < BUCKETS.length; index++) if (seconds <= BUCKETS[index]) item.buckets[index]++
      series.set(key, item)
      const entry = { timestamp: new Date().toISOString(), event: 'http_request', service, route, method, status, request_id: metadata.requestId,
        duration_ms: Math.round(seconds * 1000) }
      if (UUID.test(metadata.tenantId || '') && UUID.test(metadata.organizationId || '')) {
        entry.tenant_id = metadata.tenantId; entry.organization_id = metadata.organizationId
      }
      logger.info(JSON.stringify(entry))
    }
    response.once('finish', finish); response.once('close', finish)
    return metadata
  }
  function render() {
    const lines = ['# HELP saas_http_requests_total Completed and aborted HTTP requests.', '# TYPE saas_http_requests_total counter',
      '# TYPE saas_http_request_duration_seconds histogram', '# TYPE saas_http_errors_total counter', '# TYPE saas_http_auth_denials_total counter']
    for (const item of series.values()) {
      const labels = `service="${service}",route="${item.route}",method="${item.method}",status="${item.status}"`
      lines.push(`saas_http_requests_total{${labels}} ${item.count}`)
      for (let index = 0; index < BUCKETS.length; index++) lines.push(`saas_http_request_duration_seconds_bucket{${labels},le="${BUCKETS[index]}"} ${item.buckets[index]}`)
      lines.push(`saas_http_request_duration_seconds_bucket{${labels},le="+Inf"} ${item.count}`,
        `saas_http_request_duration_seconds_count{${labels}} ${item.count}`, `saas_http_request_duration_seconds_sum{${labels}} ${item.sum}`)
      if (item.status.startsWith('5')) lines.push(`saas_http_errors_total{${labels}} ${item.count}`)
      if (['401', '403'].includes(item.status)) lines.push(`saas_http_auth_denials_total{${labels}} ${item.count}`)
    }
    for (const [name, value] of Object.entries(gauges())) {
      if (/^saas_[a-z_]+$/.test(name) && Number.isFinite(value)) lines.push(`# TYPE ${name} gauge`, `${name}{service="${service}"} ${value}`)
    }
    for (const [name, value] of counters) lines.push(`# TYPE ${name} counter`, `${name}{service="${service}"} ${value}`)
    return lines.join('\n') + '\n'
  }
  return { observe, render, count(name) { if (/^saas_[a-z_]+_total$/.test(name)) counters.set(name, (counters.get(name) || 0) + 1) } }
}
