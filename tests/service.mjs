// Real Socket.IO clients and loopback HTTP; the platform and database below are
// explicit fixtures. This suite does not claim to exercise real platform/PG data.
import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer, request } from 'node:http'
import { io as createClient } from 'socket.io-client'
import { loadConfig } from '../config.mjs'
import { createNotificationService } from '../service.mjs'

const ORIGIN = 'https://notification-client.example.test'
const uuid = (value) => `00000000-0000-4000-8000-${value.toString(16).padStart(12, '0')}`
const ID = { user: uuid(1), otherUser: uuid(2), tenant: uuid(11), otherTenant: uuid(12), organization: uuid(21), otherOrganization: uuid(22), session: uuid(31), otherSession: uuid(32), notification: uuid(101), application: uuid(201), publication: uuid(301) }
const INVALID = { code: 'SESSION_INVALID', message: '实时通知会话已失效' }
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function context(overrides = {}) {
  return { contractVersion: '1.0', userId: ID.user, tenantId: ID.tenant, organizationId: ID.organization, sessionId: ID.session, expiresAt: new Date(Date.now() + 120_000).toISOString(), ...overrides }
}

function notification(overrides = {}) {
  return { id: ID.notification, userId: ID.user, tenantId: ID.tenant, organizationId: ID.organization, sourceKind: 'APP', applicationId: ID.application, publicationId: ID.publication, type: 'INFO', title: 'Fixture notification', content: 'Platform HTTP and DB fixture only', link: '/fixture/orders/101', createdAt: '2026-10-02T12:00:00.000Z', ...overrides }
}

function hint(row) {
  return { id: row.id, userId: row.userId, tenantId: row.tenantId, organizationId: row.organizationId }
}

async function until(predicate, message, timeout = 1800) {
  const deadline = Date.now() + timeout
  while (!predicate()) {
    assert.ok(Date.now() < deadline, message)
    await delay(5)
  }
}

function httpJson(base, pathname) {
  return new Promise((resolve, reject) => {
    const url = new URL(pathname, base)
    const outgoing = request(url, { agent: false }, (response) => {
      const chunks = []
      response.on('data', (chunk) => chunks.push(chunk))
      response.on('error', reject)
      response.on('end', () => {
        try { resolve({ status: response.statusCode, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) }) }
        catch (error) { reject(error) }
      })
    })
    outgoing.on('error', reject)
    outgoing.setTimeout(1500, () => outgoing.destroy(new Error('fixture health request timed out')))
    outgoing.end()
  })
}

function rawHttp(base, target) {
  const url = new URL(base)
  return new Promise((resolve, reject) => {
    // Preserve the actual request target; new URL(target, base) would reject
    // malformed input in the client and never exercise the service boundary.
    const outgoing = request({ hostname: url.hostname, port: url.port, method: 'GET', path: target, agent: false }, (response) => {
      const chunks = []
      response.on('data', (chunk) => chunks.push(chunk))
      response.on('error', reject)
      response.on('end', () => resolve({ status: response.statusCode, body: Buffer.concat(chunks).toString('utf8') }))
    })
    outgoing.on('error', reject)
    outgoing.setTimeout(1500, () => outgoing.destroy(new Error('fixture raw HTTP request timed out')))
    outgoing.end()
  })
}

async function fixture(t, configOverrides = {}) {
  const state = {
    tokens: new Map(), rows: new Map(), requests: [], lookups: [], logs: [], clients: [],
    timers: new Set(), databaseUp: true, platformProbeStatus: 401, forcedRow: undefined,
    onHint: null, onListeningChange: null, stopped: false, redirectVisits: 0,
  }
  const schedule = (callback, milliseconds) => {
    const timer = setTimeout(() => { state.timers.delete(timer); callback() }, milliseconds)
    state.timers.add(timer)
  }
  const platform = createServer((incoming, outgoing) => {
    const url = new URL(incoming.url, 'http://platform.fixture')
    const authorization = incoming.headers.authorization
    state.requests.push({ method: incoming.method, pathname: url.pathname, notificationId: url.searchParams.get('notificationId'), authorization })
    if (url.pathname === '/redirect-target') {
      state.redirectVisits++
      outgoing.writeHead(200, { 'Content-Type': 'application/json' })
      outgoing.end(JSON.stringify(context()))
      return
    }
    if (url.pathname !== '/api/notifications/socket-context' || incoming.method !== 'GET') {
      outgoing.writeHead(404); outgoing.end('fixture route not found'); return
    }
    if (!authorization) {
      outgoing.writeHead(state.platformProbeStatus, { 'Content-Type': 'application/json' })
      outgoing.end(JSON.stringify({ message: 'fixture requires a platform session' }))
      return
    }
    const token = authorization.startsWith('Bearer ') ? authorization.slice(7) : ''
    const policy = state.tokens.get(token)
    const delivery = url.searchParams.has('notificationId')
    const status = policy ? (delivery ? policy.deliveryStatus ?? policy.status ?? 200 : policy.status ?? 200) : 401
    const reply = delivery ? policy?.deliveryContext ?? policy?.context : policy?.context
    const body = policy?.body ?? JSON.stringify(status === 200 ? reply : { message: `Fixture failure for ${token}` })
    const respond = () => {
      if (outgoing.destroyed) return
      if (policy?.redirect) {
        outgoing.writeHead(302, { Location: `${state.platformUrl}/redirect-target` }); outgoing.end(); return
      }
      outgoing.writeHead(status, { 'Content-Type': 'application/json' })
      const delayBodyMs = delivery ? policy?.deliveryDelayBodyMs ?? policy?.delayBodyMs : policy?.delayBodyMs
      if (delayBodyMs) {
        outgoing.flushHeaders()
        schedule(() => { if (!outgoing.destroyed) outgoing.end(body) }, delayBodyMs)
      } else if (policy?.chunked) {
        outgoing.write(body.slice(0, 2048))
        schedule(() => { if (!outgoing.destroyed) outgoing.end(body.slice(2048)) }, 5)
      } else outgoing.end(body)
    }
    if (policy?.delayHeadersMs) schedule(respond, policy.delayHeadersMs)
    else respond()
  })
  await new Promise((resolve, reject) => {
    platform.once('error', reject)
    platform.listen(0, '127.0.0.1', resolve)
  })
  let service
  t.after(async () => {
    for (const client of state.clients) client.socket.disconnect()
    try { await service?.stop() } finally {
      for (const timer of state.timers) clearTimeout(timer)
      platform.closeAllConnections()
      if (platform.listening) await new Promise((resolve, reject) => platform.close((error) => error ? reject(error) : resolve()))
    }
  })
  state.platformUrl = `http://127.0.0.1:${platform.address().port}`
  const config = {
    ...loadConfig({ DATABASE_URL: 'postgresql://fixture:fixture@127.0.0.1:5432/notification_fixture', PLATFORM_API_BASE_URL: state.platformUrl, NOTIFICATION_ALLOWED_ORIGINS: ORIGIN }),
    port: 0, host: '127.0.0.1', recheckMs: 60_000, authWaitMs: 1000, authTimeoutMs: 500, healthProbeMs: 60_000, dbRetryMs: 60_000,
    ...configOverrides,
  }
  const store = {
    async start(onHint, onListeningChange) { state.onHint = onHint; state.onListeningChange = onListeningChange; onListeningChange(true) },
    async getNotification(value) {
      state.lookups.push(structuredClone(value))
      if (state.forcedRow !== undefined) return state.forcedRow
      const row = state.rows.get(value.id)
      return row && ['id', 'userId', 'tenantId', 'organizationId'].every((key) => row[key] === value[key]) && ['APP', 'SYSTEM'].includes(row.sourceKind) ? row : null
    },
    async ping() { if (!state.databaseUp) throw new Error('DB_FIXTURE_INTERNAL_SECRET'); return true },
    async stop() { state.stopped = true; state.onListeningChange?.(false) },
  }
  const capture = (...args) => state.logs.push(JSON.stringify(args, (_, value) => value instanceof Error ? { message: value.message } : value))
  service = createNotificationService(config, { store, logger: { info: capture, warn: capture, error: capture, debug: capture, log: capture } })
  const address = await service.start()
  assert.ok(address && typeof address.port === 'number' && address.port > 0, 'service must return its real ephemeral listener address')
  state.serviceUrl = `http://127.0.0.1:${address.port}`
  state.service = service
  state.config = config
  return state
}

function addToken(state, token = 'fixture-platform-session-token', value = context(), policy = {}) {
  state.tokens.set(token, { context: value, ...policy })
  return token
}

function client(state, { origin = ORIGIN, transport = 'websocket' } = {}) {
  const socket = createClient(state.serviceUrl, {
    path: state.config.socketPath, transports: [transport], extraHeaders: { Origin: origin },
    reconnection: false, forceNew: true, timeout: 1000, autoConnect: false,
  })
  const result = { socket, authenticated: [], unauthorized: [], notifications: [], disconnects: [], connectErrors: [] }
  for (const [event, collection] of [['authenticated', 'authenticated'], ['unauthorized', 'unauthorized'], ['notification', 'notifications'], ['disconnect', 'disconnects'], ['connect_error', 'connectErrors']]) socket.on(event, (value) => result[collection].push(value))
  state.clients.push(result)
  socket.connect()
  return result
}

async function connected(state, options) {
  const result = client(state, options)
  await until(() => result.socket.connected || result.connectErrors.length, 'Socket.IO fixture must connect')
  assert.ok(result.socket.connected, result.connectErrors[0]?.message)
  return result
}

async function authenticate(state, token, options) {
  const result = await connected(state, options)
  result.socket.emit('authenticate', { token })
  await until(() => result.authenticated.length || result.unauthorized.length, 'authentication must return a result')
  assert.equal(result.unauthorized.length, 0, 'fixture platform session should be accepted')
  assert.equal(result.authenticated.length, 1)
  return result
}

async function rejected(result) {
  await until(() => result.unauthorized.length && !result.socket.connected, 'invalid session must emit unauthorized and disconnect')
  assert.deepEqual(result.unauthorized, [INVALID])
  assert.equal(result.authenticated.length, 0)
}

async function invalidated(result) {
  await until(() => result.unauthorized.length && !result.socket.connected, 'bound session must emit unauthorized and disconnect')
  assert.deepEqual(result.unauthorized, [INVALID])
}

async function deliver(state, row = notification(), overrides = {}) {
  state.rows.set(row.id, row)
  await state.service.handleHint(JSON.stringify({ ...hint(row), ...overrides }))
}

async function noDelivery(result) {
  await delay(35)
  assert.equal(result.notifications.length, 0)
}

test('HTTP/DB fixtures: initial auth delegates only the Bearer and exposes the safe session DTO', async (t) => {
  const state = await fixture(t)
  const value = context()
  const token = addToken(state, undefined, value)
  const result = await authenticate(state, token)
  assert.deepEqual(result.authenticated[0], { contractVersion: '1.0', userId: value.userId, tenantId: value.tenantId, organizationId: value.organizationId, expiresAt: value.expiresAt })
  assert.ok(state.requests.some((item) => item.pathname === '/api/notifications/socket-context' && item.authorization === `Bearer ${token}` && item.notificationId === null))
  assert.ok(!JSON.stringify(result.authenticated).includes(token))
  assert.ok(!state.logs.join('\n').includes(token))
})

test('HTTP/DB fixtures: unauthenticated connection times out with the fixed error', async (t) => {
  const state = await fixture(t, { authWaitMs: 60 })
  await rejected(await connected(state))
})

for (const [name, payload] of [
  ['bare token', 'fixture-platform-session-token'], ['Bearer string', 'Bearer fixture-platform-session-token'],
  ['array', ['fixture-platform-session-token']], ['missing token', {}], ['non-string token', { token: 42 }],
  ['blank token', { token: '' }], ['spoofed user', { token: 'fixture-platform-session-token', userId: ID.otherUser }],
]) test(`HTTP/DB fixtures: strict authenticate rejects ${name}`, async (t) => {
  const state = await fixture(t)
  addToken(state)
  const result = await connected(state)
  result.socket.emit('authenticate', payload)
  await rejected(result)
})

for (const [name, policy, overrides] of [
  ['wrong token', { status: 401 }, {}], ['OAuth access token', { status: 401 }, {}], ['revoked session', { status: 401 }, {}],
  ['membership denied', { status: 403 }, {}], ['expired session', {}, { expiresAt: new Date(0).toISOString() }],
  ['unknown contract', {}, { contractVersion: '9.0' }], ['invalid UUID', {}, { tenantId: 'invalid-tenant' }],
  ['invalid expiry', {}, { expiresAt: 'not-a-date' }], ['missing session identity', {}, { sessionId: undefined }],
]) test(`HTTP/DB fixtures: initial validation rejects ${name}`, async (t) => {
  const state = await fixture(t)
  const token = addToken(state, `fixture-${name.replaceAll(' ', '-')}`, context(overrides), policy)
  const result = await connected(state)
  result.socket.emit('authenticate', { token })
  await rejected(result)
  assert.ok(!state.logs.join('\n').includes(token), 'failure logs must not disclose Bearer tokens')
})

test('HTTP/DB fixtures: APP delivery revalidates notificationId and emits a safe DTO from the persisted row', async (t) => {
  const state = await fixture(t)
  const token = addToken(state)
  const result = await authenticate(state, token)
  const row = notification({ internalSecret: 'DB_ROW_SECRET_SENTINEL' })
  await deliver(state, row)
  await until(() => result.notifications.length === 1, 'valid APP notification should arrive')
  const payload = result.notifications[0]
  assert.equal(payload.contractVersion, '1.0')
  for (const field of ['id', 'userId', 'tenantId', 'organizationId', 'applicationId', 'publicationId', 'type', 'title', 'content', 'link', 'createdAt']) assert.equal(payload[field], row[field])
  assert.ok(state.requests.some((item) => item.authorization === `Bearer ${token}` && item.notificationId === row.id), 'each delivery must ask the platform about this notification ID')
  assert.ok(state.lookups.some((item) => item.id === row.id && item.userId === row.userId && item.tenantId === row.tenantId && item.organizationId === row.organizationId))
  assert.ok(!JSON.stringify(payload).includes(token))
  assert.ok(!JSON.stringify(payload).includes('DB_ROW_SECRET_SENTINEL'))
  assert.equal(payload.sourceKind, undefined)
})

test('HTTP/DB fixtures: explicit SYSTEM rows can be delivered without application/publication IDs', async (t) => {
  const state = await fixture(t)
  const result = await authenticate(state, addToken(state))
  const row = notification({ sourceKind: 'SYSTEM', applicationId: null, publicationId: null })
  state.rows.set(row.id, row)
  await state.onHint(JSON.stringify(hint(row)))
  await until(() => result.notifications.length === 1, 'attributed SYSTEM notification should arrive')
  for (const field of ['id', 'userId', 'tenantId', 'organizationId', 'applicationId', 'publicationId']) assert.equal(result.notifications[0][field], row[field])
})

test('HTTP/DB fixtures: same-user other-tenant and other-org plus other-user connections remain isolated', async (t) => {
  const state = await fixture(t)
  const peers = []
  for (const [token, value] of [
    ['scope-primary', context()], ['scope-tenant', context({ tenantId: ID.otherTenant, sessionId: uuid(41) })],
    ['scope-organization', context({ organizationId: ID.otherOrganization, sessionId: uuid(42) })],
    ['scope-user', context({ userId: ID.otherUser, sessionId: uuid(43) })],
  ]) peers.push(await authenticate(state, addToken(state, token, value)))
  await deliver(state)
  await until(() => peers[0].notifications.length === 1, 'matching context should receive the notification')
  await delay(35)
  for (const peer of peers.slice(1)) { assert.equal(peer.notifications.length, 0); assert.ok(peer.socket.connected) }
})

for (const field of ['id', 'userId', 'tenantId', 'organizationId']) test(`HTTP/DB fixtures: mismatched DB ${field} is never emitted`, async (t) => {
  const state = await fixture(t)
  const result = await authenticate(state, addToken(state))
  const row = notification()
  state.forcedRow = { ...row, [field]: uuid(499) }
  await state.service.handleHint(JSON.stringify(hint(row)))
  await noDelivery(result)
  assert.ok(result.socket.connected)
})

for (const [name, overrides] of [
  ['LEGACY', { sourceKind: 'LEGACY', tenantId: null, organizationId: null, applicationId: null, publicationId: null }],
  ['missing organization', { organizationId: null }], ['APP without application', { applicationId: null }],
  ['APP without publication', { publicationId: null }], ['SYSTEM carrying application', { sourceKind: 'SYSTEM', publicationId: null }],
]) test(`HTTP/DB fixtures: ${name} DB row is never emitted`, async (t) => {
  const state = await fixture(t)
  const result = await authenticate(state, addToken(state))
  state.forcedRow = notification(overrides)
  await state.service.handleHint(JSON.stringify(hint(notification())))
  await noDelivery(result)
})

test('HTTP/DB fixtures: malformed or incomplete hints do not query or broadcast', async (t) => {
  const state = await fixture(t)
  const result = await authenticate(state, addToken(state))
  for (const raw of ['not json', '{}', JSON.stringify({ id: ID.notification, userId: ID.user }), JSON.stringify({ ...hint(notification()), id: 'not-a-uuid' }), JSON.stringify({ ...hint(notification()), title: 'UNTRUSTED_HINT_TITLE' })]) await state.service.handleHint(raw)
  await noDelivery(result)
  assert.equal(state.lookups.length, 0)
})

for (const status of [403, 404]) test(`HTTP/DB fixtures: per-notification ${status} drops only that delivery and the next allowed row still arrives`, async (t) => {
  const state = await fixture(t)
  const token = addToken(state)
  const result = await authenticate(state, token)
  state.tokens.get(token).deliveryStatus = status
  await deliver(state)
  await until(() => state.requests.some((item) => item.notificationId === ID.notification), 'delivery denial must be checked over HTTP')
  await noDelivery(result)
  assert.ok(result.socket.connected)
  assert.equal(result.unauthorized.length, 0)
  state.tokens.get(token).deliveryStatus = 200
  await deliver(state, notification({ id: uuid(102) }))
  await until(() => result.notifications.length === 1, 'the same connection must receive a subsequent authorized delivery')
  assert.equal(result.notifications[0].id, uuid(102))
})

for (const [name, policy] of [
  ['revocation', { deliveryStatus: 401 }],
  ...['userId', 'tenantId', 'organizationId', 'sessionId'].map((field) => [`changed ${field}`, { deliveryContext: context({ [field]: uuid(599) }) }]),
  ['expiry', { deliveryContext: context({ expiresAt: new Date(0).toISOString() }) }],
]) test(`HTTP/DB fixtures: delivery-time ${name} invalidates the bound socket`, async (t) => {
  const state = await fixture(t)
  const token = addToken(state)
  const result = await authenticate(state, token)
  Object.assign(state.tokens.get(token), policy)
  await deliver(state)
  await invalidated(result)
  assert.equal(result.notifications.length, 0)
})

for (const [name, policy] of [['platform 500', { deliveryStatus: 500 }], ['unknown delivery contract', { deliveryContext: context({ contractVersion: '9.0' }) }]]) test(`HTTP/DB fixtures: ${name} never delivers an unchecked notification`, async (t) => {
  const state = await fixture(t)
  const token = addToken(state)
  const result = await authenticate(state, token)
  Object.assign(state.tokens.get(token), policy)
  await deliver(state)
  await noDelivery(result)
  assert.ok(result.socket.connected, 'transient delivery failures must preserve the valid connection')
  assert.equal(result.unauthorized.length, 0)
  assert.ok(!state.logs.join('\n').includes(token))
  state.tokens.get(token).deliveryStatus = 200
  state.tokens.get(token).deliveryContext = state.tokens.get(token).context
  await deliver(state, notification({ id: uuid(103) }))
  await until(() => result.notifications.length === 1, 'delivery should resume after a transient platform failure')
})

test('HTTP/DB fixtures: periodic validation detects membership revocation before token expiry', async (t) => {
  const state = await fixture(t, { recheckMs: 40 })
  const token = addToken(state)
  const result = await authenticate(state, token)
  state.tokens.get(token).status = 403
  await invalidated(result)
  assert.ok(state.requests.filter((item) => item.authorization === `Bearer ${token}` && !item.notificationId).length >= 2)
})

test('HTTP/DB fixtures: expiry timer disconnects an otherwise still accepted platform session', async (t) => {
  const state = await fixture(t)
  const token = addToken(state, undefined, context({ expiresAt: new Date(Date.now() + 1000).toISOString() }))
  const result = await authenticate(state, token)
  await invalidated(result)
})

test('HTTP/DB fixtures: reauthentication cannot replace a previously bound identity', async (t) => {
  const state = await fixture(t)
  const result = await authenticate(state, addToken(state))
  const replacement = addToken(state, 'replacement-identity-token', context({ userId: ID.otherUser, sessionId: ID.otherSession }))
  result.socket.emit('authenticate', { token: replacement })
  await invalidated(result)
  assert.equal(result.authenticated.length, 1)
})

test('HTTP/DB fixtures: concurrent initial authentication cannot switch identities or accept a late result', async (t) => {
  const state = await fixture(t)
  const original = addToken(state, 'pending-original-session', context(), { delayHeadersMs: 140 })
  const replacement = addToken(state, 'pending-replacement-session', context({ userId: ID.otherUser, sessionId: ID.otherSession }))
  const result = await connected(state)
  result.socket.emit('authenticate', { token: original })
  await until(() => state.requests.some((item) => item.authorization === `Bearer ${original}`), 'the first platform validation must be pending')
  result.socket.emit('authenticate', { token: replacement })
  await rejected(result)
  await delay(160)
  assert.equal(result.authenticated.length, 0, 'late successful validation must not resurrect an invalidated socket')
})

test('HTTP/DB fixtures: a late positive delivery result cannot send after periodic revocation', async (t) => {
  const state = await fixture(t, { recheckMs: 45 })
  const token = addToken(state)
  const result = await authenticate(state, token)
  state.tokens.get(token).deliveryDelayBodyMs = 180
  const pending = deliver(state)
  await until(() => state.requests.some((item) => item.notificationId === ID.notification), 'the delivery validation must already be in flight')
  state.tokens.get(token).status = 401
  await invalidated(result)
  await pending
  assert.equal(result.notifications.length, 0)
})

for (const transport of ['polling', 'websocket']) {
  test(`HTTP/DB fixtures: ${transport} allows an exact configured Origin`, async (t) => {
    const state = await fixture(t)
    await authenticate(state, addToken(state), { transport })
  })
  for (const origin of ['https://notification-client.example.test.evil', 'http://notification-client.example.test', 'https://notification-client.example.test:444', 'null']) test(`HTTP/DB fixtures: ${transport} rejects non-allowlisted Origin ${origin}`, async (t) => {
    const state = await fixture(t)
    const result = client(state, { transport, origin })
    await until(() => result.connectErrors.length, 'unapproved browser Origin must fail the actual Engine.IO handshake')
    assert.ok(!result.socket.connected)
    assert.equal(result.authenticated.length, 0)
  })
}

for (const [name, policy] of [['late headers', { delayHeadersMs: 200 }], ['late response body', { delayBodyMs: 200 }]]) test(`HTTP/DB fixtures: platform auth timeout covers ${name} without disclosing the Bearer`, async (t) => {
  const state = await fixture(t, { authTimeoutMs: 60 })
  const token = addToken(state, `TIMEOUT_SECRET_${name.replaceAll(' ', '_')}`, context(), policy)
  const result = await connected(state)
  result.socket.emit('authenticate', { token })
  await rejected(result)
  assert.ok(state.requests.some((item) => item.authorization === `Bearer ${token}`), 'timeout must occur after the real platform request, not a local token rejection')
  assert.ok(!state.logs.join('\n').includes(token))
})

test('HTTP/DB fixtures: exactly 4096 response bytes can authenticate', async (t) => {
  const state = await fixture(t)
  const value = context()
  const json = JSON.stringify(value)
  const token = addToken(state, undefined, value, { body: json + ' '.repeat(4096 - Buffer.byteLength(json)) })
  await authenticate(state, token)
})

for (const chunked of [false, true]) test(`HTTP/DB fixtures: ${chunked ? 'chunked' : 'ordinary'} platform response above 4 KiB is rejected`, async (t) => {
  const state = await fixture(t)
  const value = context()
  const json = JSON.stringify(value)
  const token = addToken(state, undefined, value, { body: json + ' '.repeat(4097 - Buffer.byteLength(json)), chunked })
  const result = await connected(state)
  result.socket.emit('authenticate', { token })
  await rejected(result)
})

test('HTTP/DB fixtures: platform redirects are not followed and never forward the Bearer', async (t) => {
  const state = await fixture(t)
  const token = addToken(state, 'REDIRECT_BEARER_SECRET', context(), { redirect: true })
  const result = await connected(state)
  result.socket.emit('authenticate', { token })
  await rejected(result)
  assert.equal(state.redirectVisits, 0)
  assert.ok(!state.logs.join('\n').includes(token))
})

test('HTTP/DB fixtures: health tracks database, LISTEN and unauthenticated platform probe independently', async (t) => {
  const state = await fixture(t)
  const expectHealth = async (status, changes = {}) => {
    const health = await state.service.probeHealth()
    assert.equal(health.status, status === 200 ? 'UP' : 'DOWN')
    for (const [key, value] of Object.entries(changes)) assert.equal(health.checks[key], value)
    const response = await httpJson(state.serviceUrl, '/health')
    assert.equal(response.status, status)
    assert.equal(response.body.status, health.status)
    const live = await httpJson(state.serviceUrl, '/health/live')
    assert.equal(live.status, 200)
    assert.equal(live.body.status, 'UP')
    assert.ok(!JSON.stringify(response.body).includes('DB_FIXTURE_INTERNAL_SECRET'))
  }
  await expectHealth(200, { database: 'UP', listen: 'UP', platform: 'UP' })
  state.databaseUp = false
  await expectHealth(503, { database: 'DOWN' })
  state.databaseUp = true
  state.onListeningChange(false)
  await expectHealth(503, { listen: 'DOWN' })
  state.onListeningChange(true)
  state.platformProbeStatus = 200
  await expectHealth(503, { platform: 'DOWN' })
  state.platformProbeStatus = 500
  await expectHealth(503, { platform: 'DOWN' })
  state.platformProbeStatus = 401
  await expectHealth(200, { database: 'UP', listen: 'UP', platform: 'UP' })
  assert.ok(state.requests.some((item) => item.pathname === '/api/notifications/socket-context' && item.authorization === undefined))
})

test('HTTP/DB fixtures: malformed absolute HTTP target returns 400 and the service remains live', async (t) => {
  const state = await fixture(t)
  const invalid = await rawHttp(state.serviceUrl, 'http://[')
  assert.equal(invalid.status, 400)
  const live = await httpJson(state.serviceUrl, '/health/live')
  assert.equal(live.status, 200)
  assert.deepEqual(live.body, { status: 'UP' })
})

test('HTTP/DB fixtures: a denied WebSocket upgrade retains the allowed polling session', async (t) => {
  const state = await fixture(t)
  const token = addToken(state)
  const socket = createClient(state.serviceUrl, {
    path: state.config.socketPath, transports: ['polling', 'websocket'],
    transportOptions: {
      polling: { extraHeaders: { Origin: ORIGIN } },
      websocket: { extraHeaders: { Origin: 'https://unapproved-upgrade.example.test' } },
    },
    upgrade: true, reconnection: false, forceNew: true, timeout: 1000, autoConnect: false,
  })
  const result = { socket, authenticated: [], unauthorized: [], notifications: [], disconnects: [], connectErrors: [] }
  for (const [event, collection] of [['authenticated', 'authenticated'], ['unauthorized', 'unauthorized'], ['notification', 'notifications'], ['disconnect', 'disconnects'], ['connect_error', 'connectErrors']]) socket.on(event, (value) => result[collection].push(value))
  state.clients.push(result)
  socket.connect()
  const engine = socket.io.engine
  const upgradeErrors = []
  const upgrades = []
  engine.on('upgradeError', (error) => upgradeErrors.push(error))
  engine.on('upgrade', (transport) => upgrades.push(transport.name))
  await until(() => socket.connected && upgradeErrors.length, 'the actual WebSocket upgrade with an unapproved Origin must fail')
  assert.deepEqual(upgrades, [])
  assert.equal(engine.transport.name, 'polling')
  assert.equal(state.service.io.sockets.sockets.get(socket.id).conn.transport.name, 'polling')
  socket.emit('authenticate', { token })
  await until(() => result.authenticated.length || result.unauthorized.length, 'the approved polling connection must still authenticate')
  assert.equal(result.authenticated.length, 1)
  assert.equal(result.unauthorized.length, 0)
  await deliver(state)
  await until(() => result.notifications.length === 1, 'the retained polling connection must still receive an authorized notification')
  assert.equal(result.notifications[0].id, ID.notification)
  assert.ok(socket.connected)
})
