import assert from 'node:assert/strict'
import test from 'node:test'
import { createRequire } from 'node:module'
import { request as httpRequest } from 'node:http'
import net from 'node:net'
import { createRealPlatformFixture, PLATFORM_FIXTURE, until } from './helpers/rust-platform.mjs'
import { metricValue } from './helpers/rust-service.mjs'

const INVALID = { code: 'SESSION_INVALID', message: '实时通知会话已失效' }
const require = createRequire(new URL('../package.json', import.meta.url))
const WebSocket = require('ws')

// agent:false and Connection:close make every call a different real TCP owner.
// A peer reset is distinct from our own timeout; timeout cannot prove revocation.
async function rawHttp(url, { method = 'GET', body, origin, timeout = 3000 } = {}) {
  return new Promise(resolve => {
    let settled = false
    const finish = value => { if (!settled) { settled = true; resolve(value) } }
    const request = httpRequest(url, { method, agent: false, headers: {
      connection: 'close', origin,
      ...(body === undefined ? {} : { 'content-type': 'text/plain;charset=UTF-8', 'content-length': Buffer.byteLength(body) }),
    } }, response => {
      let text = ''
      response.setEncoding('utf8')
      response.on('data', chunk => { text += chunk; if (text.length > 16_384) request.destroy(Object.assign(new Error('Raw response exceeded the test bound'), { code: 'TEST_RESPONSE_LIMIT' })) })
      response.on('end', () => finish({ status: response.statusCode, body: text }))
      response.on('error', error => finish({ status: null, error: error.code ?? 'RESPONSE_ERROR' }))
    })
    request.on('error', error => finish({ status: null, error: error.code ?? 'REQUEST_ERROR' }))
    request.setTimeout(timeout, () => request.destroy(Object.assign(new Error('Raw request deadline'), { code: 'TEST_DEADLINE' })))
    request.end(body)
  })
}

test('Rust Engine.IO admission, connection permits and independent known expiry', { timeout: 240_000 }, async t => {
  const f = await createRealPlatformFixture(t, { platformProxy: true,
    config: { authTimeoutMs: 5000, recheckMs: 60_000 },
    serviceEnv: { NOTIFICATION_MAX_CONNECTIONS: '2', NOTIFICATION_VALIDATION_CONCURRENCY: '2', NOTIFICATION_MAX_INFLIGHT: '2' },
  })
  const orgA = f.ids.organizations['p10-e2e-a-root'].id
  const session = await f.login(PLATFORM_FIXTURE.publisher, orgA)
  const clients = async () => metricValue(await f.service.metrics(), 'saas_notification_clients')

  await t.test('path prefix collisions and invalid or duplicate sid never open a new Engine.IO session', async () => {
    for (const [target, statuses] of [
      ['/socket.ioevil/?EIO=4&transport=polling', [403, 404]],
      ['/socket.io/?EIO=4&transport=polling&sid=invalid', [400]],
      ['/socket.io/?EIO=4&transport=polling&sid=%2Finvalid%2F', [400]],
      ['/socket.io/?EIO=4&transport=polling&sid=bad&sid=bad', [400]],
    ]) {
      const response = await fetch(f.notificationUrl + target, { headers: { origin: f.backendUrl }, signal: AbortSignal.timeout(3000) })
      assert.ok(statuses.includes(response.status), `Expected explicit rejection for the ${target.includes('sid=') ? 'sid' : 'path'} boundary`)
      assert.ok(!(await response.text()).startsWith('0{'), 'A rejected request must not receive an Engine.IO open packet')
    }
    assert.equal(await clients(), 0)
  })

  await t.test('more connect/auth/disconnect cycles than the cap leave capacity available; rejected attempts also release it', async () => {
    for (let index = 0; index < 7; index++) {
      const current = await f.socket(session)
      current.socket.disconnect()
      await until(async () => (await clients()) === 0, 'A disconnected authenticated connection releases its real server permit')
    }
    const a = await f.socket(session), b = await f.socket(session)
    const excess = await f.socket(session, { authenticate: false, allowRejected: true })
    assert.ok(excess.errors.length > 0, 'The Engine.IO admission cap rejects before a Socket.IO namespace is allocated')
    assert.equal(excess.socket.connected, false)
    assert.deepEqual(excess.unauthorized, [])
    assert.deepEqual(excess.authenticated, [])
    const denied = await fetch(f.notificationUrl + '/socket.io/?EIO=4&transport=polling', {
      headers: { origin: f.backendUrl }, signal: AbortSignal.timeout(3000),
    })
    assert.equal(denied.status, 503)
    assert.equal(await clients(), 2)
    a.socket.disconnect(); b.socket.disconnect()
    await until(async () => (await clients()) === 0, 'Both owned connection permits return after disconnect')
    const invalid = await f.socket(session, { authenticate: false })
    invalid.socket.emit('authenticate', { token: 'not-a-valid-platform-session' })
    await until(() => invalid.unauthorized.length && !invalid.socket.connected, 'Authentication rejection also releases the connection')
    assert.deepEqual(invalid.unauthorized, [INVALID])
    await until(async () => (await clients()) === 0, 'Rejected connection has no strong-reference or task leak')
    const replacement = await f.socket(session)
    replacement.socket.disconnect()
    await until(async () => (await clients()) === 0, 'A fresh real session remains admissible after every prior path')
  })

  await t.test('server authentication rejection physically closes a raw Engine.IO peer that keeps answering heartbeats', async () => {
    const raw = new WebSocket(f.notificationUrl.replace('http:', 'ws:') + '/socket.io/?EIO=4&transport=websocket', {
      headers: { origin: f.backendUrl }, handshakeTimeout: 3000,
    })
    const events = []
    let closed = false, attempted = false, failure
    raw.on('error', () => { failure = true })
    raw.on('close', () => { closed = true })
    raw.on('message', buffer => {
      const packet = buffer.toString('utf8')
      if (packet.startsWith('0{')) raw.send('40')
      else if (packet.startsWith('40') && !attempted) {
        attempted = true
        raw.send('42' + JSON.stringify(['authenticate', { token: 'invalid-raw-platform-session' }]))
      } else if (packet === '2') raw.send('3')
      else if (packet.startsWith('42')) events.push(JSON.parse(packet.slice(2)))
      // Receiving namespace packet 41 deliberately does not close this transport.
    })
    try {
      await until(() => closed, 'The server must close the actual Engine.IO connection after unauthorized', 4000)
      assert.equal(closed, true)
      assert.deepEqual(events.filter(([name]) => name === 'unauthorized'), [['unauthorized', INVALID]])
      assert.equal(events.some(([name]) => name === 'authenticated'), false)
      await until(async () => (await clients()) === 0, 'The raw rejected transport cannot retain connection capacity')
      t.diagnostic(JSON.stringify({ rawEngineClosed: closed, transportErrorObserved: Boolean(failure), unauthorizedEvents: events.length }))
    } finally { raw.terminate() }
  })

  await t.test('the next real delivery rejects a deleted member before the long periodic recheck could run', async () => {
    const victim = await f.login(PLATFORM_FIXTURE.recipient, orgA)
    const client = await f.socket(victim)
    const identity = victim.context
    await f.proxy.control('/test/gates', { label: 'revoked-delivery', token: victim.token, deliveryOnly: true, delayMs: 0 })
    const removed = await f.query('DELETE FROM memberships WHERE user_id=$1 AND organization_id=$2 RETURNING id', [identity.userId, orgA])
    assert.equal(removed.rowCount, 1)
    try {
      const began = performance.now()
      const row = await f.insertSystem(identity, 'Unique real delivery after membership deletion')
      await until(async () => (await f.proxy.control('/test/state')).gates.find(gate => gate.label === 'revoked-delivery')?.count >= 1,
        'The new notification must cause its own actual platform delivery verification')
      await until(() => client.unauthorized.length && !client.socket.connected, 'Per-delivery invalidation must close the deleted member', 3500)
      assert.deepEqual(client.unauthorized, [INVALID])
      assert.equal(client.notifications.some(notification => notification.id === row.id), false)
      const observedMs = performance.now() - began
      assert.ok(observedMs < 3500 && observedMs < f.config.recheckMs)
      t.diagnostic(JSON.stringify({ path: 'next-delivery', configuredRecheckMs: f.config.recheckMs,
        observedRejectionMs: Math.round(observedMs), actualDeliveryValidationCount: 1 }))
    } finally {
      await f.query('INSERT INTO memberships(id,user_id,organization_id) VALUES($1,$2,$3)', [removed.rows[0].id, identity.userId, orgA])
      await f.proxy.control('/test/release', { label: 'revoked-delivery' })
    }
    await until(async () => (await clients()) === 0, 'A delivery-revoked identity returns its connection permit')
  })

  await t.test('known expiresAt disconnects on time while a real per-delivery platform verification is still blocked', async () => {
    const expiring = await f.login(PLATFORM_FIXTURE.recipient, orgA)
    await f.query("UPDATE platform_sessions SET expires_at=clock_timestamp()+interval '3 seconds' WHERE id=$1", [expiring.context.sessionId])
    const client = await f.socket(expiring)
    const expiresAt = Date.parse(client.authenticated[0].expiresAt)
    await f.proxy.control('/test/gates', { label: 'cross-expiry', token: expiring.token, deliveryOnly: true, delayMs: 12_000 })
    const row = await f.insertSystem(expiring.context, 'Real notification blocked across known expiry')
    await until(async () => (await f.proxy.control('/test/state')).gates.find(gate => gate.label === 'cross-expiry')?.inflight === 1,
      'The real platform verification is still held beyond the known session expiry')
    await until(() => client.unauthorized.length && !client.socket.connected, 'Independent expiry closes the socket while verification is pending', 4500)
    const observedLateMs = Date.now() - expiresAt
    assert.deepEqual(client.unauthorized, [INVALID])
    assert.ok(observedLateMs >= -40 && observedLateMs < 700, 'Known expiry cannot wait for the five-second platform request deadline')
    assert.equal(client.notifications.some(notification => notification.id === row.id), false)
    await f.proxy.control('/test/release', { label: 'cross-expiry' })
    await until(async () => (await clients()) === 0, 'Expiry releases its connection permit after cancelling verification')
    const replacement = await f.socket(session)
    assert.equal(replacement.socket.connected, true)
    replacement.socket.disconnect()
    t.diagnostic(JSON.stringify({ configuredDatabaseLifetimeMs: 3000, configuredPlatformDeadlineMs: 5000,
      configuredProxyDelayMs: 12_000, expiryObservedLateMs: observedLateMs }))
  })

  await t.test('later real platform validation may shorten the captured expiry but cannot extend it', async () => {
    await until(async () => (await clients()) === 0, 'The preceding connection releases its permit')
    const session = await f.login(PLATFORM_FIXTURE.recipient, orgA)
    await f.query("UPDATE platform_sessions SET expires_at=clock_timestamp()+interval '8 seconds' WHERE id=$1", [session.context.sessionId])
    const client = await f.socket(session)
    const original = Date.parse(client.authenticated[0].expiresAt)
    const shortened = (await f.query("UPDATE platform_sessions SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1 RETURNING expires_at", [session.context.sessionId])).rows[0].expires_at.getTime()
    assert.ok(shortened < original)
    const first = await f.insertSystem(session.context, 'Real validation shortens known expiry')
    await until(() => client.notifications.some(row => row.id === first.id), 'An actual per-delivery validation captures the shorter authoritative expiry')
    await f.query("UPDATE platform_sessions SET expires_at=clock_timestamp()+interval '1 hour' WHERE id=$1", [session.context.sessionId])
    const second = await f.insertSystem(session.context, 'Later validation cannot extend the captured expiry')
    await until(() => client.notifications.some(row => row.id === second.id), 'A later successful real response may not extend the already captured deadline')
    await until(() => client.unauthorized.length && !client.socket.connected, 'The shorter captured deadline remains authoritative', 3500)
    const observedLateMs = Date.now() - shortened
    assert.deepEqual(client.unauthorized, [INVALID])
    assert.ok(observedLateMs >= -40 && observedLateMs < 700)
    assert.equal((await f.http('GET', '/api/notifications/socket-context', session.token)).status, 200,
      'The test backend still recognizes the extended database session, so the close specifically proves the service captured-minimum rule')
    t.diagnostic(JSON.stringify({ originalLifetimeMs: original - shortened + 2000, shortenedLifetimeMs: 2000,
      subsequentlyExtendedDatabaseLifetimeMs: 3_600_000, expiryObservedLateMs: observedLateMs }))
  })

  await t.test('retired raw polling cannot resume over a new TCP and its Engine.IO permit really becomes available', { timeout: 70_000 }, async () => {
    await until(async () => (await clients()) === 0, 'Previous namespace connections are closed')
    const base = f.notificationUrl + '/socket.io/?EIO=4&transport=polling'
    const raw = (url, options = {}) => rawHttp(url, { origin: f.backendUrl, ...options })
    const blocker = await f.socket(session)
    let opened
    await until(async () => {
      opened = await raw(base)
      return opened.status !== 503
    }, 'The earlier short-lived expiry flush returns its Engine.IO admission before raw polling begins')
    assert.equal(opened.status, 200)
    assert.ok(opened.body.startsWith('0{'))
    const handshake = JSON.parse(opened.body.slice(1))
    assert.equal(typeof handshake.sid, 'string')
    assert.ok(Number.isFinite(handshake.pingInterval) && handshake.pingInterval > 0)
    assert.ok(Number.isFinite(handshake.pingTimeout) && handshake.pingTimeout > 0)
    const url = base + '&sid=' + encodeURIComponent(handshake.sid)
    assert.equal((await raw(url, { method: 'POST', body: '40' })).status, 200)
    const connected = await raw(url)
    assert.equal(connected.status, 200)
    assert.ok(connected.body.split('\x1e').some(packet => packet.startsWith('40{')),
      'The raw peer really joins a namespace, cancelling the pre-namespace connect timeout')
    let recoveredSid
    try {
      assert.equal((await raw(base)).status, 503, 'Both real Engine.IO permits are occupied before rejection')
      assert.equal((await raw(url, { method: 'POST', body: '42' + JSON.stringify(['authenticate', { token: 'invalid-raw-polling-session' }]) })).status, 200)
      const packets = []
      let rejectionTransportClosed = false
      await until(async () => {
        const response = await raw(url, { timeout: 1500 })
        assert.notEqual(response.error, 'TEST_DEADLINE', 'The rejection stream must produce a real packet or close')
        if (response.status === 200) packets.push(...response.body.split('\x1e'))
        else {
          t.diagnostic(JSON.stringify({ phase: 'raw-polling-rejection-stream', actualStatus: response.status,
            transportError: response.error ?? null }))
          assert.ok(response.status === null || [400, 401, 403, 404, 410].includes(response.status))
          rejectionTransportClosed = true
        }
        return packets.includes('41') || rejectionTransportClosed
      }, 'Collect real rejection packets across polling responses until namespace disconnect or physical refusal', 3500)
      assert.deepEqual(packets.filter(packet => packet.startsWith('42')).map(packet => JSON.parse(packet.slice(2))), [['unauthorized', INVALID]])
      assert.ok(packets.includes('41') || rejectionTransportClosed)
      const rejectedAt = performance.now()
      // The server deliberately allows 100ms to flush unauthorized/41. Testing
      // old-SID pong rejection during that window sends an unsolicited Engine
      // heartbeat and can itself kill the session. Start new-TCP probes only
      // after a measured 150ms lower bound on the declared flush deadline.
      const flushNotBefore = rejectedAt + 150
      while (performance.now() < flushNotBefore) {
        await new Promise(resolve => setTimeout(resolve, Math.ceil(flushNotBefore - performance.now())))
      }
      const observedFlushWaitMs = performance.now() - rejectedAt
      assert.ok(observedFlushWaitMs >= 150)
      const oldGet = await raw(url, { timeout: 1500 })
      t.diagnostic(JSON.stringify({ phase: 'raw-polling-retired-new-tcp', method: 'GET', declaredFlushMs: 100,
        observedFlushWaitMs: Math.round(observedFlushWaitMs), actualStatus: oldGet.status, transportError: oldGet.error ?? null }))
      assert.notEqual(oldGet.error, 'TEST_DEADLINE', 'A retired polling GET must not await its next Engine.IO heartbeat')
      assert.ok(oldGet.status === null || [400, 401, 403, 404, 410].includes(oldGet.status))
      const oldPost = await raw(url, { method: 'POST', body: '3', timeout: 1000 })
      t.diagnostic(JSON.stringify({ phase: 'raw-polling-retired-new-tcp', method: 'POST', actualStatus: oldPost.status,
        transportError: oldPost.error ?? null }))
      assert.notEqual(oldPost.error, 'TEST_DEADLINE', 'A new TCP must be explicitly refused or reset, not left pending')
      assert.ok(oldPost.status === null || [400, 401, 403, 404, 410].includes(oldPost.status))
      const recoveryBudgetMs = Math.min(55_000, handshake.pingInterval + handshake.pingTimeout + 5000)
      let rejectedOpens = 0
      await until(async () => {
        const next = await raw(base)
        if (next.status === 503) { rejectedOpens++; return false }
        assert.equal(next.status, 200, 'The reclaimed permit is proved by an actual new Engine.IO handshake')
        assert.ok(next.body.startsWith('0{'))
        recoveredSid = JSON.parse(next.body.slice(1)).sid
        return true
      }, 'Retired raw polling releases Engine.IO admission within its announced heartbeat/timeout window', recoveryBudgetMs)
      const recoveryObservedMs = Math.round(performance.now() - rejectedAt)
      assert.ok(recoveryObservedMs <= recoveryBudgetMs)
      assert.equal(blocker.socket.connected, true, 'The unrelated live peer was never disconnected to manufacture capacity')
      t.diagnostic(JSON.stringify({ rawPollingNewTcpRejected: true, announcedPingIntervalMs: handshake.pingInterval,
        announcedPingTimeoutMs: handshake.pingTimeout, testedRecoveryBudgetMs: recoveryBudgetMs,
        recoveryObservedMs, rejectedNewOpensAfterRetirement: rejectedOpens }))
    } finally {
      // Only the newly admitted test session receives a cooperative Engine close.
      // The rejected raw SID must have been reclaimed by the server itself.
      if (recoveredSid) await raw(base + '&sid=' + encodeURIComponent(recoveredSid), { method: 'POST', body: '1' })
      blocker.socket.disconnect()
    }
  })

  await t.test('shutdown finishes within seven seconds with idle TCP and an incomplete HTTP request head', { timeout: 14_000 }, async () => {
    const address = new URL(f.notificationUrl)
    const peers = []
    const connect = async () => {
      const peer = net.createConnection({ host: address.hostname.replace(/^\[|\]$/g, ''), port: Number(address.port) })
      peers.push(peer)
      peer.on('error', () => {})
      await new Promise((resolve, reject) => { peer.once('connect', resolve); peer.once('error', reject) })
      return peer
    }
    let timer
    try {
      await connect() // Never sends a request, so it has no Engine/namespace group.
      const partial = await connect()
      await new Promise((resolve, reject) => partial.write('GET /health/live HTTP/1.1\r\nHost: owned-fixture\r\nX-Incomplete:', error => error ? reject(error) : resolve()))
      const started = performance.now()
      await Promise.race([
        f.service.stop(),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Graceful shutdown exceeded 7000ms with incomplete HTTP/TCP peers')), 7000) }),
      ])
      const elapsedMs = Math.round(performance.now() - started)
      assert.ok(elapsedMs < 7000)
      t.diagnostic(JSON.stringify({ shutdownTrigger: 'owned-stdin-EOF', idleTcpPeers: 1, incompleteHttpHeads: 1, shutdownObservedMs: elapsedMs, testedUpperBoundMs: 7000 }))
    } finally {
      clearTimeout(timer)
      for (const peer of peers) peer.destroy()
    }
  })
})
