import assert from 'node:assert/strict'
import test from 'node:test'
import { createRealPlatformFixture, PLATFORM_FIXTURE, until } from './helpers/rust-platform.mjs'
import { metricValue } from './helpers/rust-service.mjs'
import { isNotificationSocketContext, isRealtimeNotification } from '../../../frontend/src/lib/notification-protocol.ts'

const INVALID = { code: 'SESSION_INVALID', message: '实时通知会话已失效' }
const CONTEXT_FIELDS = ['contractVersion', 'expiresAt', 'organizationId', 'tenantId', 'userId']
const NOTIFICATION_FIELDS = ['applicationId', 'content', 'contractVersion', 'createdAt', 'id', 'link',
  'organizationId', 'publicationId', 'tenantId', 'title', 'type', 'userId']

function publicContext(value, identity) {
  assert.ok(isNotificationSocketContext(value, identity), 'The real authenticated event must satisfy the current frontend contract')
  assert.deepEqual(Object.keys(value).sort(), CONTEXT_FIELDS)
}

function publicNotification(value, identity) {
  assert.ok(isRealtimeNotification(value, identity), 'The real notification must satisfy the current frontend contract and recipient identity')
  assert.deepEqual(Object.keys(value).sort(), NOTIFICATION_FIELDS)
}

function watchDisconnect(client) {
  const reasons = []
  client.socket.on('disconnect', reason => reasons.push(reason))
  return reasons
}

async function retired(client, reasons) {
  await until(() => client.unauthorized.length && reasons.length && !client.socket.connected,
    'A terminal authentication decision must emit unauthorized and disconnect the namespace', 3500)
  assert.ok(JSON.stringify(client.unauthorized) === JSON.stringify([INVALID]),
    'The terminal event must contain only the fixed public SESSION_INVALID response')
  assert.deepEqual(reasons, ['io server disconnect'])
  assert.equal(client.socket.active, false)
  assert.equal(client.errors.length, 0)
}

function noResurrection(client, secrets) {
  assert.equal(client.authenticated.length, 0, 'A retired pending attempt cannot emit a late authenticated event')
  assert.equal(client.notifications.length, 0, 'A retired attempt cannot receive any later persisted notification')
  assert.equal(client.socket.connected, false)
  const publicEvents = JSON.stringify({ authenticated: client.authenticated, notifications: client.notifications,
    unauthorized: client.unauthorized })
  assert.ok([...secrets].filter(Boolean).every(secret => !publicEvents.includes(secret)),
    'Public terminal events must not expose fixture credentials, session tokens or database URLs')
}

test('Rust initial authentication preserves receive order and cancels pending real validation', { timeout: 180_000 }, async t => {
  const f = await createRealPlatformFixture(t, { platformProxy: true,
    config: { authWaitMs: 1000, authTimeoutMs: 5000, recheckMs: 60_000 },
    serviceEnv: { NOTIFICATION_MAX_CONNECTIONS: '4', NOTIFICATION_VALIDATION_CONCURRENCY: '4', NOTIFICATION_MAX_INFLIGHT: '4' },
  })
  const orgA = f.ids.organizations['p10-e2e-a-root'].id
  const orgB = f.ids.organizations['p10-e2e-b-root'].id
  const issued = new Set()
  const metric = async name => metricValue(await f.service.metrics(), name)
  const gate = async label => (await f.proxy.control('/test/state')).gates.find(value => value.label === label)
  const release = label => f.proxy.control('/test/release', { label })
  const addGate = (label, session, delayMs = 12_000) => f.proxy.control('/test/gates',
    { label, token: session.token, deliveryOnly: false, delayMs })
  const newSession = async (username = PLATFORM_FIXTURE.publisher, organization = orgA) => {
    const session = await f.login(username, organization)
    assert.ok(!issued.has(session.token), 'Every auth race uses a distinct actual platform session')
    issued.add(session.token)
    assert.ok(isNotificationSocketContext(session.context, session.context), 'A race begins with a valid actual platform identity')
    return session
  }
  const clean = async () => until(async () => {
    const metrics = await f.service.metrics()
    return metricValue(metrics, 'saas_notification_clients') === 0 &&
      metricValue(metrics, 'saas_notification_authenticated_clients') === 0 &&
      metricValue(metrics, 'saas_notification_validation_inflight') === 0
  }, 'Retirement returns namespace state and platform validation permits')
  const replacement = async session => {
    let client
    // Namespace metrics alone do not prove that an Engine.IO admission permit returned.
    await until(async () => {
      client = await f.socket(session, { authenticate: false, allowRejected: true })
      return client.socket.connected
    }, 'A fresh real Engine.IO handshake must become admissible after retirement', 5000)
    client.socket.emit('authenticate', { token: session.token })
    await until(() => client.authenticated.length || client.unauthorized.length,
      'A replacement must authenticate against the actual platform session')
    assert.equal(client.unauthorized.length, 0)
    assert.equal(client.authenticated.length, 1)
    publicContext(client.authenticated[0], session.context)
    return client
  }
  const deliver = async (client, session, title) => {
    const row = await f.insertSystem(session.context, title)
    await until(() => client.notifications.some(value => value.id === row.id), 'A replacement receives a real committed PostgreSQL notification')
    publicNotification(client.notifications.find(value => value.id === row.id), session.context)
    return row
  }

  await t.test('duplicate valid B retires A while A is held inside its actual initial HTTP validation', async () => {
    const a = await newSession(), b = await newSession(PLATFORM_FIXTURE.recipient, orgB)
    await addGate('pending-a', a)
    await addGate('pending-b', b, 0)
    try {
      const beforeDenials = await metric('saas_notification_auth_denials_total')
      const client = await f.socket(a, { authenticate: false })
      const reasons = watchDisconnect(client)
      const began = performance.now()
      client.socket.emit('authenticate', { token: a.token })
      await until(async () => (await gate('pending-a'))?.inflight === 1,
        'A must be pending in the real Rust proxy before B is sent')
      assert.ok(await metric('saas_notification_validation_inflight') >= 1,
        'The gated initial validation must retain a real shared HTTP permit')
      client.socket.emit('authenticate', { token: b.token })
      await retired(client, reasons)
      const rejectedMs = performance.now() - began
      assert.ok(rejectedMs < f.config.authWaitMs, 'Duplicate rejection must happen before the independent auth-wait deadline')
      await clean()
      assert.equal((await gate('pending-a')).count, 1)
      assert.equal((await gate('pending-b')).count, 0, 'B must not start a replacement platform identity validation')
      assert.equal(await metric('saas_notification_auth_denials_total'), beforeDenials + 1)
      const stillValid = await f.ok('GET', '/api/notifications/socket-context', a.token)
      assert.ok(isNotificationSocketContext(stillValid, a.context), 'A remains valid at the real upstream when its cancelled request is released')
      await release('pending-a'); await release('pending-b')
      await until(async () => (await gate('pending-a')).inflight === 0, 'Released proxy work must drain before checking for resurrection')
      noResurrection(client, f.secrets)

      const live = []
      try {
        for (const session of [a, b, a, b]) live.push(await replacement(session))
        assert.equal(await metric('saas_notification_authenticated_clients'), 4)
        const excess = await f.socket(a, { authenticate: false, allowRejected: true })
        assert.equal(excess.socket.connected, false)
        assert.ok(excess.errors.length > 0, 'Four new real Engine.IO connections occupy the configured cap and reject a fifth')
        assert.equal(excess.authenticated.length, 0)
        assert.equal(excess.unauthorized.length, 0)
        await deliver(live[0], a, 'Pending initial A cannot resurrect after duplicate B')
        await deliver(live[1], b, 'Duplicate B authenticates only on a new connection')
        noResurrection(client, f.secrets)
        assert.equal(await metric('saas_notification_authenticated_clients'), 4)
      } finally { for (const item of live) item.socket.disconnect() }
      await clean()
      t.diagnostic(JSON.stringify({ initialValidationRequests: 1, duplicateValidationRequests: 0,
        rejectedBeforeAuthWaitMs: Math.round(rejectedMs), freshAuthenticatedEngineConnections: 4, excessRejected: true }))
    } finally { await release('pending-a'); await release('pending-b') }
  })

  await t.test('auth-wait retires an actual pending validation before its longer HTTP deadline', async () => {
    const session = await newSession()
    await addGate('auth-wait', session)
    try {
      const beforeDenials = await metric('saas_notification_auth_denials_total')
      const client = await f.socket(session, { authenticate: false })
      const reasons = watchDisconnect(client)
      const began = performance.now()
      client.socket.emit('authenticate', { token: session.token })
      await until(async () => (await gate('auth-wait'))?.inflight === 1, 'Initial validation must really enter the upstream gate')
      assert.ok(await metric('saas_notification_validation_inflight') >= 1,
        'The gated initial validation must retain a real shared HTTP permit')
      await retired(client, reasons)
      const observedMs = performance.now() - began
      assert.ok(observedMs >= f.config.authWaitMs - 250 && observedMs < f.config.authTimeoutMs,
        'Auth-wait must independently reject while the longer HTTP request deadline is still pending')
      await clean()
      assert.equal((await gate('auth-wait')).count, 1)
      assert.equal(await metric('saas_notification_auth_denials_total'), beforeDenials + 1)
      const stillValid = await f.ok('GET', '/api/notifications/socket-context', session.token)
      assert.ok(isNotificationSocketContext(stillValid, session.context), 'Timeout retirement must not depend on the real session becoming invalid')
      await release('auth-wait')
      await until(async () => (await gate('auth-wait')).inflight === 0, 'The released real proxy request must drain')
      noResurrection(client, f.secrets)
      const live = await replacement(session)
      try {
        await deliver(live, session, 'Actual notification after initial authentication timeout')
        noResurrection(client, f.secrets)
        assert.equal(await metric('saas_notification_authenticated_clients'), 1)
      } finally { live.socket.disconnect() }
      await clean()
      t.diagnostic(JSON.stringify({ configuredAuthWaitMs: f.config.authWaitMs, configuredHttpDeadlineMs: f.config.authTimeoutMs,
        configuredProxyDelayMs: 12_000, observedRejectionMs: Math.round(observedMs), initialValidationRequests: 1 }))
    } finally { await release('auth-wait') }
  })

  await t.test('same-tick valid A then valid B cannot let B win receive order', async () => {
    const a = await newSession(), b = await newSession(PLATFORM_FIXTURE.recipient, orgB)
    await addGate('same-tick-a', a)
    await addGate('same-tick-b', b, 0)
    try {
      const client = await f.socket(a, { authenticate: false })
      const reasons = watchDisconnect(client)
      client.socket.emit('authenticate', { token: a.token })
      client.socket.emit('authenticate', { token: b.token })
      await retired(client, reasons)
      await clean()
      assert.ok((await gate('same-tick-a')).count <= 1, 'A may be cancelled before its spawned HTTP future begins')
      assert.equal((await gate('same-tick-b')).count, 0, 'B cannot acquire the first attempt by running its future sooner')
      await release('same-tick-a'); await release('same-tick-b')
      await until(async () => (await gate('same-tick-a')).inflight === 0, 'Any actual A validation must drain after release')
      noResurrection(client, f.secrets)
      const live = await replacement(b)
      try {
        await deliver(live, b, 'Same-tick B requires a new explicitly authenticated connection')
        noResurrection(client, f.secrets)
      } finally { live.socket.disconnect() }
      await clean()
    } finally { await release('same-tick-a'); await release('same-tick-b') }
  })

  await t.test('invalid first payload consumes the attempt before same-tick valid token', async () => {
    const session = await newSession(PLATFORM_FIXTURE.recipient, orgB)
    await addGate('invalid-first-valid-second', session, 0)
    try {
      const client = await f.socket(session, { authenticate: false })
      const reasons = watchDisconnect(client)
      client.socket.emit('authenticate', { token: session.token, organizationId: orgB })
      client.socket.emit('authenticate', { token: session.token })
      await retired(client, reasons)
      await clean()
      assert.equal((await gate('invalid-first-valid-second')).count, 0,
        'Invalid first JSON and the duplicate valid token must both be rejected before any platform validation')
      await release('invalid-first-valid-second')
      const live = await replacement(session)
      try {
        await deliver(live, session, 'A valid token cannot replace the invalid first authentication attempt')
        noResurrection(client, f.secrets)
      } finally { live.socket.disconnect() }
      await clean()
    } finally { await release('invalid-first-valid-second') }
  })

  await t.test('a real application bearer cannot replace the host platform session at socket admission', async () => {
    const session = await newSession()
    const applicationToken = await f.appToken(session, PLATFORM_FIXTURE.appA)
    assert.ok(applicationToken !== session.token, 'The real OAuth API must issue a distinct application bearer')
    const client = await f.socket(session, { authenticate: false })
    const reasons = watchDisconnect(client)
    client.socket.emit('authenticate', { token: applicationToken })
    await retired(client, reasons)
    await clean()
    noResurrection(client, f.secrets)
    const live = await replacement(session)
    try {
      await deliver(live, session, 'Only the actual host session authenticates the notification socket')
      noResurrection(client, f.secrets)
      assert.equal(await metric('saas_notification_authenticated_clients'), 1)
    } finally { live.socket.disconnect() }
    await clean()
  })
})
