// Opt-in real PostgreSQL integration. The schema below is a minimal SQL fixture;
// platform authentication is an explicit HTTP fixture, not the real backend.
// No credentials are read from files and no connection URL is printed.
import assert from 'node:assert/strict'
import test from 'node:test'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import pg from 'pg'
import { io as createClient } from 'socket.io-client'
import { loadConfig } from '../config.mjs'
import { createPostgresStore } from '../postgres-store.mjs'
import { createNotificationService } from '../service.mjs'

const adminUrl = process.env.NOTIFICATION_TEST_DATABASE_URL ?? process.env.TEST_DATABASE_URL
const DATABASE_PREFIX = 'notification_ws_test_'
const ORIGIN = 'https://postgres-notification-fixture.example.test'
const LISTENER_APPLICATION = 'platform-notification-listen'
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function safeDatabaseError(error, operation) {
  const code = typeof error?.code === 'string' && /^[A-Z0-9_]+$/.test(error.code) ? error.code : 'DATABASE_ERROR'
  return new Error(`Real PostgreSQL fixture ${operation} failed (${code})`)
}

async function dbOperation(operation, callback) {
  try { return await callback() } catch (error) { throw safeDatabaseError(error, operation) }
}

const query = (client, sql, values = []) => dbOperation('query', () => client.query(sql, values))

function databaseClient(connectionString, applicationName) {
  try {
    return new pg.Client({ connectionString, application_name: applicationName, connectionTimeoutMillis: 3000, query_timeout: 5000 })
  } catch (error) { throw safeDatabaseError(error, 'client construction') }
}

function isolatedUrl(source, name) {
  try {
    const url = new URL(source)
    if (!['postgres:', 'postgresql:'].includes(url.protocol)) throw new Error()
    url.pathname = `/${name}`
    // pg connectionString parameters can override Client constructor labels.
    url.searchParams.delete('application_name')
    url.searchParams.delete('database')
    url.searchParams.delete('dbname')
    return url.toString()
  } catch { throw new Error('Real PostgreSQL fixture requires a valid PostgreSQL connection URL') }
}

async function until(predicate, message, timeout = 8000) {
  const deadline = Date.now() + timeout
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, message)
    await delay(20)
  }
}

function scopedContext(userId, tenantId, organizationId) {
  return { contractVersion: '1.0', userId, tenantId, organizationId, sessionId: randomUUID(), expiresAt: new Date(Date.now() + 180_000).toISOString() }
}

function rowFor(context, sourceKind = 'APP') {
  return {
    id: randomUUID(), userId: context.userId, tenantId: context.tenantId, organizationId: context.organizationId,
    sourceKind, applicationId: sourceKind === 'APP' ? randomUUID() : null, publicationId: sourceKind === 'APP' ? randomUUID() : null,
    type: 'INFO', title: `Real PostgreSQL ${sourceKind} fixture`, content: 'Real persistence; platform authentication remains an HTTP fixture', link: '/fixture/notifications', createdAt: '2026-10-02T12:00:00.000Z',
  }
}

const hintFor = (row) => ({ id: row.id, userId: row.userId, tenantId: row.tenantId, organizationId: row.organizationId })

async function insertRow(writer, row, notify = false) {
  await query(writer, 'BEGIN')
  try {
    await query(writer, `INSERT INTO notifications
      (id,user_id,tenant_id,organization_id,source_kind,application_id,publication_id,type,title,content,link,created_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [row.id, row.userId, row.tenantId, row.organizationId, row.sourceKind, row.applicationId, row.publicationId, row.type, row.title, row.content, row.link, row.createdAt])
    if (notify) await query(writer, "SELECT pg_notify('new_notification',$1)", [JSON.stringify(hintFor(row))])
    await query(writer, 'COMMIT')
  } catch (error) {
    await query(writer, 'ROLLBACK').catch(() => {})
    throw error
  }
}

test('opt-in real PostgreSQL / HTTP auth fixture: persistence, scoped LISTEN delivery and reconnect', {
  skip: adminUrl ? false : 'No NOTIFICATION_TEST_DATABASE_URL or TEST_DATABASE_URL supplied; real PostgreSQL integration is opt-in',
}, async (t) => {
  const nonce = randomUUID().replaceAll('-', '')
  const expectedName = `${DATABASE_PREFIX}${nonce}`
  const databaseName = expectedName
  const databaseUrl = isolatedUrl(adminUrl, databaseName)
  let created = false
  let adminConnected = false
  let service
  let realStore
  let platform
  const clients = []
  const listenTransitions = []
  const logs = []
  const tokens = new Map()
  const platformRequests = []
  const admin = databaseClient(adminUrl, 'notification-ws-fixture-admin')
  const writer = databaseClient(databaseUrl, 'notification-ws-fixture-writer')
  admin.on('error', () => logs.push('fixture admin connection unavailable'))
  writer.on('error', () => logs.push('fixture writer connection unavailable'))
  t.after(async () => {
    const failures = []
    for (const client of clients) client.socket.disconnect()
    try { await service?.stop() } catch { failures.push('service stop failed') }
    try { await writer.end() } catch { failures.push('fixture writer close failed') }
    if (platform?.listening) {
      platform.closeAllConnections()
      try { await new Promise((resolve, reject) => platform.close((error) => error ? reject(error) : resolve())) }
      catch { failures.push('fixture platform close failed') }
    }
    try {
      if (created && adminConnected) {
        assert.equal(databaseName, expectedName, 'cleanup database must be the UUID-derived name from this test')
        assert.match(databaseName, /^notification_ws_test_[0-9a-f]{32}$/)
        assert.ok(databaseName.startsWith(DATABASE_PREFIX))
        await query(admin, `DROP DATABASE "${databaseName}" WITH (FORCE)`)
        created = false
      }
    } catch { failures.push('owned fixture database cleanup failed') }
    finally {
      try { await admin.end() } catch { failures.push('fixture admin close failed') }
      adminConnected = false
    }
    assert.equal(failures.length, 0, failures.join('; '))
  })

  await dbOperation('admin connect', () => admin.connect())
  adminConnected = true
  assert.match(databaseName, /^notification_ws_test_[0-9a-f]{32}$/)
  await query(admin, `CREATE DATABASE "${databaseName}" TEMPLATE template0`)
  // Ownership is recorded only after successful creation. A name collision can
  // never cause this test to DROP a pre-existing database.
  created = true
  await dbOperation('isolated writer connect', () => writer.connect())
  const activeDatabase = await query(writer, 'SELECT current_database() AS name')
  assert.equal(activeDatabase.rows[0].name, databaseName, 'all fixture DDL must run in the newly created database')
  await query(writer, 'SET search_path TO public')
  await query(writer, `CREATE TABLE notifications (
    id uuid PRIMARY KEY, user_id uuid NOT NULL, tenant_id uuid, organization_id uuid,
    source_kind text NOT NULL CHECK(source_kind IN ('APP','SYSTEM','LEGACY')),
    application_id uuid, publication_id uuid,
    type text NOT NULL, title text NOT NULL, content text, link text, created_at timestamptz NOT NULL,
    CHECK((source_kind='APP' AND tenant_id IS NOT NULL AND organization_id IS NOT NULL AND application_id IS NOT NULL AND publication_id IS NOT NULL)
      OR (source_kind='SYSTEM' AND tenant_id IS NOT NULL AND organization_id IS NOT NULL AND application_id IS NULL AND publication_id IS NULL)
      OR (source_kind='LEGACY' AND tenant_id IS NULL AND organization_id IS NULL AND application_id IS NULL AND publication_id IS NULL))
  )`)

  const primary = scopedContext(randomUUID(), randomUUID(), randomUUID())
  const scopes = [primary,
    scopedContext(primary.userId, randomUUID(), primary.organizationId),
    scopedContext(primary.userId, primary.tenantId, randomUUID()),
    scopedContext(randomUUID(), primary.tenantId, primary.organizationId),
  ]
  platform = createServer((incoming, outgoing) => {
    const url = new URL(incoming.url, 'http://platform-auth.fixture')
    const authorization = incoming.headers.authorization
    platformRequests.push({ pathname: url.pathname, notificationId: url.searchParams.get('notificationId'), authorization })
    if (incoming.method !== 'GET' || url.pathname !== '/api/notifications/socket-context') {
      outgoing.writeHead(404); outgoing.end(); return
    }
    const token = authorization?.startsWith('Bearer ') ? authorization.slice(7) : ''
    const context = tokens.get(token)
    outgoing.writeHead(context ? 200 : 401, { 'Content-Type': 'application/json' })
    outgoing.end(JSON.stringify(context ?? { message: 'HTTP auth fixture requires a session' }))
  })
  await new Promise((resolve, reject) => {
    platform.once('error', reject)
    platform.listen(0, '127.0.0.1', resolve)
  })
  const config = {
    ...loadConfig({ DATABASE_URL: databaseUrl, PLATFORM_API_BASE_URL: `http://127.0.0.1:${platform.address().port}`, NOTIFICATION_ALLOWED_ORIGINS: ORIGIN }),
    port: 0, host: '127.0.0.1', authTimeoutMs: 1000, authWaitMs: 3000, recheckMs: 60_000, healthProbeMs: 60_000, dbRetryMs: 2000,
  }
  const capture = (...args) => logs.push(JSON.stringify(args, (_, value) => value instanceof Error ? { ...value, name: value.name, message: value.message } : value))
  const logger = { info: capture, warn: capture, error: capture, debug: capture, log: capture }
  realStore = await dbOperation('store construction', () => createPostgresStore(config, logger))
  const observedStore = {
    ...realStore,
    start: (onHint, onState) => realStore.start(onHint, (listening) => { listenTransitions.push(listening); onState(listening) }),
  }
  service = createNotificationService(config, { store: observedStore, logger })
  const address = await service.start()
  assert.ok(address?.port > 0)
  await until(async () => (await service.probeHealth()).status === 'UP', 'real PostgreSQL LISTEN and query pool must become healthy')

  for (const [index, context] of scopes.entries()) {
    const token = `REAL_PG_HTTP_FIXTURE_SESSION_${index}`
    tokens.set(token, context)
    const socket = createClient(`http://127.0.0.1:${address.port}`, { path: config.socketPath, transports: ['websocket'], extraHeaders: { Origin: ORIGIN }, reconnection: false, forceNew: true, autoConnect: false, timeout: 1500 })
    const client = { socket, token, notifications: [], authenticated: [], unauthorized: [], connectErrors: [] }
    clients.push(client)
    for (const [event, key] of [['notification', 'notifications'], ['authenticated', 'authenticated'], ['unauthorized', 'unauthorized'], ['connect_error', 'connectErrors']]) socket.on(event, (value) => client[key].push(value))
    socket.connect()
    await until(() => socket.connected || client.connectErrors.length, 'real Socket.IO fixture should connect')
    assert.ok(socket.connected, 'real Socket.IO fixture connection failed')
    socket.emit('authenticate', { token })
    await until(() => client.authenticated.length || client.unauthorized.length, 'HTTP auth fixture should bind the Socket.IO session')
    assert.equal(client.unauthorized.length, 0)
    assert.equal(client.authenticated[0].userId, context.userId)
  }

  const listeners = () => query(admin, `SELECT pid FROM pg_stat_activity
    WHERE datname=$1 AND application_name=$2 AND backend_type='client backend'`, [databaseName, LISTENER_APPLICATION])
  const appRow = rowFor(primary)
  await t.test('real PG transaction persists APP row and LISTEN delivers only its scoped recipient', async () => {
    await insertRow(writer, appRow, true)
    await until(() => clients[0].notifications.some((row) => row.id === appRow.id), 'committed real PostgreSQL APP hint should arrive')
    const event = clients[0].notifications.find((row) => row.id === appRow.id)
    for (const key of ['id', 'userId', 'tenantId', 'organizationId', 'applicationId', 'publicationId', 'title', 'content', 'createdAt']) assert.equal(event[key], appRow[key])
    const persisted = await query(writer, 'SELECT id,title,content FROM notifications WHERE id=$1', [appRow.id])
    assert.deepEqual(persisted.rows, [{ id: appRow.id, title: appRow.title, content: appRow.content }])
    assert.ok(platformRequests.some((entry) => entry.notificationId === appRow.id && entry.authorization === `Bearer ${clients[0].token}`))
    await delay(80)
    for (const peer of clients.slice(1)) assert.equal(peer.notifications.length, 0)
  })

  await t.test('real PG SYSTEM row uses null application and publication ownership', async () => {
    const row = rowFor(primary, 'SYSTEM')
    await insertRow(writer, row, true)
    await until(() => clients[0].notifications.some((event) => event.id === row.id), 'committed real PostgreSQL SYSTEM hint should arrive')
    const event = clients[0].notifications.find((entry) => entry.id === row.id)
    assert.equal(event.applicationId, null)
    assert.equal(event.publicationId, null)
    assert.equal(event.tenantId, primary.tenantId)
    assert.equal(event.organizationId, primary.organizationId)
  })

  await t.test('real PG SQL rejects legacy rows and each forged notification ownership key', async () => {
    const legacy = { ...rowFor(primary, 'LEGACY'), tenantId: null, organizationId: null }
    await insertRow(writer, legacy)
    const legacyHint = { ...hintFor(appRow), id: legacy.id }
    assert.equal(await dbOperation('legacy lookup', () => realStore.getNotification(legacyHint)), null)
    const invalidHints = [legacyHint,
      { ...hintFor(appRow), id: randomUUID() },
      { ...hintFor(appRow), tenantId: scopes[1].tenantId },
      { ...hintFor(appRow), organizationId: scopes[2].organizationId },
      { ...hintFor(appRow), userId: scopes[3].userId },
    ]
    const before = clients.map((client) => client.notifications.length)
    for (const forged of invalidHints) {
      assert.equal(await dbOperation('forged ownership lookup', () => realStore.getNotification(forged)), null, 'real SQL must match all four ownership keys')
      await query(writer, "SELECT pg_notify('new_notification',$1)", [JSON.stringify(forged)])
    }
    const barrier = rowFor(primary)
    await insertRow(writer, barrier, true)
    await until(() => clients[0].notifications.some((event) => event.id === barrier.id), 'a subsequent legitimate barrier must prove LISTEN processed the preceding forged hints')
    assert.deepEqual(clients[0].notifications.slice(before[0]).map((event) => event.id), [barrier.id])
    for (const [index, peer] of clients.entries()) if (index > 0) assert.equal(peer.notifications.length, before[index])
    const preserved = await query(writer, 'SELECT source_kind,tenant_id,organization_id FROM notifications WHERE id=$1', [legacy.id])
    assert.deepEqual(preserved.rows, [{ source_kind: 'LEGACY', tenant_id: null, organization_id: null }])
  })

  await t.test('only the unique owned LISTEN connection is terminated, health falls then reconnects and durable rows survive', async () => {
    const current = await listeners()
    assert.equal(current.rows.length, 1, 'termination requires exactly one LISTEN connection in the test-owned database')
    const oldPid = current.rows[0].pid
    assert.ok(Number.isSafeInteger(oldPid) && oldPid > 0)
    const beforeTransitions = listenTransitions.length
    const terminated = await query(admin, `SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity
      WHERE datname=$1 AND application_name=$2 AND pid=$3`, [databaseName, LISTENER_APPLICATION, oldPid])
    assert.deepEqual(terminated.rows, [{ stopped: true }], 'the only terminated connection must be the matched fixture listener')
    await until(() => listenTransitions.slice(beforeTransitions).includes(false), 'actual LISTEN loss must report DOWN')
    const down = await service.probeHealth()
    assert.equal(down.status, 'DOWN')
    assert.equal(down.checks.listen, 'DOWN')
    assert.equal(down.checks.database, 'UP', 'the persistent database can remain healthy while LISTEN is down')
    const missed = rowFor(primary)
    await insertRow(writer, missed, true)
    assert.equal(listenTransitions.at(-1), false, 'the persistent row must commit while the observed LISTEN connection is still DOWN')
    const persisted = await query(writer, 'SELECT id,title FROM notifications WHERE id=$1', [missed.id])
    assert.deepEqual(persisted.rows, [{ id: missed.id, title: missed.title }], 'a notification persisted during LISTEN downtime remains queryable')
    await until(async () => (await service.probeHealth()).status === 'UP', 'the real store must re-establish LISTEN')
    assert.ok(listenTransitions.slice(beforeTransitions).includes(true), 'reconnect must produce a new ready transition')
    const recovered = await listeners()
    assert.equal(recovered.rows.length, 1)
    assert.notEqual(recovered.rows[0].pid, oldPid, 'health must refer to a newly established listener')
    const next = rowFor(primary)
    await insertRow(writer, next, true)
    await until(() => clients[0].notifications.some((entry) => entry.id === next.id), 'a subsequent real NOTIFY must arrive after reconnect')
    assert.ok(clients[0].socket.connected)
    const allPersisted = await query(writer, 'SELECT id FROM notifications WHERE id=ANY($1::uuid[]) ORDER BY id', [[appRow.id, missed.id, next.id]])
    assert.equal(allPersisted.rows.length, 3)
  })

  assert.ok(!logs.join('\n').includes(adminUrl), 'service logs must not expose the admin connection URL')
  assert.ok(!logs.join('\n').includes(databaseUrl), 'service logs must not expose the isolated connection URL')
  for (const client of clients) {
    assert.ok(!JSON.stringify(client.notifications).includes(client.token))
    assert.ok(!logs.join('\n').includes(client.token), 'structured service logs must not expose fixture Bearer tokens')
  }
})
