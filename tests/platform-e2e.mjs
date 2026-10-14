// Opt-in real backend main + its real migrations/seed + PostgreSQL + Socket.IO.
// SQL below only adds test users/authorization in one random owned database.
// All credentials, sessions and application tokens come from real HTTP APIs.
import assert from 'node:assert/strict'
import test from 'node:test'
import { randomBytes, randomUUID, createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import { createServer } from 'node:http'
import { existsSync, lstatSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import pg from 'pg'
import { io as createClient } from 'socket.io-client'
import { loadConfig } from '../config.mjs'
import { createPostgresStore } from '../postgres-store.mjs'
import { createNotificationService } from '../service.mjs'

const adminUrl = process.env.NOTIFICATION_TEST_DATABASE_URL ?? process.env.TEST_DATABASE_URL
const binaryArgument = process.env.NOTIFICATION_E2E_BACKEND_BINARY
const DB_PREFIX = 'notification_platform_e2e_'
const TEMP_PREFIX = 'platform-notification-platform-e2e-'
const APP_A = 'p10-e2e-app-a'
const APP_B = 'p10-e2e-app-b'
const PUBLISHER = 'p10-e2e-publisher'
const RECIPIENT = 'p10-e2e-recipient'
const INVALID_SESSION = { code: 'SESSION_INVALID', message: '实时通知会话已失效' }
const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

function safeCode(error) {
  const code = error?.code
  if (typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code)) return code
  if (['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'ENOENT', 'EACCES', 'EPERM'].includes(code)) return code
  return 'UNAVAILABLE'
}

async function db(operation, callback) {
  try { return await callback() } catch (error) { throw new Error(`Real platform E2E ${operation} failed (${safeCode(error)})`) }
}
const query = (client, sql, values = []) => db('database query', () => client.query(sql, values))

function databaseClient(connectionString, applicationName) {
  try {
    return new pg.Client({ connectionString, application_name: applicationName, connectionTimeoutMillis: 3000, query_timeout: 10000 })
  } catch (error) { throw new Error(`Real platform E2E database client configuration failed (${safeCode(error)})`) }
}

function ownedDatabaseUrl(source, name) {
  try {
    const url = new URL(source)
    if (!['postgres:', 'postgresql:'].includes(url.protocol)) throw new Error()
    url.pathname = `/${name}`
    for (const key of ['database', 'dbname', 'application_name']) url.searchParams.delete(key)
    return url.toString()
  } catch { throw new Error('Real platform E2E requires a PostgreSQL test connection URL') }
}

async function until(predicate, message, timeout = 10000) {
  const deadline = Date.now() + timeout
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, message)
    await delay(25)
  }
}

async function freeIpv6Port() {
  const reservation = createServer()
  await new Promise((resolve, reject) => {
    reservation.once('error', () => reject(new Error('Real platform E2E IPv6 loopback listener is unavailable')))
    reservation.listen(0, '::1', resolve)
  })
  const port = reservation.address().port
  await new Promise((resolve, reject) => reservation.close((error) => error ? reject(new Error('Real platform E2E port reservation close failed')) : resolve()))
  return port
}

async function stopBackend(backend) {
  if (!backend || backend.closed) return
  if (backend.child.pid) backend.child.kill('SIGTERM')
  const wait = async (milliseconds) => {
    let timer
    try { await Promise.race([backend.exited, new Promise((resolve) => { timer = setTimeout(resolve, milliseconds) })]) }
    finally { clearTimeout(timer) }
  }
  await wait(3000)
  if (!backend.closed && backend.child.pid) { backend.child.kill('SIGKILL'); await wait(3000) }
  assert.ok(backend.closed, 'only the test-owned backend must be stopped before database cleanup')
}

async function fixture(t) {
  const binary = path.resolve(binaryArgument)
  assert.ok(existsSync(binary), 'NOTIFICATION_E2E_BACKEND_BINARY must identify the compiled real backend main')
  const expectedDatabase = `${DB_PREFIX}${randomUUID().replaceAll('-', '')}`
  const databaseName = expectedDatabase
  const databaseUrl = ownedDatabaseUrl(adminUrl, databaseName)
  const tempBase = realpathSync(os.tmpdir())
  const root = realpathSync(mkdtempSync(path.join(tempBase, TEMP_PREFIX)))
  const ownedRoot = root
  const secret = randomBytes(48).toString('hex')
  const secrets = new Set([secret, adminUrl, databaseUrl])
  const logs = []
  const sockets = []
  const transitions = []
  let admin
  let writer
  let created = false
  let adminConnected = false
  let backend
  let service
  t.after(async () => {
    const errors = []
    for (const item of sockets) item.socket.disconnect()
    try { await service?.stop() } catch { errors.push('owned notification service stop failed') }
    try { await stopBackend(backend) } catch { errors.push('owned backend stop failed') }
    const backendStopped = !backend || backend.closed
    if (!backendStopped) errors.push('backend exit was not confirmed; owned database and temporary directory retained')
    try { await writer?.end() } catch { errors.push('owned writer close failed') }
    try {
      if (created && adminConnected && backendStopped) {
        assert.equal(databaseName, expectedDatabase)
        assert.match(databaseName, /^notification_platform_e2e_[0-9a-f]{32}$/)
        assert.ok(databaseName.startsWith(DB_PREFIX))
        await query(admin, `DROP DATABASE "${databaseName}" WITH (FORCE)`)
        created = false
      }
    } catch { errors.push('owned database cleanup failed') }
    finally { try { await admin?.end() } catch { errors.push('owned admin close failed') } }
    if (backendStopped) {
      const absolute = path.resolve(root)
      const relative = path.relative(tempBase, absolute)
      assert.equal(absolute, ownedRoot)
      assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative))
      assert.equal(path.dirname(absolute), tempBase)
      assert.ok(path.basename(absolute).startsWith(TEMP_PREFIX))
      assert.ok(!lstatSync(absolute).isSymbolicLink())
      rmSync(absolute, { recursive: true, force: true })
    }
    const captured = logs.join('\n')
    for (const value of secrets) assert.ok(!captured.includes(value), 'backend/service logs must not contain connection URLs, signing secrets, tickets or Bearers')
    assert.equal(errors.length, 0, errors.join('; '))
  })
  admin = databaseClient(adminUrl, 'notification-platform-e2e-admin')
  writer = databaseClient(databaseUrl, 'notification-platform-e2e-writer')
  admin.on('error', () => logs.push('test administration connection unavailable'))
  writer.on('error', () => logs.push('test writer connection unavailable'))
  await db('administration connect', () => admin.connect())
  adminConnected = true
  assert.match(databaseName, /^notification_platform_e2e_[0-9a-f]{32}$/)
  await query(admin, `CREATE DATABASE "${databaseName}" TEMPLATE template0`)
  created = true
  await db('isolated writer connect', () => writer.connect())
  assert.equal((await query(writer, 'SELECT current_database() AS name')).rows[0].name, databaseName)

  const port = await freeIpv6Port()
  const base = `http://[::1]:${port}`
  // main uses dotenv_override(). An empty owned .env prevents ancestor/project
  // credential files from overriding the explicitly passed isolated DB/JWT env.
  writeFileSync(path.join(root, '.env'), '')
  const child = spawn(binary, [], {
    cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env, DATABASE_URL: databaseUrl, JWT_SECRET: secret,
      BIND_ADDR: `[::1]:${port}`, PUBLIC_APP_URL: base, RUST_LOG: 'warn',
      AUTO_CREATE_DATABASE: 'false', STATIC_DIR: '', APPLICATION_ENVIRONMENT: 'development',
      MICRO_GATEWAY_ORIGIN: base, MICRO_ALLOWED_ORIGINS: base,
      SESSION_TTL_HOURS: '1', ACCESS_TOKEN_TTL_SECONDS: '300',
    },
  })
  backend = { child, closed: false, error: null, exited: null }
  backend.exited = new Promise((resolve) => child.once('close', () => { backend.closed = true; resolve() }))
  child.on('error', (error) => { backend.error = safeCode(error) })
  child.stdout.on('data', (bytes) => logs.push(bytes.toString()))
  child.stderr.on('data', (bytes) => logs.push(bytes.toString()))

  async function http(method, endpoint, token, body, timeout = 10000) {
    const headers = { Accept: 'application/json', 'User-Agent': 'notification-real-platform-e2e' }
    if (token) headers.Authorization = `Bearer ${token}`
    if (body !== undefined) headers['Content-Type'] = 'application/json'
    let response
    try { response = await fetch(new URL(endpoint, base), { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(timeout), redirect: 'error' }) }
    catch (error) { throw new Error(`Real platform E2E HTTP request failed (${safeCode(error)})`) }
    let data
    try { data = await response.json() } catch { throw new Error('Real platform E2E HTTP response was not valid JSON') }
    return { status: response.status, data }
  }
  async function ok(method, endpoint, token, body, expected = 200) {
    const response = await http(method, endpoint, token, body)
    assert.equal(response.status, expected, 'real platform API must return the expected status without exposing response bodies')
    return response.data
  }
  await until(async () => {
    assert.ok(!backend.error && !backend.closed, 'compiled real backend exited before readiness')
    try { return (await http('GET', '/api/health', null, undefined, 1000)).status === 200 } catch { return false }
  }, 'real backend migrations, seed and listener must become ready', 45000)
  const migration = await query(writer, 'SELECT success FROM _sqlx_migrations WHERE version=22')
  assert.deepEqual(migration.rows, [{ success: true }], 'the supplied real backend binary must contain notification migration 0022')

  await query(writer, "INSERT INTO tenants(name,code,is_platform) VALUES('E2E tenant A','p10-e2e-a',false),('E2E tenant B','p10-e2e-b',false)")
  await query(writer, `INSERT INTO organizations(tenant_id,name,code)
    SELECT id,CASE code WHEN 'p10-e2e-a' THEN 'E2E A root' ELSE 'E2E B root' END,
      CASE code WHEN 'p10-e2e-a' THEN 'p10-e2e-a-root' ELSE 'p10-e2e-b-root' END
    FROM tenants WHERE code IN ('p10-e2e-a','p10-e2e-b')`)
  await query(writer, `INSERT INTO organizations(tenant_id,parent_id,name,code)
    SELECT tenant_id,id,'E2E A child','p10-e2e-a-child' FROM organizations WHERE code='p10-e2e-a-root'`)
  const copiedUsers = await query(writer, `INSERT INTO users(username,password_hash,real_name,is_super_admin)
    SELECT fixture.username,seeded.password_hash,fixture.name,false FROM users seeded
    CROSS JOIN (VALUES($1::text,'E2E publisher'),($2::text,'E2E recipient')) fixture(username,name)
    WHERE seeded.username='admin'`, [PUBLISHER, RECIPIENT])
  assert.equal(copiedUsers.rowCount, 2, 'real seed admin hash must initialize two ordinary test users')
  await query(writer, `INSERT INTO applications(code,name,base_path,entry_type,entry_url)
    VALUES($1,'E2E app A',$2,'HTML',$3),($4,'E2E app B',$5,'HTML',$6)`,
  [APP_A, `/micro/${APP_A}`, `/sub-apps/${APP_A}/index.html`, APP_B, `/micro/${APP_B}`, `/sub-apps/${APP_B}/index.html`])
  const clientHash = createHash('sha256').update(randomBytes(32)).digest('hex')
  await query(writer, `INSERT INTO oauth_clients(client_id,client_secret_hash,client_name,client_type,grant_types,scopes,access_token_validity,application_id)
    SELECT code||'-client',$1,name||' client','CONFIDENTIAL','client_credentials','profile',300,id
    FROM applications WHERE code=ANY($2::text[])`, [clientHash, [APP_A, APP_B]])
  await query(writer, `INSERT INTO memberships(user_id,organization_id)
    SELECT u.id,o.id FROM users u CROSS JOIN organizations o
    WHERE u.username=ANY($1::text[]) AND o.code=ANY($2::text[])`, [[PUBLISHER, RECIPIENT], ['p10-e2e-a-root', 'p10-e2e-a-child', 'p10-e2e-b-root']])
  await query(writer, `INSERT INTO tenant_applications(tenant_id,application_id)
    SELECT t.id,a.id FROM tenants t CROSS JOIN applications a
    WHERE t.code IN ('p10-e2e-a','p10-e2e-b') AND a.code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO organization_applications(organization_id,application_id)
    SELECT o.id,a.id FROM organizations o CROSS JOIN applications a
    WHERE o.code IN ('p10-e2e-a-root','p10-e2e-a-child','p10-e2e-b-root') AND a.code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO menus(application_id,type,title,permission,visible)
    SELECT id,'BUTTON','Publish','notification:publish',false FROM applications WHERE code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO tenant_menus(tenant_id,menu_id)
    SELECT t.id,m.id FROM tenants t CROSS JOIN menus m JOIN applications a ON a.id=m.application_id
    WHERE t.code IN ('p10-e2e-a','p10-e2e-b') AND a.code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO organization_menus(organization_id,menu_id)
    SELECT o.id,m.id FROM organizations o CROSS JOIN menus m JOIN applications a ON a.id=m.application_id
    WHERE o.code IN ('p10-e2e-a-root','p10-e2e-a-child','p10-e2e-b-root') AND a.code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO roles(tenant_id,organization_id,name,code)
    SELECT tenant_id,id,'E2E publisher','p10-e2e-publisher-role' FROM organizations WHERE code IN ('p10-e2e-a-root','p10-e2e-b-root')`)
  await query(writer, `INSERT INTO role_menus(role_id,menu_id)
    SELECT r.id,m.id FROM roles r CROSS JOIN menus m JOIN applications a ON a.id=m.application_id
    WHERE r.code='p10-e2e-publisher-role' AND a.code=ANY($1::text[])`, [[APP_A, APP_B]])
  await query(writer, `INSERT INTO user_roles(user_id,role_id,organization_id)
    SELECT u.id,r.id,r.organization_id FROM users u CROSS JOIN roles r WHERE u.username=$1 AND r.code='p10-e2e-publisher-role'`, [PUBLISHER])
  await query(writer, `INSERT INTO membership_data_grants(membership_id,application_id,resource_code,action_code,scope_type)
    SELECT m.id,a.id,'notification','publish','TENANT' FROM memberships m JOIN users u ON u.id=m.user_id
    CROSS JOIN applications a WHERE u.username=$1 AND a.code=ANY($2::text[])`, [PUBLISHER, [APP_A, APP_B]])
  const ids = {}
  for (const [label, sql, values, key] of [
    ['tenants', 'SELECT id,code FROM tenants WHERE code=ANY($1::text[])', [['p10-e2e-a', 'p10-e2e-b']], 'code'],
    ['organizations', 'SELECT id,code,tenant_id FROM organizations WHERE code=ANY($1::text[])', [['p10-e2e-a-root', 'p10-e2e-a-child', 'p10-e2e-b-root']], 'code'],
    ['users', 'SELECT id,username,is_super_admin FROM users WHERE username=ANY($1::text[])', [[PUBLISHER, RECIPIENT]], 'username'],
    ['applications', 'SELECT id,code FROM applications WHERE code=ANY($1::text[])', [[APP_A, APP_B]], 'code'],
  ]) ids[label] = Object.fromEntries((await query(writer, sql, values)).rows.map((row) => [row[key], row]))
  assert.ok(Object.values(ids.users).every((row) => row.is_super_admin === false))
  const capture = (...args) => logs.push(JSON.stringify(args, (_, value) => value instanceof Error ? { ...value, message: value.message } : value))
  const logger = { info: capture, warn: capture, error: capture, debug: capture, log: capture }
  const config = { ...loadConfig({ DATABASE_URL: databaseUrl, PLATFORM_API_BASE_URL: base, NOTIFICATION_ALLOWED_ORIGINS: base }), port: 0, host: '::1', recheckMs: 1000, authTimeoutMs: 2000, authWaitMs: 5000, healthProbeMs: 30000, dbRetryMs: 8000 }
  const store = createPostgresStore(config, logger)
  service = createNotificationService(config, { logger, store: {
    ...store, start: (onHint, onState) => store.start(onHint, (ready) => { transitions.push(ready); onState(ready) }),
  } })
  const socketAddress = await service.start()
  await until(async () => (await service.probeHealth()).status === 'UP', 'real notification service must validate its real backend and PG dependencies')

  async function fromContextResponse(response) {
    assert.ok(typeof response.accessToken === 'string', 'real context must issue a session token')
    secrets.add(response.accessToken)
    const context = await ok('GET', '/api/notifications/socket-context', response.accessToken)
    assert.equal(context.contractVersion, '1.0')
    assert.equal(context.userId, response.user.id)
    assert.equal(context.tenantId, response.tenant.id)
    assert.equal(context.organizationId, response.organization.id)
    return { token: response.accessToken, context }
  }
  async function login(username, organizationId) {
    const credential = await ok('POST', '/api/auth/login', null, { username, password: 'admin123' })
    assert.ok(typeof credential.ticket === 'string', 'real login must issue the credential ticket')
    secrets.add(credential.ticket)
    assert.ok(credential.memberships.some((row) => row.organizationId === organizationId))
    return fromContextResponse(await ok('POST', '/api/auth/context', credential.ticket, { organizationId }))
  }
  async function appToken(session, code) {
    const issued = await ok('POST', '/api/oauth/session-token', session.token, { appCode: code }, 201)
    assert.ok(typeof issued.accessToken === 'string')
    secrets.add(issued.accessToken)
    assert.equal(issued.tokenType, 'Bearer')
    assert.equal(issued.audience, code)
    return issued.accessToken
  }
  async function socket(session) {
    const client = createClient(`http://[::1]:${socketAddress.port}`, { path: config.socketPath, transports: ['websocket'], extraHeaders: { Origin: base }, reconnection: false, forceNew: true, autoConnect: false, timeout: 2500 })
    const item = { socket: client, notifications: [], authenticated: [], unauthorized: [], errors: [] }
    sockets.push(item)
    for (const [event, key] of [['notification', 'notifications'], ['authenticated', 'authenticated'], ['unauthorized', 'unauthorized'], ['connect_error', 'errors']]) client.on(event, (value) => item[key].push(value))
    client.connect()
    await until(() => client.connected || item.errors.length, 'Socket.IO must connect to the real notification service')
    assert.ok(client.connected, 'real notification service connection failed')
    client.emit('authenticate', { token: session.token })
    await until(() => item.authenticated.length || item.unauthorized.length, 'real backend must authorize the socket session')
    assert.equal(item.unauthorized.length, 0)
    assert.equal(item.authenticated[0].organizationId, session.context.organizationId)
    return item
  }
  const counts = async () => (await query(writer, `SELECT (SELECT count(*) FROM notification_publications)::int AS publications,
    (SELECT count(*) FROM notifications)::int AS notifications`)).rows[0]
  const publicationRows = async (publicationId) => (await query(writer, `SELECT id,user_id AS "userId",tenant_id AS "tenantId",
    organization_id AS "organizationId",application_id AS "applicationId",publication_id AS "publicationId",source_kind AS "sourceKind",
    type,title,content,link,created_at AS "createdAt" FROM notifications WHERE publication_id=$1 ORDER BY organization_id,user_id`, [publicationId])).rows
    .map((row) => ({ ...row, createdAt: row.createdAt.toISOString() }))
  const body = (code, key, target) => ({ idempotencyKey: key, target, type: 'INFO', title: `Real pipeline ${key}`, content: 'Published by actual authorized application API', link: `/micro/${code}/detail` })
  const publish = async (code, token, value) => {
    const publication = await ok('POST', `/api/apps/${code}/notifications`, token, value)
    assert.ok(typeof publication.publicationId === 'string')
    return { publication, rows: await publicationRows(publication.publicationId) }
  }
  const notify = (row) => query(writer, "SELECT pg_notify('new_notification',$1)", [JSON.stringify({ id: row.id, userId: row.userId, tenantId: row.tenantId, organizationId: row.organizationId })])
  return { admin, writer, service, store, transitions, ids, sockets, secrets, login, appToken, socket, ok, http, counts, publicationRows, body, publish, notify, fromContextResponse, databaseName }
}

test('opt-in actual backend + PostgreSQL + Socket.IO notification publication and revocation pipeline', {
  skip: adminUrl && binaryArgument ? false : 'Set NOTIFICATION_E2E_BACKEND_BINARY and NOTIFICATION_TEST_DATABASE_URL or TEST_DATABASE_URL to run the actual platform E2E',
}, async (t) => {
  const f = await fixture(t)
  const publisherId = f.ids.users[PUBLISHER].id
  const recipientId = f.ids.users[RECIPIENT].id
  const tenantA = f.ids.tenants['p10-e2e-a'].id
  const orgA = f.ids.organizations['p10-e2e-a-root'].id
  const childA = f.ids.organizations['p10-e2e-a-child'].id
  const orgB = f.ids.organizations['p10-e2e-b-root'].id
  const appA = f.ids.applications[APP_A].id
  let sessionA = await f.login(PUBLISHER, orgA)
  const sessionChild = await f.login(PUBLISHER, childA)
  const sessionB = await f.login(PUBLISHER, orgB)
  const recipientSession = await f.login(RECIPIENT, orgA)
  let tokenA = await f.appToken(sessionA, APP_A)
  let tokenB = await f.appToken(sessionA, APP_B)
  let main = await f.socket(sessionA)
  const child = await f.socket(sessionChild)
  const remote = await f.socket(sessionB)
  const peer = await f.socket(recipientSession)
  const userTarget = { kind: 'USER', userId: publisherId, organizationId: orgA }
  let original
  let recipientRow
  const waitEvent = (client, id) => until(() => client.notifications.some((row) => row.id === id), 'the real committed notification must arrive on its exact authorized socket')
  const invalidated = async (client) => {
    await until(() => client.unauthorized.length && !client.socket.connected, 'real session invalidation must reject and disconnect its socket')
    assert.deepEqual(client.unauthorized, [INVALID_SESSION])
  }

  await t.test('actual publication persists and pushes safe rows with user/org/tenant isolation and direct organization targeting', async () => {
    const result = await f.publish(APP_A, tokenA, f.body(APP_A, 'first-user', userTarget))
    assert.equal(result.publication.recipientCount, 1)
    assert.equal(result.publication.deliveryCount, 1)
    assert.equal(result.publication.duplicate, false)
    assert.equal(result.rows.length, 1)
    original = result.rows[0]
    assert.equal(original.sourceKind, 'APP')
    await waitEvent(main, original.id)
    const event = main.notifications.find((row) => row.id === original.id)
    assert.equal(event.contractVersion, '1.0')
    for (const key of ['id', 'userId', 'tenantId', 'organizationId', 'applicationId', 'publicationId', 'type', 'title', 'content', 'link', 'createdAt']) assert.equal(event[key], original[key])
    assert.equal(event.sourceKind, undefined)
    assert.equal(child.notifications.length, 0)
    assert.equal(remote.notifications.length, 0)
    assert.equal(peer.notifications.length, 0)
    const organization = await f.publish(APP_A, tokenA, f.body(APP_A, 'organization-direct', { kind: 'ORGANIZATION', organizationId: orgA }))
    assert.equal(organization.publication.recipientCount, 2)
    assert.equal(organization.publication.deliveryCount, 2)
    assert.ok(organization.rows.every((row) => row.organizationId === orgA))
    recipientRow = organization.rows.find((row) => row.userId === recipientId)
    await waitEvent(peer, recipientRow.id)
    assert.equal(child.notifications.length, 0, 'organization target must not include descendant memberships')
    const tenant = await f.publish(APP_A, tokenA, f.body(APP_A, 'tenant-direct-members', { kind: 'TENANT' }))
    assert.equal(tenant.publication.recipientCount, 2)
    assert.equal(tenant.publication.deliveryCount, 4)
    assert.ok(tenant.rows.every((row) => row.tenantId === tenantA))
    const childRow = tenant.rows.find((row) => row.userId === publisherId && row.organizationId === childA)
    await waitEvent(child, childRow.id)
    assert.equal(remote.notifications.length, 0, 'same UID in the other tenant must never receive tenant-A deliveries')
  })

  await t.test('same key/body is idempotent and changed body conflicts without duplicate rows or events', async () => {
    const value = f.body(APP_A, 'first-user', userTarget)
    const before = await f.counts()
    const repeated = await f.publish(APP_A, tokenA, value)
    assert.equal(repeated.publication.duplicate, true)
    assert.equal(repeated.publication.publicationId, original.publicationId)
    assert.deepEqual(await f.counts(), before)
    const conflicting = await f.http('POST', `/api/apps/${APP_A}/notifications`, tokenA, { ...value, title: 'different content for the same key' })
    assert.equal(conflicting.status, 409)
    assert.deepEqual(await f.counts(), before)
    const barrier = await f.publish(APP_B, tokenB, f.body(APP_B, 'dedupe-barrier', userTarget))
    await waitEvent(main, barrier.rows[0].id)
    assert.equal(main.notifications.filter((row) => row.id === original.id).length, 1)
  })

  await t.test('session type, application audience, operation permission, explicit data grant and cross-tenant target reject with zero writes', async () => {
    const rejectWithoutWrites = async (code, bearer, value, status) => {
      const before = await f.counts()
      const response = await f.http('POST', `/api/apps/${code}/notifications`, bearer, value)
      assert.equal(response.status, status)
      assert.deepEqual(await f.counts(), before)
    }
    await rejectWithoutWrites(APP_A, sessionA.token, f.body(APP_A, 'wrong-token-type', userTarget), 401)
    await rejectWithoutWrites(APP_B, tokenA, f.body(APP_B, 'wrong-audience', userTarget), 403)
    await rejectWithoutWrites(APP_A, tokenA, f.body(APP_A, 'cross-tenant-target', { kind: 'USER', userId: publisherId, organizationId: orgB }), 404)
    const roleMenu = (await query(f.writer, `SELECT rm.role_id,rm.menu_id FROM role_menus rm JOIN roles r ON r.id=rm.role_id
      JOIN menus m ON m.id=rm.menu_id WHERE r.organization_id=$1 AND m.application_id=$2 AND m.permission='notification:publish'`, [orgA, appA])).rows
    assert.equal(roleMenu.length, 1)
    await query(f.writer, 'DELETE FROM role_menus WHERE role_id=$1 AND menu_id=$2', [roleMenu[0].role_id, roleMenu[0].menu_id])
    try { await rejectWithoutWrites(APP_A, tokenA, f.body(APP_A, 'missing-operation', userTarget), 403) }
    finally { await query(f.writer, 'INSERT INTO role_menus(role_id,menu_id) VALUES($1,$2)', [roleMenu[0].role_id, roleMenu[0].menu_id]) }
    const grant = (await query(f.writer, `DELETE FROM membership_data_grants g USING memberships m
      WHERE g.membership_id=m.id AND m.user_id=$1 AND m.organization_id=$2 AND g.application_id=$3
      AND g.resource_code='notification' AND g.action_code='publish' RETURNING g.membership_id,g.scope_type`, [publisherId, orgA, appA])).rows
    assert.equal(grant.length, 1)
    try { await rejectWithoutWrites(APP_A, tokenA, f.body(APP_A, 'missing-data-grant', userTarget), 403) }
    finally { await query(f.writer, `INSERT INTO membership_data_grants(membership_id,application_id,resource_code,action_code,scope_type)
      VALUES($1,$2,'notification','publish',$3)`, [grant[0].membership_id, appA, grant[0].scope_type]) }
  })

  await t.test('historical APP visibility and late real DB hints recheck both app grant layers and enabled state while another app still delivers', async () => {
    const mutations = [
      { name: 'organization', apply: () => query(f.writer, 'DELETE FROM organization_applications WHERE organization_id=$1 AND application_id=$2', [orgA, appA]), restore: () => query(f.writer, 'INSERT INTO organization_applications(organization_id,application_id) VALUES($1,$2)', [orgA, appA]) },
      { name: 'tenant', apply: () => query(f.writer, 'DELETE FROM tenant_applications WHERE tenant_id=$1 AND application_id=$2', [tenantA, appA]), restore: () => query(f.writer, 'INSERT INTO tenant_applications(tenant_id,application_id) VALUES($1,$2)', [tenantA, appA]) },
      { name: 'disabled', apply: () => query(f.writer, "UPDATE applications SET status='DISABLED' WHERE id=$1", [appA]), restore: () => query(f.writer, "UPDATE applications SET status='ENABLED' WHERE id=$1", [appA]) },
    ]
    for (const mutation of mutations) {
      await mutation.apply()
      try {
        const before = main.notifications.filter((row) => row.id === original.id).length
        const context = await f.http('GET', `/api/notifications/socket-context?notificationId=${original.id}`, sessionA.token)
        assert.equal(context.status, 404)
        const inbox = await f.ok('GET', '/api/notifications?size=200', sessionA.token)
        assert.ok(inbox.records.every((row) => row.applicationId !== appA))
        await f.notify(original)
        const barrier = await f.publish(APP_B, tokenB, f.body(APP_B, `authorization-barrier-${mutation.name}`, userTarget))
        await waitEvent(main, barrier.rows[0].id)
        assert.equal(main.notifications.filter((row) => row.id === original.id).length, before)
        assert.ok(main.socket.connected)
        assert.equal(main.unauthorized.length, 0)
      } finally { await mutation.restore() }
    }
    tokenA = await f.appToken(sessionA, APP_A)
  })

  await t.test('offline socket publication remains persisted and real inbox/read/count recover within the exact context', async () => {
    main.socket.disconnect()
    const offline = await f.publish(APP_B, tokenB, f.body(APP_B, 'offline-socket', userTarget))
    assert.equal(offline.rows.length, 1)
    main = await f.socket(sessionA)
    const inbox = await f.ok('GET', '/api/notifications?size=200', sessionA.token)
    assert.ok(inbox.records.some((row) => row.id === offline.rows[0].id))
    assert.ok(inbox.records.every((row) => row.userId === publisherId && row.tenantId === tenantA && row.organizationId === orgA))
    const unread = await f.ok('GET', '/api/notifications/unread-count', sessionA.token)
    assert.equal(unread.count, inbox.records.filter((row) => !row.isRead).length)
    const wrongContext = await f.ok('PUT', `/api/notifications/${offline.rows[0].id}/read`, sessionChild.token)
    assert.deepEqual(wrongContext, { ok: false })
    assert.equal((await query(f.writer, 'SELECT is_read FROM notifications WHERE id=$1', [offline.rows[0].id])).rows[0].is_read, false)
    assert.deepEqual(await f.ok('PUT', `/api/notifications/${offline.rows[0].id}/read`, sessionA.token), { ok: true })
    const after = await f.ok('GET', '/api/notifications/unread-count', sessionA.token)
    assert.equal(after.count, unread.count - 1)
  })

  await t.test('actual publication during owned LISTEN loss persists and reconnect receives the next new notification', async () => {
    const current = await query(f.admin, "SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name='platform-notification-listen' AND backend_type='client backend'", [f.databaseName])
    assert.equal(current.rows.length, 1)
    const pid = current.rows[0].pid
    assert.ok(Number.isSafeInteger(pid) && pid > 0)
    const start = f.transitions.length
    const stopped = await query(f.admin, "SELECT pg_terminate_backend(pid) AS stopped FROM pg_stat_activity WHERE datname=$1 AND application_name='platform-notification-listen' AND pid=$2", [f.databaseName, pid])
    assert.deepEqual(stopped.rows, [{ stopped: true }])
    await until(() => f.transitions.slice(start).includes(false), 'only the owned listener loss must report DOWN')
    assert.equal((await f.service.probeHealth()).checks.listen, 'DOWN')
    const offline = await f.publish(APP_B, tokenB, f.body(APP_B, 'offline-listen', userTarget))
    assert.equal(f.transitions.at(-1), false, 'actual publication must commit during the observed LISTEN downtime')
    assert.equal(offline.rows.length, 1)
    assert.ok((await f.ok('GET', '/api/notifications?size=200', sessionA.token)).records.some((row) => row.id === offline.rows[0].id))
    await until(async () => (await f.service.probeHealth()).status === 'UP', 'real PostgreSQL listener must reconnect', 25000)
    assert.ok(f.transitions.slice(start).includes(true))
    const nextPid = await query(f.admin, "SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name='platform-notification-listen'", [f.databaseName])
    assert.equal(nextPid.rows.length, 1)
    assert.notEqual(nextPid.rows[0].pid, pid)
    const next = await f.publish(APP_B, tokenB, f.body(APP_B, 'after-listen-reconnect', userTarget))
    await waitEvent(main, next.rows[0].id)
  })

  await t.test('real context switch invalidates the old socket while the new tenant and independent peer contexts stay isolated', async () => {
    remote.socket.disconnect()
    const old = main
    const switched = await f.ok('POST', '/api/auth/context', sessionA.token, { organizationId: orgB })
    const next = await f.fromContextResponse(switched)
    assert.equal(next.context.organizationId, orgB)
    await invalidated(old)
    const newRemote = await f.socket(next)
    const newRemoteToken = await f.appToken(next, APP_B)
    const beforePeer = peer.notifications.length
    const publication = await f.publish(APP_B, newRemoteToken, f.body(APP_B, 'switched-tenant', { kind: 'USER', userId: publisherId, organizationId: orgB }))
    await waitEvent(newRemote, publication.rows[0].id)
    assert.equal(peer.notifications.length, beforePeer)
    assert.ok(peer.socket.connected)
    const freshA = await f.login(PUBLISHER, orgA)
    sessionA = freshA
    main = await f.socket(freshA)
    tokenA = await f.appToken(freshA, APP_A)
    tokenB = await f.appToken(freshA, APP_B)
    const aBarrier = await f.publish(APP_A, tokenA, f.body(APP_A, 'independent-root-a', userTarget))
    await waitEvent(main, aBarrier.rows[0].id)
    assert.equal(newRemote.notifications.filter((row) => row.id === aBarrier.rows[0].id).length, 0)
    assert.deepEqual(await f.ok('POST', '/api/auth/logout', next.token), { ok: true })
    await invalidated(newRemote)
    assert.ok(main.socket.connected, 'revoking one real session must not revoke the separately logged-in context')
  })

  await t.test('real membership deletion revokes its bound socket and late delivery while valid peer publication remains available', async () => {
    const before = peer.notifications.length
    await query(f.writer, 'DELETE FROM memberships WHERE user_id=$1 AND organization_id=$2', [recipientId, orgA])
    await f.notify(recipientRow)
    await invalidated(peer)
    assert.equal(peer.notifications.length, before)
    const invalid = await f.http('GET', '/api/notifications/socket-context', recipientSession.token)
    assert.equal(invalid.status, 401)
    const barrier = await f.publish(APP_B, tokenB, f.body(APP_B, 'after-member-revocation', userTarget))
    await waitEvent(main, barrier.rows[0].id)
    const scopedSibling = await f.login(RECIPIENT, childA)
    assert.equal(scopedSibling.context.organizationId, childA, 'deleting one membership must preserve the different direct membership')
  })

  for (const item of f.sockets) {
    const serialized = JSON.stringify({ authenticated: item.authenticated, unauthorized: item.unauthorized, notifications: item.notifications })
    for (const secret of f.secrets) assert.ok(!serialized.includes(secret), 'Socket.IO public events must not disclose credentials or signing secrets')
  }
})
