// Real Rust platform main, migrations/seed, PostgreSQL and Rust notifications.
// SQL below only adds test users/authorization in one random owned database.
// All credentials, sessions and application tokens come from real HTTP APIs.
import assert from 'node:assert/strict'
import { randomBytes, randomUUID, createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import { createServer } from 'node:http'
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import os from 'node:os'
import path from 'node:path'
import pg from 'pg'
import { io as createClient } from 'socket.io-client'
import { spawnRustNotificationService, spawnRustPeer, cleanChildEnvironment } from './rust-service.mjs'
const DB_PREFIX = 'notification_rust_e2e_'
const TEMP_PREFIX = 'platform-notification-rust-e2e-'
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

export async function createRealPlatformFixture(t, options = {}) {
  const adminUrl = options.databaseUrl ?? process.env.NOTIFICATION_TEST_DATABASE_URL ?? process.env.TEST_DATABASE_URL
  const binaryArgument = options.backendBinary ?? process.env.NOTIFICATION_E2E_BACKEND_BINARY
  assert.ok(adminUrl, 'Explicit isolated PostgreSQL URL is required; this suite never skips')
  assert.ok(binaryArgument, 'Compiled real backend binary is required; this suite never skips')
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
  const safeLogs = () => [...secrets].filter(Boolean).reduce((text, secret) => text.replaceAll(secret, '[REDACTED]'), logs.join('\n'))
    .replace(/\bpostgres(?:ql)?:\/\/[^\s"'<>]+/gi, '[database-url-redacted]')
    .replace(/\bBearer\s+[^\s"']+/gi, 'Bearer [redacted]')
  const sockets = []
  const transitions = []
  let admin
  let writer
  let created = false
  let adminConnected = false
  let backend
  let service
  let proxyPeer
  let readerRole
  let readerRoleCreated = false
  t.after(async () => {
    const errors = []
    for (const item of sockets) item.socket.disconnect()
    try { await service?.stop() } catch { errors.push('owned notification service stop failed') }
    try { await proxyPeer?.stop() } catch { errors.push('owned Rust platform proxy stop failed') }
    try { await stopBackend(backend) } catch { errors.push('owned backend stop failed') }
    const backendStopped = !backend || backend.closed
    if (!backendStopped) errors.push('backend exit was not confirmed; owned database and temporary directory retained')
    try { await writer?.end() } catch { errors.push('owned writer close failed') }
    try {
      if (created && adminConnected && backendStopped) {
        assert.equal(databaseName, expectedDatabase)
        assert.match(databaseName, /^notification_rust_e2e_[0-9a-f]{32}$/)
        assert.ok(databaseName.startsWith(DB_PREFIX))
        await query(admin, `DROP DATABASE "${databaseName}" WITH (FORCE)`)
        created = false
      }
      if (readerRoleCreated && !created) {
        assert.match(readerRole, /^notification_reader_[0-9a-f]{32}$/)
        await query(admin, `DROP ROLE "${readerRole}"`)
        readerRoleCreated = false
      }
    } catch { errors.push('owned database or read role cleanup failed') }
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
    const artifactDirectory = process.env.NOTIFICATION_TEST_ARTIFACTS_DIR
    if (artifactDirectory) {
      assert.ok(path.isAbsolute(artifactDirectory), 'Test diagnostics require an explicit absolute output directory')
      mkdirSync(artifactDirectory, { recursive: true })
      writeFileSync(path.join(artifactDirectory, databaseName + '.log'), safeLogs(), { mode: 0o600 })
    }
    for (const value of secrets) assert.ok(!captured.includes(value), 'backend/service logs must not contain connection URLs, signing secrets, tickets or Bearers')
    assert.equal(errors.length, 0, errors.join('; '))
  })
  admin = databaseClient(adminUrl, 'notification-platform-e2e-admin')
  writer = databaseClient(databaseUrl, 'notification-platform-e2e-writer')
  admin.on('error', () => logs.push('test administration connection unavailable'))
  writer.on('error', () => logs.push('test writer connection unavailable'))
  await db('administration connect', () => admin.connect())
  adminConnected = true
  assert.match(databaseName, /^notification_rust_e2e_[0-9a-f]{32}$/)
  await query(admin, `CREATE DATABASE "${databaseName}" TEMPLATE template0`)
  created = true
  await db('isolated writer connect', () => writer.connect())
  assert.equal((await query(writer, 'SELECT current_database() AS name')).rows[0].name, databaseName)

  const port = await freeIpv6Port()
  const base = `http://[::1]:${port}`
  // main uses dotenv_override(). An empty owned .env prevents ancestor/project
  // credential files from overriding the explicitly passed isolated DB/JWT env.
  writeFileSync(path.join(root, '.env'), '')
  const backendStartedAt = performance.now()
  const child = spawn(binary, [], {
    cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...cleanChildEnvironment(), DATABASE_URL: databaseUrl, JWT_SECRET: secret,
      BIND_ADDR: `[::1]:${port}`, PUBLIC_APP_URL: base, RUST_LOG: 'warn',
      AUTO_CREATE_DATABASE: 'false', STATIC_DIR: '', APPLICATION_ENVIRONMENT: 'development',
      // The real reference catalog COPY exceeds the request-oriented 5s default
      // on cold CI disks. This fixture is not the Phase32 API latency benchmark.
      DB_STATEMENT_TIMEOUT_MS: '60000',
      MICRO_GATEWAY_ORIGIN: base, MICRO_ALLOWED_ORIGINS: base,
      APP_ENV: 'development', SESSION_TTL_HOURS: '1', ACCESS_TOKEN_TTL_SECONDS: '300',
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
    assert.ok(!backend.error && !backend.closed, 'compiled real backend exited before readiness\n' + safeLogs().slice(-6000))
    try { return (await http('GET', '/api/health', null, undefined, 1000)).status === 200 } catch { return false }
  }, 'real backend migrations, seed and listener must become ready', 90_000)
  const migrationDirectory = fileURLToPath(new URL('../../../../backend/migrations/', import.meta.url))
  const expectedMigrations = readdirSync(migrationDirectory).filter(file => /^[0-9]+_.+\.sql$/.test(file)).sort()
    .map(file => ({ version: Number(file.split('_')[0]), checksum: createHash('sha384').update(readFileSync(path.join(migrationDirectory, file))).digest() }))
  const migrations = await query(writer, 'SELECT version,success,checksum FROM _sqlx_migrations ORDER BY version')
  assert.deepEqual(migrations.rows.map(row => Number(row.version)), expectedMigrations.map(row => row.version), 'The real binary must apply the current source migration set')
  for (const [index, row] of migrations.rows.entries()) {
    assert.equal(row.success, true)
    assert.deepEqual(row.checksum, expectedMigrations[index].checksum, 'The compiled real migration checksum must match its unchanged source file')
  }
  const regionRows = (await query(writer, "SELECT count(*)::int AS count FROM regions WHERE version='2023'")).rows[0].count
  assert.equal(regionRows, 665923, 'The real main must complete its full region seed; tests do not skip initialization')
  t.diagnostic(JSON.stringify({ realMigrationsApplied: migrations.rows.length, realRegionRows: regionRows,
    realBackendStartupObservedMs: Math.round(performance.now() - backendStartedAt), testStartupBudgetMs: 90_000 }))

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
  let platformBase = base
  if (options.platformProxy) {
    proxyPeer = spawnRustPeer({ binary: options.proxyBinary ?? process.env.NOTIFICATION_E2E_PROXY_BINARY,
      cwd: root, logger, env: { PHASE23_UPSTREAM: base, PHASE23_PROXY_PORT: '0', RUST_LOG: 'warn' } })
    const ready = await proxyPeer.ready
    platformBase = `http://127.0.0.1:${ready.port}`
  }
  // The runtime gets notification columns only, never platform identity tables or write access.
  readerRole = `notification_reader_${randomUUID().replaceAll('-', '')}`
  const readerPassword = randomBytes(32).toString('hex')
  secrets.add(readerPassword)
  assert.match(readerRole, /^notification_reader_[0-9a-f]{32}$/)
  assert.match(readerPassword, /^[0-9a-f]{64}$/)
  await query(admin, `CREATE ROLE "${readerRole}" LOGIN PASSWORD '${readerPassword}'`)
  readerRoleCreated = true
  await query(admin, `ALTER ROLE "${readerRole}" SET default_transaction_read_only=on`)
  await query(writer, `GRANT CONNECT ON DATABASE "${databaseName}" TO "${readerRole}"`)
  await query(writer, `GRANT USAGE ON SCHEMA public TO "${readerRole}"`)
  await query(writer, `GRANT SELECT (id,user_id,tenant_id,organization_id,source_kind,application_id,publication_id,type,title,content,link,created_at) ON notifications TO "${readerRole}"`)
  const readerUrl = new URL(databaseUrl)
  readerUrl.username = readerRole
  readerUrl.password = readerPassword
  const serviceDatabaseUrl = readerUrl.toString()
  secrets.add(serviceDatabaseUrl)
  const config = { socketPath: '/socket.io/', host: '::1', recheckMs: 1000, authTimeoutMs: 2000, authWaitMs: 5000, healthProbeMs: 1000, dbRetryMs: 8000, ...options.config }
  service = spawnRustNotificationService({
    binary: options.notificationBinary ?? process.env.NOTIFICATION_E2E_SERVICE_BINARY,
    cwd: root, logger, transitions, env: {
      DATABASE_URL: serviceDatabaseUrl, PLATFORM_API_BASE_URL: platformBase, NOTIFICATION_ALLOWED_ORIGINS: (options.allowedOrigins ?? [base]).join(','),
      NOTIFICATION_HOST: config.host, NOTIFICATION_PORT: '0', NOTIFICATION_AUTH_RECHECK_MS: String(config.recheckMs),
      NOTIFICATION_AUTH_TIMEOUT_MS: String(config.authTimeoutMs), NOTIFICATION_AUTH_WAIT_MS: String(config.authWaitMs),
      NOTIFICATION_HEALTH_PROBE_MS: String(config.healthProbeMs), NOTIFICATION_DATABASE_RETRY_MS: String(config.dbRetryMs),
      ...options.serviceEnv,
    },
  })
  secrets.add(service.metricsToken)
  await service.start()
  await until(async () => (await service.probeHealth()).status === 'UP', 'Rust notification service must validate its real backend and PG dependencies', 30000)

  async function fromContextResponse(response) {
    assert.ok(typeof response.accessToken === 'string', 'real context must issue a session token')
    secrets.add(response.accessToken)
    if (typeof response.refreshToken === 'string' && response.refreshToken) secrets.add(response.refreshToken)
    const context = await ok('GET', '/api/notifications/socket-context', response.accessToken)
    assert.equal(context.contractVersion, '1.0')
    assert.equal(context.userId, response.user.id)
    assert.equal(context.tenantId, response.tenant.id)
    assert.equal(context.organizationId, response.organization.id)
    return { token: response.accessToken, accessToken: response.accessToken, refreshToken: response.refreshToken, profile: response, context }
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
  async function socket(session, socketOptions = {}) {
    const client = createClient(service.url, { path: config.socketPath, transports: socketOptions.transports ?? ['websocket'], upgrade: socketOptions.upgrade ?? true, extraHeaders: { Origin: socketOptions.origin ?? (options.allowedOrigins?.[0] ?? base) }, reconnection: false, forceNew: true, autoConnect: false, timeout: 2500 })
    const item = { socket: client, notifications: [], authenticated: [], unauthorized: [], errors: [] }
    sockets.push(item)
    for (const [event, key] of [['notification', 'notifications'], ['authenticated', 'authenticated'], ['unauthorized', 'unauthorized'], ['connect_error', 'errors']]) client.on(event, (value) => item[key].push(value))
    client.connect()
    await until(() => client.connected || item.errors.length || item.unauthorized.length, 'Socket.IO must connect or explicitly reject admission')
    if (socketOptions.allowRejected && !client.connected) return item
    assert.ok(client.connected, 'real notification service connection failed')
    if (socketOptions.authenticate === false) return item
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
  const insertSystem = async (identity, title = 'Real persisted SYSTEM fixture') => {
    await query(writer, 'BEGIN')
    try {
      const rows = await query(writer, `INSERT INTO notifications(user_id,tenant_id,organization_id,source_kind,type,title,content,link)
        VALUES($1,$2,$3,'SYSTEM','INFO',$4,'Written in owned PostgreSQL by the test driver','/dashboard')
        RETURNING id,user_id AS "userId",tenant_id AS "tenantId",organization_id AS "organizationId"`,
      [identity.userId, identity.tenantId, identity.organizationId, title])
      const row = rows.rows[0]
      await notify(row)
      await query(writer, 'COMMIT')
      return row
    } catch (error) { await query(writer, 'ROLLBACK'); throw error }
  }
  const proxy = options.platformProxy ? {
    url: platformBase,
    async control(endpoint, body) {
      const response = await fetch(platformBase + endpoint, { method: body === undefined ? 'GET' : 'POST',
        headers: body === undefined ? {} : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(3000), redirect: 'error' })
      assert.ok(response.status >= 200 && response.status < 300, 'The owned Rust proxy control must succeed')
      return response.status === 204 ? undefined : response.json()
    },
  } : undefined
  return { backendUrl: base, notificationUrl: service.url, databaseUrl, serviceDatabaseUrl, readerRole, config,
    readerClient: () => databaseClient(serviceDatabaseUrl, 'notification-test-reader-proof'),
    query: (sql, values = []) => query(writer, sql, values), admin, writer, service, transitions, ids, sockets, secrets,
    login, appToken, socket, ok, http, counts, publicationRows, body, publish, notify, insertSystem, proxy,
    fromContextResponse, databaseName }
}


export const PLATFORM_FIXTURE = Object.freeze({ publisher: PUBLISHER, recipient: RECIPIENT, appA: APP_A, appB: APP_B })
export { until, query }
