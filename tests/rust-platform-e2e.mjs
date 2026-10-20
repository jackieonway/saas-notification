import assert from 'node:assert/strict'
import test from 'node:test'
import { createRealPlatformFixture, PLATFORM_FIXTURE, query, until } from './helpers/rust-platform.mjs'
const { publisher: PUBLISHER, recipient: RECIPIENT, appA: APP_A, appB: APP_B } = PLATFORM_FIXTURE
const INVALID_SESSION = { code: 'SESSION_INVALID', message: '实时通知会话已失效' }

test('Rust notifications + actual platform + PostgreSQL publication and revocation pipeline', { timeout: 180_000 }, async (t) => {
  const f = await createRealPlatformFixture(t)
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
  const invalidated = async (client, started = performance.now(), observation = 'after-mutation') => {
    await until(() => client.unauthorized.length && !client.socket.connected, 'real session invalidation must reject and disconnect its socket')
    assert.deepEqual(client.unauthorized, [INVALID_SESSION])
    const elapsed = performance.now() - started
    assert.ok(elapsed < 7000, 'Configured 1s recheck plus bounded verification must close the invalid session within the measured 7s test window')
    t.diagnostic(JSON.stringify({ observation, invalidationObservedMs: Math.round(elapsed), configuredRecheckMs: f.config.recheckMs,
      configuredPlatformDeadlineMs: f.config.authTimeoutMs, testedUpperBoundMs: 7000 }))
  }

  await t.test('Rust liveness/readiness and private metrics use a column-restricted read-only database role', async () => {
    for (const endpoint of ['/health', '/health/live', '/health/ready']) {
      const response = await fetch(f.notificationUrl + endpoint, { signal: AbortSignal.timeout(5000) })
      assert.equal(response.status, 200)
    }
    assert.deepEqual((await f.service.probeHealth()).checks, { database: 'UP', listen: 'UP', platform: 'UP' })
    const deniedMetrics = await fetch(f.notificationUrl + '/metrics', { signal: AbortSignal.timeout(3000) })
    assert.ok([401, 403].includes(deniedMetrics.status))
    const metrics = await f.service.metrics()
    assert.match(metrics, /saas_notification_/)
    for (const secret of f.secrets) assert.ok(!metrics.includes(secret))
    const reader = f.readerClient()
    try {
      await reader.connect()
      assert.equal((await reader.query('SHOW default_transaction_read_only')).rows[0].default_transaction_read_only, 'on')
      assert.equal((await reader.query('SELECT id,user_id,tenant_id,organization_id FROM notifications LIMIT 0')).rowCount, 0)
      // Prove column/table ACL independently of the extra read-only session default.
      await reader.query('SET default_transaction_read_only=off')
      for (const sql of ['SELECT id FROM users LIMIT 1', 'UPDATE notifications SET title=title WHERE false', 'DELETE FROM notifications WHERE false']) {
        await assert.rejects(reader.query(sql), error => error.code === '42501')
      }
    } finally { await reader.end() }
  })

  for (const mode of [
    { label: 'forced polling', transports: ['polling'], upgrade: false, transport: 'polling' },
    { label: 'polling Upgrade', transports: ['polling', 'websocket'], transport: 'websocket' },
  ]) {
    await t.test(`${mode.label} authenticates a real platform session and receives its committed notification`, async () => {
      const client = await f.socket(sessionA, mode)
      try {
        await until(() => client.socket.io.engine.transport.name === mode.transport, 'Expected real Engine.IO transport must become active')
        const row = await f.insertSystem(sessionA.context, `Phase23 ${mode.label}`)
        await waitEvent(client, row.id)
        assert.equal(client.notifications.find(notification => notification.id === row.id).organizationId, orgA)
      } finally { client.socket.disconnect() }
    })
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
    const beforeTenantBroadcast = await f.counts()
    const tenantBroadcast = await f.http('POST', `/api/apps/${APP_A}/notifications`, tokenA,
      f.body(APP_A, 'tenant-direct-members', { kind: 'TENANT' }))
    assert.equal(tenantBroadcast.status, 403, 'a non-platform root TENANT grant is bounded to its organization subtree')
    assert.deepEqual(await f.counts(), beforeTenantBroadcast, 'the denied tenant broadcast must not persist any partial publication')
    const descendant = await f.publish(APP_A, tokenA, f.body(APP_A, 'descendant-direct-members', { kind: 'ORGANIZATION', organizationId: childA }))
    assert.equal(descendant.publication.recipientCount, 2)
    assert.equal(descendant.publication.deliveryCount, 2)
    assert.ok(descendant.rows.every((row) => row.tenantId === tenantA && row.organizationId === childA))
    const childRow = descendant.rows.find((row) => row.userId === publisherId && row.organizationId === childA)
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
    const logoutStarted = performance.now()
    assert.deepEqual(await f.ok('POST', '/api/auth/logout', next.token), { ok: true })
    // No notification hint is sent: this specifically proves the periodic path.
    await invalidated(newRemote, logoutStarted, 'before-real-logout-without-hint')
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

  for (const transport of ['websocket', 'polling']) {
    await t.test(`${transport}: actual database session expiry bounds the acknowledged lifetime and rejects later delivery`, async () => {
      const session = await f.login(RECIPIENT, childA)
      await query(f.writer, "UPDATE platform_sessions SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1", [session.context.sessionId])
      const client = await f.socket(session, { transports: [transport] })
      const expiry = Date.parse(client.authenticated[0].expiresAt)
      const acknowledgedLifetimeMs = expiry - Date.now()
      assert.ok(expiry > Date.now() && expiry - Date.now() <= 2200)
      const live = await f.insertSystem(session.context, `Phase23 ${transport} before expiry`)
      await waitEvent(client, live.id)
      await invalidated(client)
      assert.ok(Date.now() >= expiry - 40)
      t.diagnostic(JSON.stringify({ transport, configuredDatabaseLifetimeMs: 2000, acknowledgedLifetimeMs,
        expiryObservedLateMs: Date.now() - expiry, configuredRecheckMs: f.config.recheckMs,
        configuredPlatformDeadlineMs: f.config.authTimeoutMs }))
      const expired = await f.insertSystem(session.context, `Phase23 ${transport} after expiry`)
      assert.equal(client.notifications.some(row => row.id === expired.id), false)
      assert.equal((await f.http('GET', '/api/notifications/socket-context', session.token)).status, 401)
    })
  }

  await t.test('real administrator kick rejects the next delivery and periodic revalidation closes the kicked user only', async () => {
    const target = await f.login(RECIPIENT, childA)
    const client = await f.socket(target)
    const platformOrganization = (await query(f.writer, `SELECT m.organization_id FROM memberships m
      JOIN users u ON u.id=m.user_id JOIN organizations o ON o.id=m.organization_id JOIN tenants t ON t.id=o.tenant_id
      WHERE u.username='admin' AND t.is_platform ORDER BY o.created_at LIMIT 1`)).rows[0]?.organization_id
    assert.ok(platformOrganization)
    const administrator = await f.login('admin', platformOrganization)
    const kicked = await f.ok('POST', '/api/oauth/tokens/kick', administrator.token, { userId: recipientId })
    assert.ok(kicked.platformSessionsRevoked >= 1)
    const before = client.notifications.length
    await f.insertSystem(target.context, 'Phase23 late delivery after administrator kick')
    await invalidated(client)
    assert.equal(client.notifications.length, before)
    assert.ok(main.socket.connected, 'Administrator kick must preserve the unrelated publisher session')
    assert.equal((await f.http('GET', '/api/notifications/socket-context', target.token)).status, 401)
  })

  for (const item of f.sockets) {
    const serialized = JSON.stringify({ authenticated: item.authenticated, unauthorized: item.unauthorized, notifications: item.notifications })
    for (const secret of f.secrets) assert.ok(!serialized.includes(secret), 'Socket.IO public events must not disclose credentials or signing secrets')
  }
})
