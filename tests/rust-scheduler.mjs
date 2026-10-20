import assert from 'node:assert/strict'
import test from 'node:test'
import { createRealPlatformFixture, PLATFORM_FIXTURE, until } from './helpers/rust-platform.mjs'
import { metricValue } from './helpers/rust-service.mjs'

const metric = (text, suffix) => metricValue(text, 'saas_notification_' + suffix)

test('Rust recipient scheduler preserves order and bounds work against real platform/PG through a Rust delay proxy', { timeout: 180_000 }, async t => {
  const f = await createRealPlatformFixture(t, { platformProxy: true,
    config: { recheckMs: 300_000, authTimeoutMs: 5000 },
    serviceEnv: { NOTIFICATION_MAX_INFLIGHT: '2', NOTIFICATION_VALIDATION_CONCURRENCY: '2',
      NOTIFICATION_MAX_PARTITIONS: '3', NOTIFICATION_PER_PARTITION_QUEUE: '2',
      NOTIFICATION_TOTAL_QUEUE: '4', NOTIFICATION_MAX_CONNECTIONS: '16' },
  })
  const orgA = f.ids.organizations['p10-e2e-a-root'].id
  const orgB = f.ids.organizations['p10-e2e-b-root'].id
  const childA = f.ids.organizations['p10-e2e-a-child'].id
  const slow = await f.login(PLATFORM_FIXTURE.publisher, orgA)
  const fast = await f.login(PLATFORM_FIXTURE.recipient, orgA)
  const third = await f.login(PLATFORM_FIXTURE.publisher, orgB)
  const fourth = await f.login(PLATFORM_FIXTURE.recipient, childA)
  const slowClient = await f.socket(slow), fastClient = await f.socket(fast)
  const thirdClient = await f.socket(third), fourthClient = await f.socket(fourth)
  const gate = (label, session) => f.proxy.control('/test/gates', { label, token: session.token, deliveryOnly: true, delayMs: 12_000 })
  const release = label => f.proxy.control('/test/release', { label })
  const inGate = async label => (await f.proxy.control('/test/state')).gates.find(gate => gate.label === label)?.inflight ?? 0
  const waitNotification = (client, id) => until(() => client.notifications.some(row => row.id === id), 'A persisted real notification must reach its exact recipient')

  await t.test('a slow recipient does not block another key, and each recipient remains FIFO', async () => {
    await gate('slow-order', slow)
    const first = await f.insertSystem(slow.context, 'Slow ordered 1')
    await until(async () => (await inGate('slow-order')) === 1, 'The selected token reaches the real Rust proxy gate')
    const second = await f.insertSystem(slow.context, 'Slow ordered 2')
    const third = await f.insertSystem(slow.context, 'Slow ordered 3')
    const queued = await f.service.metrics()
    assert.ok(metric(queued, 'pending_hints') <= 4)
    const began = performance.now()
    const independent = await f.insertSystem(fast.context, 'Fast independent recipient')
    await waitNotification(fastClient, independent.id)
    assert.ok(performance.now() - began < 2500, 'Fast key must complete before the selected slow request deadline')
    assert.equal(slowClient.notifications.some(row => row.id === first.id), false)
    assert.equal(await inGate('slow-order'), 1)
    await release('slow-order')
    await waitNotification(slowClient, third.id)
    const wanted = new Set([first.id, second.id, third.id])
    assert.deepEqual(slowClient.notifications.filter(row => wanted.has(row.id)).map(row => row.id), [first.id, second.id, third.id])
    assert.equal(fastClient.notifications.some(row => wanted.has(row.id)), false)
  })

  await t.test('active keys, queued hints, partitions and actual verification concurrency stay within independent configured limits', async () => {
    await until(async () => metric(await f.service.metrics(), 'active_partitions') === 0, 'Previous real scheduler work has drained')
    const before = await f.service.metrics()
    const droppedBefore = metric(before, 'scheduler_dropped_total')
    await gate('bounded-a', slow)
    await gate('bounded-b', fast)
    await f.insertSystem(slow.context, 'Bounded active A')
    await f.insertSystem(fast.context, 'Bounded active B')
    await until(async () => (await inGate('bounded-a')) === 1 && (await inGate('bounded-b')) === 1, 'Two selected real platform verifications occupy the global budget')
    const queuedRows = []
    for (let index = 0; index < 2; index++) queuedRows.push(await f.insertSystem(slow.context, `Bounded queued A ${index}`))
    for (let index = 0; index < 2; index++) queuedRows.push(await f.insertSystem(third.context, `Bounded queued third ${index}`))
    await until(async () => metric(await f.service.metrics(), 'pending_hints') === 4, 'The configured global queue reaches its exact cap')
    const droppedRows = []
    for (let index = 0; index < 24; index++) droppedRows.push(await f.insertSystem(index % 2 ? slow.context : fourth.context, `Durable overload ${index}`))
    await until(async () => metric(await f.service.metrics(), 'scheduler_dropped_total') > droppedBefore, 'Real LISTEN hints above the admission limits are explicitly counted as dropped')
    const saturated = await f.service.metrics()
    assert.equal(metric(saturated, 'pending_hints'), 4)
    assert.equal(metric(saturated, 'active_partitions'), 2)
    assert.equal(metric(saturated, 'partitions'), 3)
    assert.ok(metric(saturated, 'validation_inflight') <= 2)
    assert.equal(metric(saturated, 'validation_high_water'), 2)
    for (const [name, expected] of [['connection_limit', 16], ['partition_limit', 3], ['per_partition_queue_limit', 2],
      ['total_queue_limit', 4], ['inflight_limit', 2], ['validation_limit', 2]]) assert.equal(metric(saturated, name), expected)
    await release('bounded-a')
    await release('bounded-b')
    await until(async () => metric(await f.service.metrics(), 'active_partitions') === 0 && metric(await f.service.metrics(), 'pending_hints') === 0,
      'Release drains every admitted key without unbounded background work')
    for (const session of [slow, fourth]) {
      const inbox = await f.ok('GET', '/api/notifications?size=200', session.token)
      const expected = droppedRows.filter(row => row.userId === session.context.userId && row.organizationId === session.context.organizationId)
      assert.ok(expected.every(row => inbox.records.some(record => record.id === row.id)), 'HTTP inbox retains every durable publication even when its hint is dropped')
    }
    const admittedThird = queuedRows.filter(row => row.userId === third.context.userId && row.organizationId === third.context.organizationId)
    for (const row of admittedThird) await waitNotification(thirdClient, row.id)
    assert.deepEqual(thirdClient.notifications.filter(row => admittedThird.some(queued => queued.id === row.id)).map(row => row.id), admittedThird.map(row => row.id))
    assert.equal(fourthClient.notifications.filter(row => droppedRows.some(dropped => dropped.id === row.id)).length, 0)
    t.diagnostic(JSON.stringify({ active: metric(saturated, 'active_partitions'), queued: metric(saturated, 'pending_hints'),
      partitions: metric(saturated, 'partitions'), validationHighWater: metric(saturated, 'validation_high_water'),
      dropped: metric(saturated, 'scheduler_dropped_total') - droppedBefore, durableOverloadRows: droppedRows.length }))
  })

  await t.test('the actual HTTP verification deadline releases a slow worker and preserves its durable inbox row', async () => {
    const before = metric(await f.service.metrics(), 'validation_failures_total')
    await gate('deadline', slow)
    const row = await f.insertSystem(slow.context, 'Durable row beyond HTTP verification deadline')
    await until(async () => (await inGate('deadline')) === 1, 'The selected verification is blocked in the real Rust proxy')
    const started = performance.now()
    await until(async () => {
      const metrics = await f.service.metrics()
      return metric(metrics, 'active_partitions') === 0 && metric(metrics, 'validation_failures_total') > before
    }, 'The configured five-second HTTP deadline cancels verification and releases its scheduler worker', 7500)
    const elapsed = performance.now() - started
    assert.ok(elapsed < 6500, 'A slow upstream cannot retain work beyond its configured total verification budget')
    assert.equal(slowClient.notifications.some(notification => notification.id === row.id), false)
    assert.equal(slowClient.socket.connected, true, 'Per-delivery platform unavailability drops this hint while preserving the otherwise valid socket')
    assert.ok((await f.ok('GET', '/api/notifications?size=200', slow.token)).records.some(record => record.id === row.id))
    await release('deadline')
    const recovered = await f.insertSystem(slow.context, 'Fast delivery after bounded timeout')
    await waitNotification(slowClient, recovered.id)
    t.diagnostic(JSON.stringify({ configuredHttpDeadlineMs: 5000, observedWorkerReleaseMs: Math.round(elapsed) }))
  })
})
