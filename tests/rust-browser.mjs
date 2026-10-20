import assert from 'node:assert/strict'
import test from 'node:test'
import { createRealPlatformFixture, PLATFORM_FIXTURE } from './helpers/rust-platform.mjs'
import { reservePhase23BrowserOrigin, runPhase23NotificationBrowser } from '../../../tests/phase23-notification-browser.mjs'

test('two actual host pages consume Rust notifications and the shared real platform context', { timeout: 240_000 }, async t => {
  const reserved = await reservePhase23BrowserOrigin()
  const f = await createRealPlatformFixture(t, { allowedOrigins: [reserved.origin] })
  const orgA = f.ids.organizations['p10-e2e-a-root'].id
  const orgB = f.ids.organizations['p10-e2e-b-root'].id
  const recipient = await f.login(PLATFORM_FIXTURE.recipient, orgA)
  const publisherA = await f.login(PLATFORM_FIXTURE.publisher, orgA)
  const publisherB = await f.login(PLATFORM_FIXTURE.publisher, orgB)
  const tokens = new Map([[orgA, await f.appToken(publisherA, PLATFORM_FIXTURE.appB)],
    [orgB, await f.appToken(publisherB, PLATFORM_FIXTURE.appB)]])
  const ownerA = recipient.context
  const ownerB = { userId: ownerA.userId, tenantId: f.ids.tenants['p10-e2e-b'].id, organizationId: orgB }
  const result = await runPhase23NotificationBrowser({ ...reserved, platformUrl: f.backendUrl,
    notificationUrl: f.notificationUrl, session: recipient, ownerA, ownerB,
    artifactsDir: process.env.NOTIFICATION_BROWSER_ARTIFACTS_DIR,
    async publishToOwner(owner, { title, idempotencyKey }) {
      const body = f.body(PLATFORM_FIXTURE.appB, idempotencyKey, { kind: 'USER', userId: owner.userId, organizationId: owner.organizationId })
      const result = await f.publish(PLATFORM_FIXTURE.appB, tokens.get(owner.organizationId), { ...body, title })
      assert.equal(result.rows.length, 1)
      return result.rows[0]
    },
  })
  assert.equal(result.pages, 2)
  assert.equal(result.checks.length, 6)
})
