import pg from 'pg'

const { Client, Pool } = pg
const CHANNEL = 'new_notification'

export function createPostgresStore(config, logger = console) {
  const pool = new Pool({ connectionString: config.databaseUrl, application_name: 'platform-notification-query', max: 4, connectionTimeoutMillis: 2000, query_timeout: 5000 })
  let listener = null
  let listenerReady = false
  let listenerFailure = () => {}
  let retryTimer = null
  let stopped = false
  let started = false
  let generation = 0
  let failures = 0
  let onHint = () => {}
  let onState = () => {}
  pool.on('error', () => logger.warn('[notification-ws] database pool connection unavailable'))

  function reconnect() {
    if (stopped || retryTimer) return
    const delay = Math.min(30000, config.dbRetryMs * 2 ** Math.min(failures++, 5))
    retryTimer = setTimeout(() => { retryTimer = null; void connect() }, delay)
    retryTimer.unref?.()
  }

  async function connect() {
    if (stopped) return
    const attempt = ++generation
    const client = new Client({ connectionString: config.databaseUrl, application_name: 'platform-notification-listen', connectionTimeoutMillis: 2000, query_timeout: 2000, keepAlive: true, keepAliveInitialDelayMillis: 10000 })
    listener = client
    listenerReady = false
    let lost = false
    const disconnected = () => {
      if (lost || stopped || attempt !== generation) return
      lost = true
      listenerReady = false
      onState(false)
      logger.warn('[notification-ws] PostgreSQL LISTEN disconnected; reconnect scheduled')
      void client.end().catch(() => {})
      reconnect()
    }
    client.on('error', disconnected)
    client.on('end', disconnected)
    listenerFailure = disconnected
    client.on('notification', (message) => {
      if (!stopped && !lost && attempt === generation && message.channel === CHANNEL) {
        void Promise.resolve(onHint(message.payload ?? '')).catch(() => logger.warn('[notification-ws] notification hint handling failed'))
      }
    })
    try {
      await client.connect()
      await client.query(`LISTEN ${CHANNEL}`)
      if (stopped || lost || attempt !== generation) { await client.end().catch(() => {}); return }
      failures = 0
      listenerReady = true
      onState(true)
      logger.info('[notification-ws] PostgreSQL LISTEN ready')
    } catch {
      if (!stopped && attempt === generation) disconnected()
      await client.end().catch(() => {})
    }
  }

  return {
    async start(hintCallback, stateCallback) {
      if (started) return
      started = true
      onHint = hintCallback
      onState = stateCallback
      onState(false)
      await connect()
    },
    async getNotification(hint) {
      const result = await pool.query(`
        SELECT id, user_id AS "userId", tenant_id AS "tenantId",
          organization_id AS "organizationId", source_kind AS "sourceKind",
          application_id AS "applicationId", publication_id AS "publicationId",
          type, title, content, link, created_at AS "createdAt"
        FROM notifications
        WHERE id = $1 AND tenant_id = $2 AND organization_id = $3 AND user_id = $4
          AND source_kind IN ('APP', 'SYSTEM')
      `, [hint.id, hint.tenantId, hint.organizationId, hint.userId])
      const row = result.rows[0]
      if (!row) return null
      return { ...row, createdAt: row.createdAt instanceof Date ? row.createdAt.toISOString() : row.createdAt }
    },
    async ping() {
      // Check the Phase10 schema and privileges too; old-schema SELECT 1 is insufficient.
      let queryError
      try {
        await pool.query(`SELECT id, tenant_id, organization_id, source_kind, application_id, publication_id
          FROM notifications WHERE source_kind IN ('APP', 'SYSTEM') LIMIT 0`)
      } catch (error) { queryError = error }
      // A separate pool can be healthy while its idle LISTEN socket is broken.
      // Ping that exact connection too, and rebuild it on an uncertain result.
      if (listenerReady && listener) {
        const checked = listener
        const failed = listenerFailure
        try { await checked.query('SELECT 1') } catch { failed() }
      }
      if (queryError) throw queryError
    },
    async stop() {
      stopped = true
      listenerReady = false
      generation++
      if (retryTimer) clearTimeout(retryTimer)
      retryTimer = null
      onState(false)
      await listener?.end().catch(() => {})
      await pool.end()
    },
  }
}
