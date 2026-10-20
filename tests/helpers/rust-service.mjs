import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { existsSync } from 'node:fs'
import path from 'node:path'

// Keep production credentials and dotenv/Node injection options out of Rust peers.
export function cleanChildEnvironment(source = process.env) {
  return Object.fromEntries(['PATH', 'Path', 'SystemRoot', 'WINDIR', 'COMSPEC', 'PATHEXT',
    'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'SSL_CERT_FILE', 'SSL_CERT_DIR']
    .filter(key => source[key] !== undefined).map(key => [key, source[key]]))
}

export function spawnRustPeer({ binary, cwd, env, logger = { info() {}, warn() {} }, timeout = 45_000 }) {
  assert.ok(typeof binary === 'string' && path.isAbsolute(binary) && existsSync(binary), 'An existing absolute Rust executable is required; never skip or run a Node server')
  const child = spawn(binary, [], { cwd, env: { ...cleanChildEnvironment(), ...env },
    stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true })
  let closed = false, pending = '', readyTimer, stopping, spawnError
  const exited = new Promise(resolve => child.once('close', (code, signal) => { closed = true; resolve({ code, signal }) }))
  const ready = new Promise((resolve, reject) => {
    readyTimer = setTimeout(() => reject(new Error('Rust peer did not report readiness before its startup deadline')), timeout)
    child.on('error', () => { spawnError = true; clearTimeout(readyTimer); reject(new Error('Rust peer executable could not start')) })
    child.once('close', () => { clearTimeout(readyTimer); reject(new Error('Rust peer exited before its readiness event')) })
    child.stdout.on('data', bytes => {
      const text = bytes.toString('utf8')
      logger.info(text)
      pending = (pending + text).slice(-65536)
      while (pending.includes('\n')) {
        const end = pending.indexOf('\n'), line = pending.slice(0, end)
        pending = pending.slice(end + 1)
        try {
          const message = JSON.parse(line)
          if (message.event === 'ready' && Number.isInteger(message.port) && message.port > 0 && message.port < 65536) {
            clearTimeout(readyTimer); resolve(message)
          }
        } catch { /* Tracing lines are diagnostics, not readiness. */ }
      }
    })
  })
  child.stderr.on('data', bytes => logger.warn(bytes.toString('utf8')))
  // Start callers can perform other setup before awaiting readiness.
  ready.catch(() => {})
  return {
    child, ready, exited, get closed() { return closed },
    stop() {
      return stopping ??= (async () => {
        clearTimeout(readyTimer)
        if (spawnError) return
        child.stdin.end()
        const timer = setTimeout(() => child.kill('SIGTERM'), 6000)
        const force = setTimeout(() => child.kill('SIGKILL'), 10_000)
        try {
          const result = await exited
          assert.equal(result.code, 0, 'The owned Rust peer must exit gracefully on stdin EOF')
        } finally { clearTimeout(timer); clearTimeout(force) }
      })()
    },
  }
}

export function spawnRustNotificationService({ binary, cwd, env, logger, transitions = [] }) {
  const metricsToken = randomBytes(32).toString('base64url')
  let peer, address, monitor, probing = false, observed
  const service = {
    metricsToken,
    get url() {
      assert.ok(address, 'The Rust service must start before its address is used')
      const host = env.NOTIFICATION_HOST ?? '127.0.0.1'
      return `http://${host.includes(':') ? `[${host}]` : host}:${address.port}`
    },
    async start() {
      peer = spawnRustPeer({ binary, cwd, logger, env: { NOTIFICATION_ENV: 'development',
        NOTIFICATION_SHUTDOWN_ON_STDIN: '1', RUST_LOG: 'warn', METRICS_TOKEN: metricsToken, ...env } })
      address = await peer.ready
      monitor = setInterval(async () => {
        if (probing) return
        probing = true
        try { await service.probeHealth() } catch { /* The next explicit health check reports the failure. */ }
        finally { probing = false }
      }, 50)
      return address
    },
    async probeHealth() {
      const response = await fetch(service.url + '/health/ready', { signal: AbortSignal.timeout(4000), redirect: 'error' })
      assert.ok([200, 503].includes(response.status), 'Readiness exposes an explicit UP/DOWN result')
      const health = await response.json()
      if (health.checks?.listen !== undefined) {
        const ready = health.checks.listen === 'UP'
        if (ready !== observed) { transitions.push(ready); observed = ready }
      }
      return health
    },
    async metrics() {
      const response = await fetch(service.url + '/metrics', {
        headers: { authorization: `Bearer ${metricsToken}` }, signal: AbortSignal.timeout(3000), redirect: 'error',
      })
      assert.equal(response.status, 200)
      return response.text()
    },
    async stop() {
      clearInterval(monitor)
      await peer?.stop()
    },
  }
  return service
}

export function metricValue(text, name) {
  const values = text.split('\n').filter(line => line === name || line.startsWith(name + ' ') || line.startsWith(name + '{'))
    .map(line => Number(line.trim().split(/\s+/).at(-1)))
  assert.ok(values.length && values.every(Number.isFinite), `Missing or invalid metric ${name}`)
  return values.reduce((sum, value) => sum + value, 0)
}
