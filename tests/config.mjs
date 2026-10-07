import assert from 'node:assert/strict'
import test from 'node:test'
import { loadConfig } from '../config.mjs'

const base = {
  DATABASE_URL: 'postgresql://fixture@127.0.0.1:5432/fixture',
  PLATFORM_API_BASE_URL: 'http://127.0.0.1:3001',
  NOTIFICATION_ALLOWED_ORIGINS: 'https://host.example.com',
}

test('minimal required configuration starts with the default /socket.io/ contract and no JWT key', () => {
  const config = loadConfig({ ...base, JWT_SECRET: 'IGNORED_FIXTURE_KEY' })
  assert.equal(config.socketPath, '/socket.io/')
  assert.equal(config.port, 3003)
  assert.equal(config.recheckMs, 30000)
  assert.equal(config.authTimeoutMs, 2000)
  assert.equal('JWT_SECRET' in config, false)
  assert.deepEqual(config.allowedOrigins, ['https://host.example.com'])
})

test('required deployment fields cannot be silently inferred from developer files', () => {
  for (const field of ['DATABASE_URL', 'PLATFORM_API_BASE_URL', 'NOTIFICATION_ALLOWED_ORIGINS']) {
    assert.throws(() => loadConfig({ ...base, [field]: '' }), new RegExp(field))
  }
})

test('platform and socket origins reject credentials, paths, wildcard and opaque origins', () => {
  for (const value of ['*', 'null', 'https://host.example.com/', 'https://user:pass@host.example.com', 'https://host.example.com?x=1', 'ftp://host.example.com']) {
    assert.throws(() => loadConfig({ ...base, NOTIFICATION_ALLOWED_ORIGINS: value }))
    assert.throws(() => loadConfig({ ...base, PLATFORM_API_BASE_URL: value }))
  }
})

test('origins preserve exact scheme/port while trimming and deduplicating a configured list', () => {
  const config = loadConfig({ ...base, NOTIFICATION_ALLOWED_ORIGINS: 'http://localhost:3000, https://host.example.com,http://localhost:3000' })
  assert.deepEqual(config.allowedOrigins, ['http://localhost:3000', 'https://host.example.com'])
})

test('a dedicated Socket.IO path accepts its dots but rejects root, traversal and ambiguous separators', () => {
  assert.equal(loadConfig({ ...base, NOTIFICATION_SOCKET_PATH: '/custom/socket.io/' }).socketPath, '/custom/socket.io/')
  for (const value of ['/', '/socket.io', 'socket.io/', '/socket//io/', '/socket/../io/', '/socket/./io/', '/socket.io/?x=1']) {
    assert.throws(() => loadConfig({ ...base, NOTIFICATION_SOCKET_PATH: value }))
  }
})

test('deployment timer and port bounds reject unsafe or invalid values', () => {
  for (const [name, value] of [['NOTIFICATION_PORT', '0'], ['NOTIFICATION_PORT', '65536'], ['NOTIFICATION_AUTH_TIMEOUT_MS', '99'], ['NOTIFICATION_AUTH_TIMEOUT_MS', '10001'], ['NOTIFICATION_AUTH_RECHECK_MS', '999'], ['NOTIFICATION_AUTH_RECHECK_MS', '300001'], ['NOTIFICATION_AUTH_WAIT_MS', 'NaN']]) {
    assert.throws(() => loadConfig({ ...base, [name]: value }), new RegExp(name))
  }
})

test('production notification peers use exact HTTPS and an independent strong metrics token', () => {
  const production = { ...base, NODE_ENV: 'production', PLATFORM_API_BASE_URL: 'https://platform.example.test',
    METRICS_TOKEN: 'FixtureOnlyMetricsToken0123456789AbCd' }
  assert.equal(loadConfig(production).production, true)
  assert.throws(() => loadConfig({ ...production, METRICS_TOKEN: undefined }), /METRICS_TOKEN/)
  for (const field of ['PLATFORM_API_BASE_URL', 'NOTIFICATION_ALLOWED_ORIGINS']) {
    for (const value of ['http://127.0.0.1:3000', 'http://outside.example.test']) assert.throws(() => loadConfig({ ...production, [field]: value }), /HTTPS/)
    assert.throws(() => loadConfig({ ...base, [field]: 'http://outside.example.test' }), /HTTPS/)
  }
  assert.equal(loadConfig(base).metricsToken, null)
})
