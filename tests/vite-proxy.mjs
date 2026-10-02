// Real project Vite server and Socket.IO transport fixture. This does not
// exercise real platform authentication, notification authorization or PG.
import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer, request } from 'node:http'
import { createRequire } from 'node:module'
import { existsSync, lstatSync, mkdtempSync, realpathSync, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..')
const frontend = path.join(workspace, 'frontend')
const viteModule = path.join(frontend, 'node_modules/vite/dist/node/index.js')
const localRequire = createRequire(path.join(workspace, 'mini-services/notification-ws/package.json'))
const ORIGIN = 'https://vite-socket-fixture.example.test'
const SOCKET_PATH = '/socket.io/'
const QUERY_MARKER = 'version/1.0.0?标签=传输'
const TARGET_VARIABLE = 'NOTIFICATION_DEV_PROXY_TARGET'
const TEMP_PREFIX = 'dante-notification-vite-proxy-test-'
const tempBase = realpathSync(os.tmpdir())

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
async function until(predicate, message, timeout = 3500) {
  const deadline = Date.now() + timeout
  while (!predicate()) {
    assert.ok(Date.now() < deadline, message)
    await delay(10)
  }
}

function get(base, target, headers = {}) {
  const url = new URL(base)
  return new Promise((resolve, reject) => {
    const outgoing = request({ hostname: url.hostname, port: url.port, path: target, method: 'GET', agent: false, headers }, (response) => {
      const chunks = []
      response.on('data', (chunk) => chunks.push(chunk))
      response.on('error', reject)
      response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks).toString('utf8') }))
    })
    outgoing.on('error', reject)
    outgoing.setTimeout(5000, () => outgoing.destroy(new Error('Vite proxy fixture HTTP request timed out')))
    outgoing.end()
  })
}

test('real Vite config / transport-only fixture: Socket.IO proxy preserves host and HMR routes', {
  skip: existsSync(viteModule) ? false : 'Frontend Vite dependency unavailable; install frontend dependencies to run the real Vite proxy integration',
}, async (t) => {
  const root = realpathSync(mkdtempSync(path.join(tempBase, TEMP_PREFIX)))
  const ownedRoot = root
  const hadTarget = Object.hasOwn(process.env, TARGET_VARIABLE)
  const previousTarget = process.env[TARGET_VARIABLE]
  const clients = new Set()
  const hmrClients = new Set()
  const requests = []
  let vite
  let io
  const upstream = createServer((_, response) => { response.writeHead(404); response.end('TRANSPORT_FIXTURE_ONLY') })
  t.after(async () => {
    const errors = []
    for (const client of clients) client.disconnect()
    for (const client of hmrClients) client.terminate()
    try { await vite?.close() } catch { errors.push('test-owned Vite close failed') }
    try {
      if (io) await new Promise((resolve) => io.close(resolve))
      else if (upstream.listening) {
        upstream.closeAllConnections()
        await new Promise((resolve, reject) => upstream.close((error) => error ? reject(error) : resolve()))
      }
    } catch { errors.push('test-owned Socket.IO fixture close failed') }
    finally {
      if (hadTarget) process.env[TARGET_VARIABLE] = previousTarget
      else delete process.env[TARGET_VARIABLE]
      const absolute = path.resolve(root)
      const relative = path.relative(tempBase, absolute)
      assert.equal(absolute, ownedRoot)
      assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative))
      assert.equal(path.dirname(absolute), tempBase)
      assert.ok(path.basename(absolute).startsWith(TEMP_PREFIX))
      assert.ok(!lstatSync(absolute).isSymbolicLink())
      rmSync(absolute, { recursive: true, force: true })
    }
    assert.equal(errors.length, 0, errors.join('; '))
  })
  const { Server } = localRequire('socket.io')
  const { io: createClient } = localRequire('socket.io-client')
  const WebSocket = localRequire('ws')
  io = new Server(upstream, {
    path: SOCKET_PATH, serveClient: false,
    cors: { origin: [ORIGIN], methods: ['GET', 'POST'], credentials: false },
    allowRequest: (incoming, callback) => callback(null, incoming.headers.origin === ORIGIN),
  })
  const record = (incoming, kind) => requests.push({ kind, method: incoming.method, url: incoming.url, origin: incoming.headers.origin })
  upstream.prependListener('request', (incoming) => record(incoming, 'http'))
  upstream.prependListener('upgrade', (incoming, socket) => {
    record(incoming, 'upgrade')
    if (incoming.headers.origin !== ORIGIN) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n')
      socket.destroy()
    }
  })
  io.on('connection', (socket) => socket.on('vite:echo', (payload, acknowledge) => {
    const packet = { fixture: 'SOCKET_IO_TRANSPORT_ONLY', payload, transport: socket.conn.transport.name }
    if (typeof acknowledge === 'function') acknowledge(packet)
    socket.emit('vite:packet', packet)
  }))
  await new Promise((resolve, reject) => {
    upstream.once('error', reject)
    upstream.listen(0, '127.0.0.1', resolve)
  })
  process.env[TARGET_VARIABLE] = `http://127.0.0.1:${upstream.address().port}`
  const { createServer: createViteServer } = await import(pathToFileURL(viteModule).href)
  vite = await createViteServer({
    root: frontend, configFile: path.join(frontend, 'vite.config.ts'), envFile: false,
    cacheDir: path.join(root, 'vite-cache'), logLevel: 'silent',
    optimizeDeps: { noDiscovery: true, include: [] },
    server: { port: 0, strictPort: false, host: '127.0.0.1', preTransformRequests: false },
  })
  await vite.listen()
  const address = vite.httpServer.address()
  assert.ok(address && address.port > 0, 'real Vite must bind its own ephemeral port')
  const base = `http://127.0.0.1:${address.port}`
  let hmrModule

  await t.test('host HTML and /@vite/client remain real Vite routes', async () => {
    const host = await get(base, '/')
    assert.equal(host.status, 200)
    assert.match(host.headers['content-type'], /^text\/html\b/i)
    assert.ok(host.body.includes('<div id="root"></div>'))
    assert.ok(host.body.includes('/src/main.tsx'))
    assert.ok(host.body.includes('/@vite/client'))
    assert.ok(!host.body.includes('TRANSPORT_FIXTURE_ONLY'))
    const client = await get(base, '/@vite/client')
    assert.equal(client.status, 200)
    assert.match(client.headers['content-type'], /javascript/i)
    assert.ok(client.body.includes('createHotContext'))
    assert.ok(client.body.includes('vite-hmr'))
    assert.ok(!client.body.includes('SOCKET_IO_TRANSPORT_ONLY'))
    hmrModule = client.body
    assert.equal(requests.length, 0, 'host and Vite-client requests must never reach the notification transport fixture')
  })

  await t.test('real Vite HMR WebSocket remains independent of /socket.io proxying', async () => {
    const token = hmrModule.match(/const wsToken = "([^"]+)"/)?.[1]
    assert.ok(token, 'the real Vite client module must expose its HMR token')
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/?token=${encodeURIComponent(token)}`, 'vite-hmr', { headers: { Origin: base } })
    hmrClients.add(socket)
    const packets = []
    const errors = []
    socket.on('message', (bytes) => { try { packets.push(JSON.parse(bytes.toString())) } catch { errors.push('invalid HMR packet') } })
    socket.on('error', () => errors.push('HMR transport failed'))
    try {
      await until(() => packets.length || errors.length, 'the actual HMR connection must reach Vite')
      assert.deepEqual(errors, [])
      assert.ok(packets.some((packet) => packet.type === 'connected'))
      assert.equal(requests.length, 0, 'Vite HMR must not reach the Socket.IO upstream')
    } finally { socket.terminate(); hmrClients.delete(socket) }
  })

  function open(caseName, options = {}) {
    const socket = createClient(base, {
      path: SOCKET_PATH, query: { viteCase: caseName, queryMarker: QUERY_MARKER },
      transports: ['websocket'], extraHeaders: { Origin: ORIGIN },
      reconnection: false, forceNew: true, timeout: 1500, autoConnect: false, ...options,
    })
    clients.add(socket)
    const state = { socket, errors: [], upgrades: [], upgradeErrors: [], packets: [] }
    socket.on('connect_error', (error) => state.errors.push(error))
    socket.on('vite:packet', (packet) => state.packets.push(packet))
    socket.connect()
    socket.io.engine.on('upgrade', (transport) => state.upgrades.push(transport.name))
    socket.io.engine.on('upgradeError', (error) => state.upgradeErrors.push(error))
    return state
  }
  const caseRequests = (caseName) => requests.filter((entry) => new URL(entry.url, base).searchParams.get('viteCase') === caseName)
  async function echo(state, caseName, transport) {
    const payload = { caseName, text: '真实双向传输', nested: { marker: QUERY_MARKER } }
    const ack = await new Promise((resolve, reject) => state.socket.timeout(1500).emit('vite:echo', payload, (error, response) => error ? reject(error) : resolve(response)))
    await until(() => state.packets.length === 1, 'a server-to-client packet must traverse the real Vite proxy')
    const expected = { fixture: 'SOCKET_IO_TRANSPORT_ONLY', payload, transport }
    assert.deepEqual(ack, expected)
    assert.deepEqual(state.packets, [expected])
  }

  for (const [caseName, transports, transport] of [
    ['pure-polling', ['polling'], 'polling'],
    ['polling-upgrade', ['polling', 'websocket'], 'websocket'],
    ['pure-websocket', ['websocket'], 'websocket'],
  ]) await t.test(`transport-only fixture: ${caseName} preserves complete path/query/Origin and duplex packets`, async () => {
    const state = open(caseName, { transports })
    try {
      await until(() => state.socket.connected || state.errors.length, 'Socket.IO must connect through the actual Vite proxy')
      assert.ok(state.socket.connected, state.errors[0]?.message)
      await until(() => state.socket.io.engine.transport.name === transport, 'the expected real transport must be active')
      if (caseName === 'polling-upgrade') assert.deepEqual(state.upgrades, ['websocket'])
      await echo(state, caseName, transport)
      const entries = caseRequests(caseName)
      assert.ok(entries.length > 0)
      for (const entry of entries) {
        const url = new URL(entry.url, base)
        assert.equal(url.pathname, SOCKET_PATH)
        assert.equal(url.searchParams.get('EIO'), '4')
        assert.equal(url.searchParams.get('queryMarker'), QUERY_MARKER)
        assert.equal(entry.origin, ORIGIN)
      }
      if (transports.includes('polling')) {
        assert.ok(entries.some((entry) => entry.kind === 'http' && entry.method === 'POST'), 'actual polling POST packets must reach the upstream')
        assert.ok(entries.some((entry) => new URL(entry.url, base).searchParams.get('sid') === state.socket.io.engine.id))
      }
      if (transport === 'websocket') assert.ok(entries.some((entry) => entry.kind === 'upgrade'))
    } finally { state.socket.disconnect(); clients.delete(state.socket) }
  })

  await t.test('transport-only fixture: upstream rejects a wrong polling Origin', async () => {
    const response = await get(base, `${SOCKET_PATH}?EIO=4&transport=polling`, { Origin: 'https://unapproved-origin.example.test' })
    assert.equal(response.status, 403)
    assert.equal(response.headers['access-control-allow-origin'], undefined)
  })
  await t.test('transport-only fixture: upstream rejects a wrong WebSocket Origin', async () => {
    const state = open('bad-websocket', { extraHeaders: { Origin: 'https://unapproved-origin.example.test' } })
    try {
      await until(() => state.errors.length || state.socket.connected, 'a wrong-origin WebSocket must be rejected')
      assert.ok(!state.socket.connected)
      assert.ok(caseRequests('bad-websocket').some((entry) => entry.origin === 'https://unapproved-origin.example.test'))
    } finally { state.socket.disconnect(); clients.delete(state.socket) }
  })
  await t.test('transport-only fixture: a denied upgrade keeps allowed polling operational', async () => {
    const state = open('bad-upgrade', {
      transports: ['polling', 'websocket'],
      transportOptions: {
        polling: { extraHeaders: { Origin: ORIGIN } },
        websocket: { extraHeaders: { Origin: 'https://unapproved-upgrade.example.test' } },
      },
    })
    try {
      await until(() => state.socket.connected && state.upgradeErrors.length, 'the wrong-origin upgrade must be denied by the upstream')
      assert.deepEqual(state.upgrades, [])
      assert.equal(state.socket.io.engine.transport.name, 'polling')
      await echo(state, 'bad-upgrade', 'polling')
      assert.ok(caseRequests('bad-upgrade').some((entry) => entry.kind === 'upgrade' && entry.origin === 'https://unapproved-upgrade.example.test'))
    } finally { state.socket.disconnect(); clients.delete(state.socket) }
  })
  assert.ok(requests.every((entry) => new URL(entry.url, base).pathname === SOCKET_PATH), 'only Socket.IO paths may reach the transport upstream')
})
