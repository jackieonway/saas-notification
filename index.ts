/**
 * notification-ws — Dante Cloud 实时通知 WebSocket mini-service
 *
 * 端口固定 3003。socket.io path 必须是 '/' (Caddy 网关按 XTransformPort=3003 转发依赖它)。
 *
 * 关键实现点:
 * - socket.io path:'/' 时 engine.io 的 check() 会匹配所有以 '/' 开头的请求并全部拦截,
 *   直接注册的 /health 处理器永远不会执行 (实测返回 400 "Transport unknown")。
 *   解法: 先 new Server(httpServer) 让 engine.io attach, 再把它的 request dispatcher
 *   重新包一层 —— /health 在 engine.io 之前短路返回, 其余原样交给 engine.io。
 * - JWT 认证: 客户端连上后 emit "authenticate" {token}, 校验通过 join `user:${userId}` 房间。
 *   接受 typ=access (OAuth 访问令牌) 与 typ=session (前端登录会话令牌); credential 登录票据等其它 typ 拒绝。
 * - PostgreSQL LISTEN 'new_notification' (优先) / 失败时降级 2s 轮询 notifications 表 (fallback)。
 */
import { createServer } from "node:http"
import { readFileSync } from "node:fs"
import type { IncomingMessage, ServerResponse } from "node:http"
import { Server } from "socket.io"
import jwt from "jsonwebtoken"
import postgres from "postgres"

const PORT = 3003
const PG_URL = "postgresql://dante@127.0.0.1:5432/dante_cloud"
const NOTIFY_CHANNEL = "new_notification"

/* ============ JWT_SECRET (从 backend/.env 手工解析, 不引入 dotenv) ============ */

function loadJwtSecret(): string {
  const env = process.env.JWT_SECRET
  if (env && env.trim()) return env.trim()
  const raw = readFileSync("/home/z/my-project/backend/.env", "utf8")
  for (const line of raw.split("\n")) {
    const t = line.trim()
    if (t.startsWith("JWT_SECRET=") && t.length > "JWT_SECRET=".length) {
      return t.slice("JWT_SECRET=".length).trim()
    }
  }
  throw new Error("无法读取 JWT_SECRET (backend/.env)")
}

const JWT_SECRET = loadJwtSecret()

/* ============ HTTP + Socket.IO (path '/') ============ */

const httpServer = createServer()

const io = new Server(httpServer, {
  // DO NOT change the path, it is used by Caddy to forward the request to the correct port
  path: "/",
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  pingTimeout: 60_000,
  pingInterval: 25_000,
})

/**
 * engine.io (path '/') 拦截全部请求 → 重新包装 dispatcher, 让 /health 可达。
 * 实测: 不包装时 curl /health 得到 400 {"code":0,"message":"Transport unknown"}。
 */
const engineListeners = httpServer
  .listeners("request")
  .slice(0) as Array<(req: IncomingMessage, res: ServerResponse) => void>
httpServer.removeAllListeners("request")
httpServer.on("request", (req, res) => {
  const url = req.url ?? ""
  if (url === "/health" || url.startsWith("/health?")) {
    res.writeHead(200, { "Content-Type": "application/json" })
    res.end(JSON.stringify({ status: "UP" }))
    return
  }
  for (const listener of engineListeners) listener.call(httpServer, req, res)
})

/* ============ JWT 认证 ============ */

interface Claims {
  sub: string
  typ: string
  tenant?: string
  org?: string
  cid?: string
  scope?: string
  exp: number
  iat: number
}

/** 允许的令牌类型: access=OAuth 访问令牌, session=前端登录会话令牌 */
const ALLOWED_TYP = new Set(["access", "session"])

function reject(socket: { emit: (ev: string, data: unknown) => void; disconnect: (close?: boolean) => void }, message: string) {
  socket.emit("unauthorized", { message })
  void socket.disconnect(true)
}

io.on("connection", (socket) => {
  let authenticated = false

  socket.on("authenticate", (data: unknown) => {
    if (authenticated) return
    // 兼容 {token} 对象 / "Bearer xxx" 字符串 / 裸 JWT 字符串
    const raw =
      typeof data === "string"
        ? data
        : data && typeof data === "object" && "token" in data
          ? String((data as { token: unknown }).token ?? "")
          : ""
    const token = raw.replace(/^Bearer\s+/i, "").trim()

    if (!token) {
      reject(socket, "缺少令牌")
      return
    }

    jwt.verify(token, JWT_SECRET, { algorithms: ["HS256"] }, (err, decoded) => {
      if (err || !decoded) {
        reject(socket, "令牌无效或已过期")
        return
      }
      const claims = decoded as Claims
      if (!claims.sub || (claims.typ && !ALLOWED_TYP.has(claims.typ))) {
        reject(socket, "令牌类型不被接受")
        return
      }
      authenticated = true
      socket.data.userId = claims.sub
      socket.data.username = claims.sub
      void socket.join(`user:${claims.sub}`)
      socket.emit("authenticated", { userId: claims.sub })
      console.log(`[ws] 已认证 socket=${socket.id} user=${claims.sub}`)
    })
  })

  socket.on("disconnect", (reason) => {
    console.log(`[ws] 断开 socket=${socket.id} user=${String(socket.data.userId ?? "-")} (${reason})`)
  })
})

/* ============ 通知广播 ============ */

interface NotifyPayload {
  userId?: string
  user_id?: string
  id?: string
  [key: string]: unknown
}

function broadcast(payloadRaw: string) {
  let n: NotifyPayload
  try {
    n = JSON.parse(payloadRaw)
  } catch {
    console.warn("[pg] 通知 payload 不是合法 JSON, 已忽略")
    return
  }
  const userId = n.userId ?? n.user_id
  if (!userId || typeof userId !== "string") {
    console.warn("[pg] 通知 payload 缺少 userId, 已忽略")
    return
  }
  io.to(`user:${userId}`).emit("notification", n)
  console.log(`[ws] 通知已推送 → user:${userId} (id=${String(n.id ?? "-")})`)
}

/* ============ PostgreSQL LISTEN / NOTIFY (失败降级轮询) ============ */

const sql = postgres(PG_URL, { max: 1 })
let listening = false

async function startListener() {
  try {
    await sql.listen(NOTIFY_CHANNEL, (payload: string) => broadcast(payload))
    listening = true
    console.log(`[pg] LISTEN ${NOTIFY_CHANNEL} 已建立 (postgres.js)`)
  } catch (e) {
    console.warn(`[pg] LISTEN ${NOTIFY_CHANNEL} 失败, 降级为 2 秒轮询 notifications 表: ${e}`)
    startPolling()
  }
}

/** fallback: 每 2 秒对比 created_at 水位, 捡出新行按 userId 广播 */
function startPolling() {
  let watermark = new Date()
  const seen = new Set<string>()
  setInterval(() => {
    void (async () => {
      try {
        const rows = await sql`
          SELECT id, user_id AS "userId", type, title, content, link, created_at AS "createdAt"
          FROM notifications
          WHERE created_at >= ${watermark}
          ORDER BY created_at ASC
          LIMIT 50
        `
        for (const r of rows) {
          const key = String(r.id)
          if (seen.has(key)) continue
          seen.add(key)
          if (seen.size > 500) seen.delete(seen.values().next().value as string)
          watermark = new Date(r.createdAt as string)
          broadcast(JSON.stringify(r))
        }
      } catch (e) {
        console.warn(`[pg] 轮询查询失败: ${e}`)
      }
    })()
  }, 2_000)
}

process.on("SIGTERM", () => {
  console.log("收到 SIGTERM, 关闭 notification-ws")
  void io.close()
  void sql.end()
  process.exit(0)
})
process.on("SIGINT", () => {
  console.log("收到 SIGINT, 关闭 notification-ws")
  void io.close()
  void sql.end()
  process.exit(0)
})

httpServer.listen(PORT, () => {
  console.log(`notification-ws 已启动: http/ws 0.0.0.0:${PORT} (socket.io path '/', health=/health)`)
  console.log(`JWT_SECRET 已加载, 接受 typ=${[...ALLOWED_TYP].join("/")} 令牌`)
  void startListener()
  // 心跳日志: 每 5 分钟输出一次在线 socket 与监听状态
  setInterval(() => {
    const sockets = io.sockets.sockets
    const users = new Set<string>()
    sockets.forEach((s) => {
      if (typeof s.data.userId === "string") users.add(s.data.userId)
    })
    console.log(
      `[stat] 在线 socket=${sockets.size} 已认证用户=${users.size} pg=${listening ? "LISTEN" : "POLLING(降级)"}`
    )
  }, 300_000).unref?.()
})
