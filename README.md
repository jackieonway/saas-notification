# notification-ws

独立 Socket.IO 服务用于提示持久通知的新记录。数据库收件箱是权威来源；本服务不接受发布命令、不改变已读状态，也不把 `onNotify` Toast 写入数据库。完整通知协议见[通知接入方案](../../docs/architecture.md#notifications)。

## 启动与配置

正式运行使用已验证的标准 Node 24 和纯 JavaScript 依赖。服务不读取 backend `.env`、不持有 `JWT_SECRET`，平台 session 身份完全通过内部 HTTP 校验取得。

```powershell
cd mini-services/notification-ws
npm ci --ignore-scripts
# 将 config.env.example 复制为 Git 忽略的 .env，并替换配置占位。
node --env-file=.env index.ts
```

容器构建上下文为本服务目录：`docker build -t platform-notification-ws mini-services/notification-ws`。部署提供必填环境变量，并让网关保留 `/socket.io/` 路径转发 polling 与 WebSocket；宿主使用相同路径。Dockerfile 使用 Node 24 和非 root 用户；没有依赖项目之外的个人开发机文件。

仓库 Compose 已编排 `notification-ws`，只在容器网络暴露 3003。生产 `PLATFORM_API_BASE_URL` 和浏览器来源必须显式配置精确 HTTPS origin；开发模式仅允许 loopback HTTP，容器 DNS 的 HTTP 不属于此例外。`NOTIFICATION_DATABASE_URL` 可覆盖独立读取账号；通知容器不接收 `JWT_SECRET`。生产设置 `NODE_ENV=production`、独立 `METRICS_TOKEN` 和实际浏览器 origin；测试 CA 通过 `NODE_EXTRA_CA_CERTS` 加载，禁止关闭 TLS 验证。

| 变量 | 默认或范围 | 用途 |
| --- | --- | --- |
| `DATABASE_URL` | 必填 PostgreSQL URL | 查询通知与独立 LISTEN 连接 |
| `PLATFORM_API_BASE_URL` | 必填精确 HTTPS origin | 无路径、query 或凭据；开发额外允许 loopback HTTP |
| `NODE_ENV` / `METRICS_TOKEN` | 生产必须 `production` 与独立随机 token | 私有 `/metrics` 的精确 Bearer；至少 32 字符，不能是占位值 |
| `NOTIFICATION_ALLOWED_ORIGINS` | 必填，逗号分隔精确 origin | 同时约束 polling、WebSocket 握手和升级；不支持 `*` 或 `null` |
| `NOTIFICATION_HOST` / `NOTIFICATION_PORT` | `0.0.0.0` / `3003` | HTTP/Socket.IO 监听 |
| `NOTIFICATION_SOCKET_PATH` | `/socket.io/` | 明确专用路径，以 `/` 结尾；不允许根路径 |
| `NOTIFICATION_AUTH_TIMEOUT_MS` | `2000`，100–10000 | 单次平台请求及响应体读取超时 |
| `NOTIFICATION_AUTH_RECHECK_MS` | `30000`，1000–300000 | 空闲认证连接重新验证 |
| `NOTIFICATION_AUTH_WAIT_MS` | `10000`，100–60000 | 未认证连接等待上限 |
| `NOTIFICATION_HEALTH_PROBE_MS` | `5000`，1000–60000 | 依赖探针间隔 |
| `NOTIFICATION_DATABASE_RETRY_MS` | `1000`，100–30000 | LISTEN 重连初始间隔，退避最多 30 秒 |

工程时间参数不构成消息投递 SLA。来源必须显式包含浏览器实际 origin（包括端口），例如开发站点的 `http://localhost:3000`。没有 Origin 的 Socket.IO 请求也拒绝；健康探针不要求浏览器 Origin。

数据库账号只需连接目标库及读取新通知表，不需要平台 JWT 密钥或通知写入权限。通知表须已完成通知归属与发布迁移；就绪探针会查询新归属字段，旧表不能被误报为已就绪。

## 认证与投递

1. 客户端连接 `/socket.io/` 后仅能初次发送 `authenticate`，严格数据为 `{ token: string }`。
2. 服务捕获该 Bearer，调用 `GET /api/notifications/socket-context`。平台完整验证 `typ=session`、数据库 session、当前成员关系和租户/组织有效状态。服务不解码 JWT，也不相信客户端传入的 user/tenant/org。
3. 成功后加入 `tenant:<tenantId>:org:<organizationId>:user:<userId>` 房间，并发送 `authenticated`：`contractVersion/userId/tenantId/organizationId/expiresAt`，不含 token 或 sessionId。
4. `new_notification` 仅接受 `{id,tenantId,organizationId,userId}` 提示。按四个字段查数据库，APP/SYSTEM 完整归属行才可发送；LEGACY、未知类型及伪造/不一致载荷不广播。
5. 每次投递到某 socket 前，再调用 `socket-context?notificationId=<已核对通知ID>`。平台验证归属；APP 行还要求当前应用在该租户及组织可访问。返回身份与最初的 user/tenant/org/sessionId 必须一致。
6. 通知特定 403/404 只跳过该通知，保留其它合法应用通知；401、到期或身份/sessionId 变化发送 `unauthorized` 后断开。网络、5xx、异常协议或响应超过 4 KiB 不发送当前事件；初次或定时校验不能确认时断开。
7. 初次设置到期 timer，后续平台验证只能缩短已知期限；定时再验清理没有新事件的失效会话。再次 authenticate（包括试图换身份）拒绝并断开。

内部请求固定指向配置的平台 origin，不跟随 redirect，不记录连接串、Bearer、响应原文或签名密钥。通知事件只有公开协议字段：`contractVersion/id/userId/tenantId/organizationId/applicationId/publicationId/type/title/content/link/createdAt`。

## 健康与断线恢复

`GET /health/live` 返回进程存活。`GET /health` 同时检查数据库新 schema 可查询、LISTEN 已建立和平台保护路由可达，全部 UP 才返回 200，否则 503；JSON 的 `checks.database/listen/platform` 分别显示 UP、DOWN 或 UNKNOWN。未带 Bearer 的平台探针必须收到 401，这只证明保护路由存在。

`GET /metrics` 只接受独立监控 Bearer。生产网关拒绝暴露该路由，私有 runner 从同机 loopback 抓取。请求日志为 JSON、路由与指标 labels 使用静态集合，不包含查询、用户／会话 ID、token 或通知内容；合法 UUID `X-Request-Id` 会返回并传递给内部会话验证。完整指标和本地 DOWN／RECOVERED 告警配置见[可观测性文档](../../docs/architecture.md#observability)。

LISTEN 用独立 PostgreSQL Client。断开立即将 listen 标为 DOWN，关闭旧连接并按有上限的退避新建连接，重新成功 LISTEN 后恢复 UP。查询使用独立连接池；就绪探针还会 ping 该条 LISTEN 连接，避免连接池正常却监听链路失效。`pg_stat_activity.application_name` 分别是 `platform-notification-listen` 与 `platform-notification-query`。终止信号会关闭 Socket.IO、timer、HTTP 和数据库连接。

服务不通过不稳定的 `created_at` 水位轮询声称事件重放。断线期间数据库通知仍由平台持久保存，宿主认证或重连成功后补拉数据库列表与未读数，并保留收件数轮询；历史补拉不批量弹 Toast。本地提示队列最多 1000 项，过载提示可丢弃，不能因此重新持久发布或变更幂等键。

## 网关与开发代理

生产 [Caddy](../../deploy/Caddyfile.production) 用 `handle /socket.io/*` 保留完整路径反代到 `notification-ws:3003`，自动转发 WebSocket Upgrade。不要使用 `handle_path` 剥掉前缀。微应用公开版本目录的通用静态 CORS 规则不会作用于该路径。

[Nginx](../../deploy/nginx.conf) 将 `/socket.io/` 原样代理到 `127.0.0.1:3003`，显式使用 HTTP/1.1、Upgrade/Connection、75 秒读取超时和关闭 buffering；Origin 原样交给服务校验。API/OAuth 继续由平台后端处理。部署替换对应上游地址和 TLS 配置。

[Vite](../../frontend/vite.config.ts) 仅代理 `/socket.io`，默认目标 `http://127.0.0.1:3003`，可用 `NOTIFICATION_DEV_PROXY_TARGET` 指向开发服务；去掉旧根路径 bypass 与 `XTransformPort`。这不会代理宿主路由或 HMR。默认路径在服务、宿主和网关一致；若自行改路径，三处必须同步。

## 验证范围

```powershell
cd mini-services/notification-ws
npm test
```

`tests/service.mjs` 使用实际 Socket.IO 客户端、HTTP 平台 fixture 和数据库接口 fixture。它验证实时服务的认证/再验、房间、过期、Origin、HTTP 限制、异步竞争和依赖健康；fixture 不是实际平台权限实现或真实持久 PostgreSQL。

`tests/postgres.mjs` 通过 `NOTIFICATION_TEST_DATABASE_URL` 或 `TEST_DATABASE_URL` 显式启用；没有这些变量则准确跳过。账号需有创建独立测试库权限。测试只创建随机 `notification_ws_test_<UUID>` 数据库，在其中建立最小 SQL fixture，验证真实事务行、NOTIFY、查询归属与 LISTEN 断开/恢复；只终止该库标记 `platform-notification-listen` 的唯一连接，完成后删除自有测试库。平台认证仍是 HTTP fixture，这不代替实际迁移、发布授权和幂等 API 验收。

如使用仓库根目录 Git 忽略的 `.env.test`，从本服务目录执行：

```powershell
node --env-file=../../.env.test --test tests/postgres.mjs
```

`tests/platform-e2e.mjs` 使用编译好的真实 backend main、完整实际迁移/种子、真实 PostgreSQL 和本 Socket.IO 服务。显式设置 `NOTIFICATION_E2E_BACKEND_BINARY` 为当前平台 backend main 的绝对路径，并提供上述测试数据库 URL 后运行：

```powershell
$env:NOTIFICATION_E2E_BACKEND_BINARY = (Resolve-Path ../../backend/target/debug/saas-platform-backend.exe).Path
node --env-file=../../.env.test --test tests/platform-e2e.mjs
```

Linux 设置相同变量为 Linux backend 二进制路径，使用 Node 24 执行。缺少任一配置准确跳过。测试启动 main 执行包含 0022 的全部迁移，在自有空 `.env` 的临时 cwd 传入隔离配置；通过真实 login/context/session-token API 取得两名非 super 用户的 session 和应用 token，不签发假 JWT。覆盖持久发布和幂等、权限及数据授权、四个 user/org/tenant 组合的实时隔离、APP 当前访问权变化、真实 inbox/read/count、WS/LISTEN 离线持久化与恢复、context switch、logout 和 membership 撤销。清理只处理本次创建的数据库、LISTEN 连接与 backend 子进程，公共事件和内存日志检查凭据泄漏。详细步骤见[部署排查](../../docs/architecture.md#notifications)。

测试不读取生产凭据或输出数据库连接串。服务协议 fixture、真实数据库、平台联合和网关验证分别报告；结果以实际运行输出为准，缺少配置时跳过不等于通过。容器构建与启动需在目标环境验证。历史验收统一见[架构文档](../../docs/architecture.md#validation)。
