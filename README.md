# 平台实时通知服务

`saas-notification` 是独立部署的 Rust Socket.IO 服务，向平台宿主提示收件箱中新产生的持久通知。平台 HTTP API 负责发布、收件箱查询、已读和删除，PostgreSQL 中的通知记录是事实来源；本服务读取通知并验证当前会话后发送实时事件。

qiankun 负责前端宿主与微应用的生命周期。本仓库没有前端入口，也不实现微应用 loader、iframe 或 SDK bridge；采用 qiankun 后，通知仍通过独立网络服务工作。微应用使用 [SDK 3.0.0](../saas-micro-app-sdk/README.md)向平台发布通知，宿主建立 Socket.IO 连接接收提示。

## 阅读入口

| 任务 | 文档 / 文件 |
| --- | --- |
| 本地启动、环境选项、检查和联调 | [开发指南](docs/development.md) |
| 镜像、数据库权限、网关和生产运维 | [部署指南](docs/deployment.md) |
| 环境变量示例 | [config.env.example](config.env.example) |
| 当前 Rust 与依赖版本 | [rust-toolchain.toml](rust-toolchain.toml)、[Cargo.toml](Cargo.toml)、[Cargo.lock](Cargo.lock) |
| CI 真实检查命令 | [.github/workflows/ci.yml](.github/workflows/ci.yml) |

## 数据与认证流程

1. 平台把 APP 或 SYSTEM 通知持久化到收件箱，提交数据库提示。
2. 服务 `LISTEN new_notification`，仅接受版本为 `1`、精确包含八个字段的私有提示；按账号、租户、组织、具体收件成员、成员类型及身份版本分区进入有界队列。
3. 浏览器连接默认 `/socket.io/`、namespace `/`，在认证等待期内发送一次 `authenticate`，内容严格为 `{ token }`。
4. 服务使用该平台会话令牌调用 `GET /api/internal/notifications/context-proof`，消费平台返回的严格十一字段私有证明。证明绑定用户、租户、组织、session、具体成员、成员类型、身份版本、账号版本和期限；浏览器不能提交这些身份字段。
5. 首次成功后发送 `authenticated`，公开数据仍为 Notification 1 的五个字段：`contractVersion`、`userId`、`tenantId`、`organizationId`、`expiresAt`。
6. 投递每条通知前读取持久行，核对 `recipient_membership_id`、`recipient_member_type`、`recipient_identity_version`，并携带 `notificationId` 向内部证明接口再次检查可见性。只有收件成员、身份代际和当前连接证明全部一致且未过期的连接收到 `notification` 事件。

服务周期性复验会话，不能在同一连接上改绑用户、租户、组织、session、成员、类型或身份版本；账号版本改变也使原连接失效，首次绑定的期限不会被延长。重复认证、非法认证、到期、会话失效、证明改绑，以及首次或周期校验失败都会使连接断开；连接被拒绝时可发送 `unauthorized`。逐条投递遇到权限拒绝、权威不可用或非法证明时拒绝本条通知，不使用缓存授权；连接仍受后续周期复验约束。客户端需在取得新的有效上下文后建立新连接。

内部证明与 LISTEN 提示的完整字段、版本和整数边界见[开发指南](docs/development.md)。它们属于服务端私有协议，不扩展浏览器的 `{ token }` 输入、五字段 `authenticated` 或十二字段 `notification` 公共载荷。`accountVersion` 仅用于连接证明，不进入提示或调度分区。

平台上下文、数据库提示和公开通知中的 UUID 身份只接受标准带连字符的 UUID v7 与 RFC variant。请求只复用单个有效的 v7 `X-Request-Id`；缺失、旧版本、非法或重复 header 均重发新的 v7 请求 ID。LISTEN 重连抖动使用独立 CSPRNG 字节，不从 UUID 时间前缀取随机值。该版本需与 v7 数据重建和平台发布协调切换，不能直接对仍含 v4 身份的源库宣称兼容。

实时数据只包含允许公开的通知字段，不含已读状态、持久化来源标识、session 或私有成员证明。通知行的 `applicationId`、`publicationId` 在 APP 消息中同时存在，在 SYSTEM 消息中同时为空。旧 LEGACY 行和缺少 recipient 证明的 NULL 行仅保留归档，不按当前成员补绑，也不进入此投递路径。旧四字段提示被拒绝。首版客户通知保持关闭，默认广播仅面向有效员工；同账号同组织的客户关系不能继承员工通知。

## 恢复与运行边界

服务不签发或刷新令牌、不持有平台 JWT 签名密钥、不写收件箱、不迁移平台数据库，也不建立独立用户库。它使用专用只读数据库账号，并通过平台 HTTP 校验实时授权。

实时提示可能因队列过载、网络故障或重启而丢失；它们没有持久重放保证。浏览器认证或重连成功后应通过平台 HTTP 重新读取列表、未读数及偏好，按通知 ID 去重。`emit` 成功与相关指标只表示传输层接受发送，不表示用户已经收到或阅读，不能通过重新发布业务通知来补偿丢失提示。

## 健康与指标

| 端点 | 含义 |
| --- | --- |
| `GET /health/live` | 进程 HTTP 存活，返回 `UP` |
| `GET /health/ready`、`GET /health` | 数据库查询、LISTEN 和平台探针均健康时 200，否则 503；响应含三个检查状态 |
| `GET /metrics` | Prometheus 文本；必须提供独立的 `Authorization: Bearer <METRICS_TOKEN>`，否则 401 |

服务启动时先建立监听，依赖检查随后更新就绪状态；启动日志中的 `ready` 事件不等于所有依赖就绪。平台探针期待未携带会话令牌的内部 context-proof 请求返回 401；有效会话与通知权限仍需真实联调验证。

## 技术基线

工具链固定为 Rust `1.97.0`；Socket.IO 实现为 `socketioxide 0.18.7`，锁文件固定 `engineioxide 0.17.7`，平台使用 `socket.io-client 4.8.4`。服务对接通知协议 `1.0`，内部 DTO 和边界检查在 `src/protocol.rs`，没有引入平台业务实现依赖。

升级传输依赖、平台会话接口或通知结构时，重新验证 polling、WebSocket、传输升级、认证、撤权、连接销毁与 HTTP 恢复。日常文档变更可先运行格式检查和现有库测试；完整门禁及联调要求见开发指南。
