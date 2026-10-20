# Rust notification-ws

本服务以Socket.IO提示已落库通知的新记录；PostgreSQL收件箱是事实来源。提示丢失、过载或断线不重新发布、不改变已读状态或幂等键。独立Cargo crate/lock使用socketioxide0.18.7/engineioxide0.17.7，保持当前4.8.4客户端及notification1.0 DTO。旧Node后台与启动文件已移除，npm仅提供PG/客户端测试驱动。

## 启动与部署

固定Rust1.97.0；在进程环境中显式设置专用只读DATABASE_URL、PLATFORM_API_BASE_URL、NOTIFICATION_ALLOWED_ORIGINS及METRICS_TOKEN。程序不加载平台.env，并拒绝JWT_SECRET。

```sh
cargo run --locked --manifest-path mini-services/notification-ws/Cargo.toml --target-dir target/notification-ws
```

根开发调度支持 `node scripts/dev.mjs --apps host --with-notifications`，等待 `/health/ready`，首次编译可增大 `--timeout-ms`；通知子进程移除JWT_SECRET，并将NOTIFICATION_DATABASE_URL/NOTIFICATION_METRICS_TOKEN映射为专用配置。

生产采用[正式通知Compose](../../deploy/compose.notification.yml)与[环境模板](../../deploy/compose.notification.env.example)，按本目录Dockerfile独立构建。镜像非root10003、只读根文件系统、私网3003，无宿主端口、源码或Docker socket挂载。NOTIFICATION_IMAGE使用固定版本/digest；legacy override已移出通知。入口先有HTTPS，再显式将/socket.io/路由到notification-ws:3003；入口不能依赖通知ready形成探针循环。私有CA使用[只读CA overlay](../../deploy/compose.notification-ca.yml)，保持正常TLS校验。

```sh
docker compose --env-file /secure/platform.env --env-file /secure/notification.env -f docker-compose.yml -f deploy/compose.notification.yml --profile notification config --quiet
```

## 配置

| 名称 | 默认值与边界 |
| --- | --- |
| DATABASE_URL | 必填PostgreSQL URL，专用读取/LISTEN身份 |
| PLATFORM_API_BASE_URL | 必填精确HTTPS origin；开发额外允许回环HTTP |
| NOTIFICATION_ALLOWED_ORIGINS | 必填精确origin，最多32个；缺失/null/*拒绝 |
| NOTIFICATION_ENV | development；production/development/test，兼容NODE_ENV |
| METRICS_TOKEN | production必填，32–256随机URL安全字符，独立精确Bearer |
| NOTIFICATION_PLATFORM_CA_FILE | 可选追加PEM CA，最大1MiB，不关闭验证 |
| NOTIFICATION_HOST / NOTIFICATION_PORT | 0.0.0.0 / 3003；端口0仅开发测试 |
| NOTIFICATION_SOCKET_PATH | /socket.io/，明确非根路径且末尾/；网关/宿主同步 |
| NOTIFICATION_AUTH_TIMEOUT_MS | 2000，100–10000；含HTTP许可等待、连接和body总deadline |
| NOTIFICATION_AUTH_WAIT_MS | 10000，100–60000；首次事件认证等待 |
| NOTIFICATION_AUTH_RECHECK_MS | 30000，1000–300000；另加每连接0–20%复验抖动 |
| NOTIFICATION_HEALTH_PROBE_MS | 5000，1000–60000 |
| NOTIFICATION_DATABASE_RETRY_MS | 1000，100–30000；指数退避/80–120%抖动，最大30s |
| NOTIFICATION_MAX_CONNECTIONS | 256，1–4096；Engine及namespace准入上限C |
| NOTIFICATION_MAX_PARTITIONS | 128，1–4096 |
| NOTIFICATION_PER_PARTITION_QUEUE | 32，1–4096，不大于全局pending上限 |
| NOTIFICATION_TOTAL_QUEUE | 1000，1–65536 |
| NOTIFICATION_MAX_INFLIGHT | 8，1–256；同时处理的提示分区 |
| NOTIFICATION_VALIDATION_CONCURRENCY | 16，1–256且不大于C；所有平台验证共用 |
| NOTIFICATION_SHUTDOWN_ON_STDIN | 默认关闭；仅测试显式1时EOF停止，生产使用信号 |

## 身份与调度

连接后仅接受一次严格JSON对象 `authenticate {token}`；数组、额外字段、非法/超长token拒绝。第一次尝试在future调度前按收包顺序消费，重复事件立即拒绝并退役，迟到结果不能复活连接。平台Bearer真实调用 `GET /api/notifications/socket-context`，绑定user/tenant/org/sessionId；公开authenticated只含contractVersion/userId/tenantId/organizationId/expiresAt。

平台HTTP禁止redirect/环境代理，正常rustls校验，整个交换限4096字节JSON及统一deadline。通知服务不解码JWT签名，也不接受浏览器自报归属。`new_notification`严格四UUID，SQL按id和完整归属只读APP/SYSTEM；实际逐socket投递前带notificationId实时复验。APP特定403/404只跳当前提示；投递网络/5xx/异常协议也停止当前提示。401、身份/session变化或到期断开；初次/周期无法确认则fail closed。已知expiresAt只能缩短，独立watch不被慢HTTP延迟。

按user/tenant/org分区FIFO，同一key一个handler、ready分区公平轮转；慢key不会占住其他key的空闲worker。pending最多total_queue，active最多max_inflight，驻留提示上界为两者之和；过载只丢提示，数据库收件箱保留。平台HTTP许可有界、query池4连接、独立LISTEN池1连接；每连接两个watch，Engine上限C、TCP上限4C+16，Socket.IO buffers32、payload/WS/frame8192字节。退出丢pending并取消/等待active，不写收件箱；Engine关闭与100ms刷新后的全TCP撤销共同await，idle TCP和未完成HTTP请求头也被回收。

拒绝保持 `unauthorized {code:"SESSION_INVALID",message:"实时通知会话已失效"}` 和namespace断开；100ms flush后还会撤销该Engine当前TCP，阻止不合作raw peer忽略41。HTTP keepalive按当前Engine所有权处理，旧连接不能误关复用于其他会话或health的TCP。namespace、物理IO关闭与Engine内部状态回收分别核验；polling旧Engine元数据可能等待库心跳/连接deadline回收，其总量仍受C限制。

周期失效候选窗口由1.2×recheck+authTimeout+flush及实际调度决定；默认参数不是SLA，不承诺成员变化瞬间物理断连。具体测试配置和实测时长见[Phase23证据](../../docs/validation/phase-23.md)。

## 数据库、健康与观测

[notification-role.sql](../../deploy/notification-role.sql)提供离线管理员配置模板：只授目标库CONNECT、schema USAGE及notifications所需12列SELECT，不授users或业务写入；默认只读事务。服务不建表、不迁移、不发布。application_name分别为platform-notification-query与platform-notification-listen，URL参数不能覆盖标记。

`/health/live`仅表示进程可响应；`/health`和`/health/ready`要求database/listen/platform全UP，否则503。实际检查通知schema和同一LISTEN连接，平台无Bearer探针须真实收到401。监听丢失先DOWN、丢旧连接、按有界抖动退避重建；不以pool正常替代LISTEN正常。

`/metrics`仅接受独立Bearer，公开网关继续拒绝。固定series无用户/组织/session/token标签，包含clients、authenticated_clients、pending_hints、partitions、active_partitions、各limit、validation_inflight/high_water、validation耗时、queue_wait sum/max、投递/拒绝/丢弃/重连计数。指标不替代持久收件箱完整性验证。

## 切流与回退

先隔离验证目标镜像和只读角色，核对固定版本/CA/路由/origin，再排空旧Node通知并用其原部署清单精确停止该服务，启用Rust profile同一路径。Compose合并不会自动停止原Node容器；不要用泛化remove-orphans误及其他服务。旧宿主静态资源在升级窗口保留，支持既有1.0 wire和重连再认证；继续实时失权，不放宽sandbox、origin或token类型。

宿主认证/重连后真实HTTP补拉列表、计数、偏好，历史不弹Toast。`[websocket,polling]`本身不保证初始WS失败自动polling回退，HTTP补拉也不是该能力。服务不改schema，回退到保留兼容Rust镜像或关闭实时路由均不改变持久通知；不可为补提示重新发布或绕过授权。外部registry推送、目标生产部署和正式容量承诺另需环境与执行请求。

## 验证入口

```sh
cargo test --locked --manifest-path mini-services/notification-ws/Cargo.toml --all-targets
node scripts/ci/notification.mjs --run-tests --out-dir /absolute/new-report
node scripts/ci/notification.mjs --run-tests --out-dir /absolute/new-browser-report --browser chromium
node scripts/ci/notification-image.mjs --out-dir /absolute/new-image-report --source-revision FULL_GIT_SHA
```

联合driver只接受owned PG；缺配置、零测试、skip或报告缺失都失败。真实平台main/完整迁移seed/实际session/app token、Rust通知与只延迟转发真实平台的Rust proxy共同验证持久发布、撤权、恢复、预算和边界；浏览器使用两个实际宿主页验证共享切组织、旧HTTP隔离和重连补拉。Node仅是PG/API驱动、Vite及浏览器工具。

镜像driver独立证明非root/只读/无Node、默认closed stdin仍存活、依赖DOWN时live200/ready503、私有metrics、SIGTERM和源码对应；它不替代真实平台PG联合。旧Node fixture退役，历史记录保留其原语义。当前实际结果、失败/环境记录、源码摘要和清理见[Phase23证据](../../docs/validation/phase-23.md)。
