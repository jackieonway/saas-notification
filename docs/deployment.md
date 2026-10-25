# 通知服务部署指南

部署单元为 `saas-notification-ws` 二进制或本仓库 Dockerfile 生成的镜像。它通过平台 HTTPS API 校验会话，使用专用只读账号连接平台 PostgreSQL，并在私网监听 3003；公开 `/socket.io/` 由平台网关代理。

## 构建与交付

在通知仓库根执行：

```sh
cargo build --release --locked --bin saas-notification-ws
docker build --build-arg SOURCE_REVISION=<完整提交SHA> -t saas-notification:local .
```

构建阶段使用 Rust `1.97.0`，运行阶段为 Debian bookworm，安装 CA、curl 与时区数据。镜像使用 `10003:10003`，入口为二进制，默认环境为 production，健康检查访问本地 `/health/ready`。`SOURCE_REVISION` 写入镜像标签，交付时另保存源码修订、锁文件和镜像摘要。

镜像发布到团队 registry 后，以 `repository@sha256:<64位摘要>` 固定部署身份。部署过程使用已审阅镜像；当前仓库 CI 只执行 Rust 检查和构建测试镜像，没有自动发布生产镜像。

## 数据库权限与前置条件

先在平台完成通知表和触发器迁移，再由数据库管理员审阅 [notification-role.sql](../../saas-plateform/backend/notification-role.sql) 并在目标数据库执行。脚本建立或检查专用 `notification_ws` 非特权角色、只读事务默认值和通知所需列的 SELECT 权限；密码通过独立秘密管理渠道配置。脚本不会在通知服务启动时自动执行。

查询需要 `notifications` 的通知 ID、接收用户/租户/组织、`recipient_membership_id`、`recipient_member_type`、`recipient_identity_version`、来源、应用/发布 ID、类型、标题、内容、链接与创建时间。本次成员隔离升级须先完成平台通知迁移，再执行更新后的列级 GRANT；仅升级 binary 或沿用旧列权限不能完成部署。LISTEN 使用 PostgreSQL 通道 `new_notification`；PostgreSQL 没有单独的 LISTEN GRANT。

通知账号不需要、也不应获得 `users` 或 `memberships` 的 SELECT；成员和账号的实时权威状态通过平台内部 HTTP 证明获取。账号不得拥有写入、角色成员关系或管理权限，管理员还需审计 PUBLIC 继承权限。不要授予整表 SELECT 或身份表权限来绕过缺少 recipient 列权限的问题。

服务使用最多 4 个查询池连接和独立 LISTEN 连接，连接应用名分别为 `platform-notification-query`、`platform-notification-listen`。连接失败时会重试并保持未就绪。不要用平台写账号替换通知账号来掩盖权限或迁移问题。

旧通知中缺少 recipient 成员、类型或身份版本的 NULL 行只保留归档；本次新库身份功能没有历史通知回填任务。通知服务不会根据当前 `user_id / organization_id` 为旧行补绑，也不签发、刷新令牌或执行平台迁移。首版客户通知保持关闭，缺省广播只展开到有效员工，员工和客户共用账号或组织不能合并收件箱。

## 生产环境

必需值为 `DATABASE_URL`、`PLATFORM_API_BASE_URL`、`NOTIFICATION_ALLOWED_ORIGINS`、`METRICS_TOKEN`。显式设置 `NOTIFICATION_ENV=production`；平台 origin 与允许来源必须为精确 HTTPS，包含实际端口且无路径或尾部斜线。服务环境中出现 `JWT_SECRET` 会导致启动失败。

指标 token 与平台签名密钥、数据库密码独立，使用 32–256 个 URL-safe 随机字符。完整的计时、队列与并发参数见 [开发指南](development.md)和 [配置模板](../config.env.example)。修改预算后依据连接数、队列等待、丢提示与平台验证指标重新测量容量。

服务默认校验系统 CA。私有 CA 通过 `NOTIFICATION_PLATFORM_CA_FILE` 指向只读挂载的 PEM 公共证书文件，保留主机名和证书链验证；镜像内实际 UID 必须可读。平台 API 不跟随重定向，需直接配置最终 HTTPS origin，且容器必须可解析和访问该公开平台地址。

平台 origin 必须提供 `GET /api/internal/notifications/context-proof`。此路径消费平台会话 Bearer，并在逐条投递时接收 `notificationId`；未经认证的健康探针应得到 401。成功响应是 `proofVersion="1"` 的精确十一字段私有证明，不能改用公开 socket-context 响应或代理缓存证明。身份版本为至少 1 的 i64 整数，账号版本为至少 0 的 i64 整数；完整字段见[开发指南](development.md)。代理和网络规则须允许该内部调用，并保留认证头与 query。

## 与平台 Compose 集成

以下路径按工作区的兄弟仓库布局引用平台 `saas-plateform/backend`；独立克隆时使用自己平台检出的对应文件，不需要把平台源码复制进通知镜像。

平台提供：

- [compose.notification.yml](../../saas-plateform/backend/compose.notification.yml)：增加 profile 为 `notification` 的私网服务。
- [compose.notification.env.example](../../saas-plateform/backend/compose.notification.env.example)：部署变量模板。
- [compose.host-nginx.notification.yml](../../saas-plateform/backend/compose.host-nginx.notification.yml)：宿主机 Nginx 使用的回环端口叠加配置。
- [compose.notification-ca.yml](../../saas-plateform/backend/compose.notification-ca.yml)：可选私有 CA 文件挂载。

将实际平台 env 与通知 env 保存在源码之外。通知部署 env 使用 `NOTIFICATION_DATABASE_URL`、`NOTIFICATION_METRICS_TOKEN`、`NOTIFICATION_IMAGE`，Compose 会映射为容器内服务读取的 `DATABASE_URL` 和 `METRICS_TOKEN`；不要直接把模板中的占位值当成已就绪配置。

在平台 `backend` 目录先渲染并审阅配置，再启动：

```sh
docker compose --env-file /srv/saas/private/platform.env --env-file /srv/saas/private/notification.env -f compose.yml -f compose.notification.yml --profile notification config --quiet
docker compose --env-file /srv/saas/private/platform.env --env-file /srv/saas/private/notification.env -f compose.yml -f compose.notification.yml --profile notification up -d
```

需要私有 CA 时，两条命令均叠加 `-f compose.notification-ca.yml`，并在部署环境指定主机上的 `NOTIFICATION_PLATFORM_CA_FILE` 绝对路径。每次渲染与启动使用同一组 env、Compose 文件及 profile。

平台自带网关时，将私有网关 JSON 的 `notification` 设为 `notification-ws:3003`，再按 [Nginx 网关说明](../../saas-plateform/backend/nginx/README.md)重新渲染。若显式配置 `allowedServices`，其中也必须保留 `notification-ws`。先让平台 HTTPS 可用，再等待通知就绪；网关不能依赖通知健康才能启动，否则平台验证会形成启动依赖循环。

已有宿主机 Nginx 时，按 [平台现有 Nginx 接入](../../saas-plateform/backend/docs/existing-nginx.md)使用 `compose.host-nginx.yml`，再叠加通知基础与宿主 Nginx 通知配置。服务默认只发布 `127.0.0.1:13003`，由宿主 Nginx 转发。自定义 `NOTIFICATION_SOCKET_PATH` 时必须同时调整浏览器、代理和服务；平台提供的默认路由为 `/socket.io/`。

代理须保留 Origin、polling GET/POST、Socket.IO 路径与 WS Upgrade，并提供足够长的连接超时。只允许网关到达私网服务；`/metrics` 和健康端点不作为公网业务路由。使用多个实例时，先验证 polling 会话的粘性路由、每实例 LISTEN 与重新连接行为；服务没有共享内存会话或持久提示队列。

## 验收、监控与退出

验收顺序为数据库迁移与列权限、平台 HTTPS 与内部证明接口、通知依赖就绪、浏览器首次认证、真实通知发布与 HTTP 收件箱、实时提示、成员及账号撤权/到期、网络恢复及进程退出。`/health/live` 正常只说明进程可响应；`/health/ready` 还要求 database、listen、platform 全部 UP。

投递验收需区分同账号同组织的员工与客户、同成员的不同身份版本：员工通知不能被客户连接或新代连接读取；局部成员停用不得撤销另一有效身份，全局账号撤销则应作用于全部连接。旧四字段 LISTEN 提示和 recipient 证明缺失的旧行必须拒绝投递。新提示精确包含八字段，以 `hintVersion="1"` 标识；调度分区包含 user、tenant、organization、membership、memberType 和 identityVersion，账号版本仅由连接证明校验。浏览器的 `{ token }`、五字段 `authenticated` 和十二字段 `notification` 继续使用 Notification 1 公共合同，不暴露私有证明。

`tests/identity_isolation.rs` 使用真实通知服务、隔离 PostgreSQL 和受控 HTTP authority；执行条件见[开发指南](development.md)。平台真实成员／通知授权由平台 PostgreSQL 测试验证，不能将这两组证据称为两个真实服务的端到端联调。部署仍须使用实际平台发布 API、实际会话和浏览器完成上述链路，记录真实结果；本文不表示这些门禁或部署验收已经通过。

私网使用独立 Bearer 抓取 `/metrics`，重点观察连接数、验证并发/失败/耗时、LISTEN 重连、队列等待、提示丢弃和发送接受量。丢提示不改变持久通知，恢复依靠客户端 HTTP 补拉，发送指标不能作为用户已读统计。

平台提供的 Compose 对服务启用只读根文件系统、临时目录、能力移除、无新增特权和 20 秒停止宽限。收到 Ctrl+C / SIGTERM 后程序停止调度、关闭 Socket.IO 与 TCP、等待受管任务并关闭数据库池；不要启用测试专用 stdin EOF 退出作为生产生命周期。

## 升级与回退

升级前记录镜像摘要、源码修订、平台通知协议、Socket.IO 客户端版本、路径、来源、预算和 CA 配置。在隔离环境重跑传输、认证、撤权、恢复与依赖故障矩阵，再替换生产摘要。

成员隔离升级按平台 recipient 存储与触发器迁移、`notification-role.sql` 列权限、匹配内部证明和八字段提示的通知镜像顺序交付。提前部署的新服务拒绝旧四字段提示；升级窗口可能缺少实时提示，客户端通过当前授权下的 HTTP 收件箱补拉。发布前验证目标组合，不能通过放宽解析或给旧记录补绑成员来恢复提示。

回退时恢复仍能校验具体成员与身份版本的兼容镜像和配置，或临时关闭公开实时路由并保留平台 HTTP 收件箱。通知服务不拥有数据库迁移和持久记录；回退无需通过重发业务事件补偿实时提示。涉及平台通知结构变更时，先确认新旧平台与通知服务组合的兼容性，不能回退到仅按用户／组织猜测身份的投递行为。
