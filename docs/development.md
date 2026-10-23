# 通知服务开发指南

本服务为单一 Rust crate，二进制为 `saas-notification-ws`，库公开入口为 `run_from_env()`。`main.rs` 负责启动和退出码，配置、平台请求、数据库监听、调度和连接生命周期在独立模块中实现。

## 环境准备与启动

使用仓库固定的 Rust `1.97.0`。先启动 PostgreSQL 与平台 API，并完成平台通知表和 LISTEN 触发器迁移，再配置通知专用只读账号。权限说明见 [部署指南](deployment.md)。本服务不自动执行数据库迁移。

所有运行参数来自进程环境；程序不自动加载 `.env`、`config.env.example` 或平台配置文件。可用 IDE、进程管理器或 Docker 的 env-file 功能注入；将模板复制为 `.env` 后仍须由启动方式显式加载。

下面为 PowerShell 本地示例，数据库账号密码替换为自己配置的真实值。平台 API 默认开发地址为 3001，宿主 Vite 默认来源为 3000：

```powershell
$env:NOTIFICATION_ENV = 'development'
$env:DATABASE_URL = 'postgresql://notification_ws:REPLACE_PASSWORD@127.0.0.1:5432/saas_platform'
$env:PLATFORM_API_BASE_URL = 'http://127.0.0.1:3001'
$env:NOTIFICATION_ALLOWED_ORIGINS = 'http://127.0.0.1:3000'
$env:NOTIFICATION_HOST = '127.0.0.1'
$env:NOTIFICATION_PORT = '3003'
cargo run --locked --bin saas-notification-ws
```

密码中的 URL 保留字符需要编码。不要从平台终端整体继承包含 `JWT_SECRET` 的环境；程序检测到这个变量即拒绝启动。开发环境允许省略 `METRICS_TOKEN`，此时 `/metrics` 始终返回 401。

平台宿主的 Vite 配置已将 `/socket.io` 代理到 `http://127.0.0.1:3003`，保留 polling、POST 和 WS Upgrade。更改通知端口时，启动宿主前设置其 `NOTIFICATION_DEV_PROXY_TARGET`；更改宿主域名或端口时，同步更新通知服务的来源白名单。`localhost` 和 `127.0.0.1` 是不同来源。

## 配置规则

完整示例见 [config.env.example](../config.env.example)，解析实现见 [src/config.rs](../src/config.rs)。未知环境变量不会作为服务配置读取。

| 变量 | 默认 / 必填 | 约束 |
| --- | --- | --- |
| `DATABASE_URL` | 必填 | 具有 host 的 `postgres:` 或 `postgresql:` URL，使用通知只读账号 |
| `PLATFORM_API_BASE_URL` | 必填 | 精确平台 origin，不带路径、尾部斜线、query 或凭据 |
| `NOTIFICATION_ALLOWED_ORIGINS` | 必填 | 逗号分隔、最多 32 个精确 origin；去重；不接受 `*`、`null`、尾斜线或非规范写法 |
| `NOTIFICATION_ENV` | `development` | `production`、`development`、`test`；缺失时可回退 `NODE_ENV` |
| `NOTIFICATION_HOST` | `0.0.0.0` | IP 绑定地址，不接受主机名 |
| `NOTIFICATION_PORT` | `3003` | 生产 1–65535；开发/测试额外允许 0 用于临时端口 |
| `NOTIFICATION_SOCKET_PATH` | `/socket.io/` | 显式绝对路径，长度 3–128，必须以 `/` 结束，不含空段、`.`、`..` 或 query |
| `METRICS_TOKEN` | 生产必填 | 32–256 个 URL-safe 字符，拒绝占位符和明显低变化值；指标专用 |
| `NOTIFICATION_PLATFORM_CA_FILE` | 可选 | 启动时读取的 PEM CA 文件，非空且最多 1 MiB |

生产平台 origin 和允许来源均要求 HTTPS；开发/测试仅额外允许 `localhost`、`127.0.0.1`、`[::1]` 的 HTTP。白名单比较包含 scheme、host、port；重复或缺失 Origin 的 Socket.IO 请求也被拒绝。

平台 HTTP 客户端保留标准 TLS 主机名和证书链验证；额外 CA 通过文件加入信任根，不禁用验证。客户端不跟随重定向、不使用系统 HTTP 代理，平台校验响应限制为 4 KiB JSON。

所有计时值以毫秒计，整数不能带空格、符号或小数：

| 变量 | 默认 | 可配置范围 |
| --- | --- | --- |
| `NOTIFICATION_AUTH_TIMEOUT_MS` | 2000 | 100–10000；含排队等待的单次平台验证期限 |
| `NOTIFICATION_AUTH_RECHECK_MS` | 30000 | 1000–300000；周期复验基准，连接另有最多约 20% 相位偏移 |
| `NOTIFICATION_AUTH_WAIT_MS` | 10000 | 100–60000；等待首次认证的期限 |
| `NOTIFICATION_HEALTH_PROBE_MS` | 5000 | 1000–60000；依赖健康检查与 LISTEN 心跳 |
| `NOTIFICATION_DATABASE_RETRY_MS` | 1000 | 100–30000；LISTEN 重连基准，指数退避加抖动，最终最多 30 秒 |

内存和并发预算：

| 变量 | 默认 | 可配置范围 |
| --- | --- | --- |
| `NOTIFICATION_MAX_CONNECTIONS` | 256 | 1–4096，约束 Engine / namespace 连接准入 |
| `NOTIFICATION_MAX_PARTITIONS` | 128 | 1–4096，身份分区数量 |
| `NOTIFICATION_PER_PARTITION_QUEUE` | 32 | 1–4096，不大于总队列 |
| `NOTIFICATION_TOTAL_QUEUE` | 1000 | 1–65536，等待提示总量 |
| `NOTIFICATION_MAX_INFLIGHT` | 8 | 1–256，同时处理的分区数 |
| `NOTIFICATION_VALIDATION_CONCURRENCY` | 16 | 1–256，不大于最大连接数 |

分区键为用户/租户/组织，单分区按接收次序处理，跨分区可并行。提示超预算会被丢弃并计入指标，持久通知继续通过 HTTP 可见。Socket.IO 接收 payload 和 WS 消息/帧当前各限 8 KiB，发送缓冲为 32 条。

## 源码导航

| 模块 | 职责 |
| --- | --- |
| `config.rs` | 读取并验证显式环境配置 |
| `platform.rs` | 有界平台会话和逐条通知可见性校验 |
| `protocol.rs` | 身份、认证输入、数据库提示和公开通知 DTO |
| `store.rs` | 只读通知查询、LISTEN、健康与重连 |
| `scheduler.rs` | 身份分区、有界队列、公平调度与关闭 |
| `server.rs` | Origin、Socket.IO、连接绑定、复验、投递与服务编排 |
| `transport.rs` | TCP 所有权与物理关闭，覆盖未完成握手和空闲连接 |
| `metrics.rs` | 固定低基数指标和依赖健康快照 |

Ctrl+C 和 Unix SIGTERM 触发退出：停止准入、关闭队列和连接、等待受管任务结束、关闭数据库资源。`NOTIFICATION_SHUTDOWN_ON_STDIN=1` 仅为测试驱动启用 stdin EOF 退出，普通生产启动不依赖 stdin。

## 检查与联调

在本仓库根执行，命令与 CI 一致：

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

快速检查现有纯库测试可用 `cargo test --locked --lib`，覆盖配置、协议、查询行投影、调度、有界响应和传输销毁。单元测试通过不证明数据库和浏览器真实链路通过。

联调使用平台宿主已有通知连接；它在 `connect` 后发送一次平台会话令牌，收到并校验 `authenticated` 后补拉 HTTP 收件箱、未读数和偏好。应用令牌用于 SDK 发布通知，Socket.IO 连接使用平台会话上下文。

逐项验证：polling、直接 WebSocket、polling 升级；非法/缺失 Origin；首次认证超时、重复认证、错误令牌；用户/租户/组织/session 变化；到期与撤权；每条通知可见性；数据库中断和 LISTEN 重连；平台 API 不可用；队列过载；断网恢复与 HTTP 补拉；退出后不存在遗留连接或后台任务。使用实际授权 API 发布测试通知，避免绕过持久化和授权直接伪造数据库提示作为完整验收。

常见定位顺序为 `/health/ready` 三项状态、私网指标、平台接口状态、数据库只读权限、来源与代理路径。日志只记录公开请求标识和固定错误，不输出令牌、数据库 URL、平台原始响应或身份上下文。
