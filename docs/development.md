# 通知服务开发指南

## 环境与依赖

使用 Rust 1.97.0。先启动平台及 PostgreSQL，准备通知专用只读数据库身份和可访问的平台 API。服务不加载平台 .env，也不会自动读取本仓库配置文件；参数通过进程环境注入。

在仓库根按 config.env.example 设置实际值，PowerShell 示例：

```powershell
$env:NOTIFICATION_ENV='development'
$env:DATABASE_URL='postgresql://<通知只读账号>:<URL编码密码>@127.0.0.1:5432/saas_platform'
$env:PLATFORM_API_BASE_URL='http://127.0.0.1:3000'
$env:NOTIFICATION_ALLOWED_ORIGINS='http://127.0.0.1:3000'
$env:NOTIFICATION_HOST='127.0.0.1'
$env:NOTIFICATION_PORT='3003'
$env:METRICS_TOKEN='<独立随机指标凭据>'
cargo run --locked
```

启动进程不能带 JWT_SECRET；服务通过平台验证会话而不是本地验签替代实时授权。HTTPS 使用正常 CA 校验，可用 NOTIFICATION_PLATFORM_CA_FILE 添加私有 CA。

## 检查

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

单元检查不证明实际 PostgreSQL LISTEN、平台会话、逐条授权和浏览器恢复。联调时分别验证 polling、WebSocket、升级、首次认证、重复认证、到期/撤权、断线补拉及健康依赖故障。

默认端口 3003，路径 /socket.io/。宿主、代理与服务端路径必须一致，来源需要精确 scheme/host/port；缺失、null 和通配来源不能作为合法宿主来源。
