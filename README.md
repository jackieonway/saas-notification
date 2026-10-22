# 平台实时通知服务

独立 Rust Socket.IO 服务，向已授权客户端提示平台收件箱中的持久消息。平台数据库是事实来源，本服务不签发平台令牌、不写收件箱、不建立独立用户库。

| 使用目的 | 文档 |
| --- | --- |
| 本地配置、启动和测试 | [开发指南](docs/development.md) |
| 构建镜像、只读账号与网关接入 | [部署指南](docs/deployment.md) |
| 所有运行选项 | [配置模板](config.env.example)及 src 中的配置解析 |

## 服务协议

路径 /socket.io/、namespace /，连接后发送 authenticate {token}。后端调用平台 /api/notifications/socket-context 绑定用户/租户/组织/session，随后发送公开 authenticated；重复认证、失效或身份变化被拒绝并断开。

数据库提示按身份分区排队，投递前复验当前授权。队列和验证并发有界；过载丢提示但不改变持久通知。客户端认证或重连后 HTTP 补拉，不能靠重复发布补偿提示。

/health/live 检查进程，/health/ready 和 /health 要求数据库查询、LISTEN 与平台探针正常。/metrics 使用独立 Bearer，仅在私网采集。Socket.IO 的网络重连不等于失效会话可自动恢复。

当前使用 Rust 1.97.0、socketioxide 0.18.7、engineioxide 0.17.7，与 socket.io-client 4.8.4 及通知 1.0 DTO 配合。升级这些版本后需要重新验证传输、认证、撤权和恢复矩阵。
