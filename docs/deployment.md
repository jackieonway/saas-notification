# 通知服务部署指南

## 构建

在本仓库根执行：

```sh
cargo build --release --locked
docker build --build-arg SOURCE_REVISION=<完整提交SHA> -t saas-notification:local .
```

将审阅镜像交付到自己的 registry 并记录不可变摘要。运行镜像使用用户 10003，端口 3003，不需要平台源码、Docker socket 或 JWT_SECRET。

## 数据库和运行配置

通知连接平台数据库的独立只读账号。平台 backend/notification-role.sql 是配置模板：只授目标库/schema 与通知所需列的读取权限，不给业务写入或用户表访问。部署前由数据库管理员核对实际迁移和角色权限，不用平台 writer 连接代替。

生产需要 DATABASE_URL、PLATFORM_API_BASE_URL、NOTIFICATION_ALLOWED_ORIGINS、METRICS_TOKEN，完整预算见 config.env.example。平台地址与宿主来源都必须是精确 HTTPS。指标凭据与平台签名、数据库密码独立。

## 接入平台 Compose

平台已有宿主机 Nginx 时，按 [现有 Nginx 接入](../../rust-saas/backend/docs/existing-nginx.md)使用 compose.host-nginx.yml，并叠加 compose.host-nginx.notification.yml 暴露仅回环可达的通知端口。下面命令适用于平台自带 gateway 的入口。

平台检出中提供 backend/compose.notification.yml 和 compose.notification.env.example。分别创建源码之外的 platform.env、notification.env，填 NOTIFICATION_IMAGE 摘要、NOTIFICATION_DATABASE_URL、NOTIFICATION_METRICS_TOKEN 与准确来源。

在平台 backend 执行：

```sh
docker compose --env-file /srv/saas/private/platform.env --env-file /srv/saas/private/notification.env -f compose.yml -f compose.notification.yml --profile notification config --quiet
docker compose --env-file /srv/saas/private/platform.env --env-file /srv/saas/private/notification.env -f compose.yml -f compose.notification.yml --profile notification up -d
```

先让平台 HTTPS 可用，再启用通知。网关 JSON 显式设置 notification 为 notification-ws:3003；非默认白名单需加入对应服务。使用私有 CA 时按 compose.notification-ca.yml 配置只读 CA 文件，保持证书与主机名校验。

## 验证与升级

只在私网开放 /metrics；/health/ready 必须真实检查数据库 LISTEN、查询及平台探针。实际验证浏览器认证、收件箱补拉、撤权和网络恢复，不能用 live 正常替代就绪。

升级前记录镜像、路径、来源、CA 与身份协议，隔离验证后替换摘要。回退到兼容 Rust 镜像或关闭实时路由不改变数据库消息；不为丢失提示重新发布通知。
