# Build from the standalone service directory; never copy a host target/ tree.
ARG RUST_IMAGE=rust:1.97.0-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${RUST_IMAGE} AS build
WORKDIR /app
ARG CARGO_BUILD_JOBS=2
COPY Cargo.toml Cargo.lock ./
# Keep dependency compilation reusable across source-only candidate revisions.
# Delete both stub targets and their Cargo fingerprints before compiling real IO.
RUN mkdir -p src && printf 'fn main() {}\n' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked --bin saas-notification-ws \
    && rm -rf src target/release/saas-notification-ws* target/release/libsaas_notification_ws* \
      target/release/deps/saas_notification_ws-* target/release/deps/libsaas_notification_ws-* \
      target/release/.fingerprint/saas-notification-ws-*
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --bin saas-notification-ws

FROM ${RUNTIME_IMAGE}
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="Rust notification service" \
    org.opencontainers.image.revision="${SOURCE_REVISION}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10003 notification \
    && useradd --system --uid 10003 --gid notification --no-create-home notification
COPY --from=build /app/target/release/saas-notification-ws /usr/local/bin/saas-notification-ws
ENV NOTIFICATION_ENV=production NOTIFICATION_HOST=0.0.0.0 NOTIFICATION_PORT=3003 NOTIFICATION_SOCKET_PATH=/socket.io/ TZ=Asia/Shanghai RUST_LOG=info
USER 10003:10003
EXPOSE 3003
HEALTHCHECK --interval=30s --timeout=4s --start-period=20s --retries=3 \
    CMD curl --fail --silent --max-time 3 "http://127.0.0.1:${NOTIFICATION_PORT}/health/ready" > /dev/null || exit 1
ENTRYPOINT ["saas-notification-ws"]
