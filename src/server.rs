use crate::{
    config::Config,
    metrics::{Health, Metrics},
    platform::{Platform, ValidationError},
    protocol::{authentication_token, Context, Hint, Identity},
    scheduler::{self, Limits},
    store::Store,
    transport::{Control, ControlledListener, Group},
};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde_json::json;
use socketioxide::{
    extract::{SocketRef, TryData},
    SocketIo,
};
use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{watch, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

#[derive(Clone)]
struct Bound {
    token: String,
    context: Context,
}

struct Connection {
    socket: SocketRef,
    request_id: Uuid,
    attempted: AtomicBool,
    closed: AtomicBool,
    bound: Mutex<Option<Bound>>,
    expiry: watch::Sender<Option<chrono::DateTime<Utc>>>,
    changed: Notify,
    cancel: CancellationToken,
}

impl Connection {
    fn snapshot(&self) -> Option<Bound> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.bound.lock().ok()?.clone()
    }
}

#[derive(Clone)]
struct Shared {
    config: Arc<Config>,
    platform: Platform,
    store: Store,
    metrics: Arc<Metrics>,
    health: Arc<Health>,
    queue: scheduler::Sender<Identity, Hint>,
    connections: Arc<Mutex<HashMap<String, Arc<Connection>>>>,
    engines: Arc<Mutex<HashMap<String, Weak<EngineSession>>>>,
    cancel: CancellationToken,
    tracker: TaskTracker,
    admission: Arc<Semaphore>,
}

// Engine.IO retains the initial request's extensions. Holding the permit there
// caps pre-namespace and malicious idle transports as well as authenticated peers.
#[derive(Clone)]
struct EngineAdmission(Arc<EngineSession>);

struct EngineSession {
    _permit: OwnedSemaphorePermit,
    group: Group,
}

#[derive(Clone, Copy)]
struct RequestId(Uuid);

impl Shared {
    fn retire(&self, connection: &Arc<Connection>, unauthorized: bool) {
        if connection.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        connection.cancel.cancel();
        if let Ok(mut bound) = connection.bound.lock() {
            if bound.take().is_some() {
                self.metrics.authenticated.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if let Ok(mut connections) = self.connections.lock() {
            if connections
                .remove(&connection.socket.id.to_string())
                .is_some()
            {
                self.metrics.connections.fetch_sub(1, Ordering::Relaxed);
            }
        }
        if unauthorized {
            self.metrics.auth_denials.fetch_add(1, Ordering::Relaxed);
            if connection.socket.connected() {
                let _ = connection.socket.emit(
                    "unauthorized",
                    &json!({"code":"SESSION_INVALID", "message":"实时通知会话已失效"}),
                );
                let _ = connection.socket.clone().disconnect();
            }
            if let Some(engine) = connection
                .socket
                .req_parts()
                .extensions
                .get::<EngineAdmission>()
                .cloned()
            {
                // Flush unauthorized/41 before enforcing a physical close. Holding
                // admission until this ends bounds these delayed close tasks too.
                self.tracker.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    engine.0.group.close();
                });
            }
        }
    }

    fn bind(&self, connection: &Arc<Connection>, token: String, mut context: Context) -> bool {
        if connection.closed.load(Ordering::Acquire) || connection.cancel.is_cancelled() {
            return false;
        }
        let Ok(mut bound) = connection.bound.lock() else {
            return false;
        };
        if connection.closed.load(Ordering::Acquire) {
            return false;
        }
        if let Some(previous) = bound.as_ref() {
            if previous.context.identity != context.identity
                || previous.context.session_id != context.session_id
            {
                return false;
            }
            context.expires_at = context.expires_at.min(previous.context.expires_at);
        }
        if context.expires_at <= Utc::now() {
            return false;
        }
        let initial = bound.is_none();
        *bound = Some(Bound {
            token,
            context: context.clone(),
        });
        let _ = connection.expiry.send(Some(context.expires_at));
        if initial {
            self.metrics.authenticated.fetch_add(1, Ordering::Relaxed);
            if connection
                .socket
                .emit("authenticated", &context.public())
                .is_err()
            {
                return false;
            }
        }
        connection.changed.notify_one();
        true
    }

    async fn validate_bound(
        &self,
        connection: &Arc<Connection>,
        notification: Option<Uuid>,
    ) -> bool {
        let Some(bound) = connection.snapshot() else {
            return false;
        };
        if bound.context.expires_at <= Utc::now() {
            self.retire(connection, true);
            return false;
        }
        let result = tokio::select! {
            biased;
            _ = connection.cancel.cancelled() => return false,
            result = self.platform.validate(&bound.token, notification, Some(connection.request_id)) => result,
        };
        match result {
            Ok(context) => {
                if self.bind(connection, bound.token, context) {
                    true
                } else {
                    self.retire(connection, true);
                    false
                }
            }
            Err(ValidationError::SessionInvalid) => {
                self.retire(connection, true);
                false
            }
            Err(_) if notification.is_some() => {
                self.metrics.delivery_denied.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(_) => {
                self.retire(connection, true);
                false
            }
        }
    }

    async fn monitor(self, connection: Arc<Connection>) {
        let auth_deadline = Instant::now() + self.config.auth_wait;
        // A bounded, deterministic per-connection phase shift avoids synchronized rechecks.
        let jitter = connection.request_id.as_u128() % (self.config.recheck.as_millis() / 5 + 1);
        let period = self.config.recheck + Duration::from_millis(jitter as u64);
        let mut recheck_at = Instant::now() + period;
        loop {
            if connection.closed.load(Ordering::Acquire) {
                return;
            }
            let bound = connection.snapshot();
            let deadline = if let Some(ref bound) = bound {
                let remaining = (bound.context.expires_at - Utc::now())
                    .to_std()
                    .unwrap_or_default();
                recheck_at.min(Instant::now() + remaining)
            } else {
                auth_deadline
            };
            tokio::select! {
                biased;
                _ = connection.cancel.cancelled() => return,
                _ = connection.changed.notified() => continue,
                _ = tokio::time::sleep_until(deadline) => {},
            }
            let Some(current) = connection.snapshot() else {
                self.retire(&connection, true);
                return;
            };
            if current.context.expires_at <= Utc::now() {
                self.retire(&connection, true);
                return;
            }
            if Instant::now() >= recheck_at {
                if !self.validate_bound(&connection, None).await {
                    return;
                }
                recheck_at = Instant::now() + period;
            }
        }
    }

    async fn expiry_watch(
        self,
        connection: Arc<Connection>,
        mut expiry: watch::Receiver<Option<chrono::DateTime<Utc>>>,
    ) {
        loop {
            let at = *expiry.borrow_and_update();
            if let Some(at) = at {
                let remaining = (at - Utc::now()).to_std().unwrap_or_default();
                tokio::select! {
                    biased;
                    _ = connection.cancel.cancelled() => return,
                    result = expiry.changed() => { if result.is_err() { return; } },
                    _ = tokio::time::sleep(remaining) => {
                        if connection.snapshot().is_some_and(|bound| bound.context.expires_at <= Utc::now()) {
                            self.retire(&connection, true); return;
                        }
                    },
                }
            } else {
                tokio::select! {
                    _ = connection.cancel.cancelled() => return,
                    result = expiry.changed() => { if result.is_err() { return; } },
                }
            }
        }
    }

    async fn deliver(&self, identity: Identity, hint: Hint) {
        if self.cancel.is_cancelled() {
            return;
        }
        let event = match self.store.get(&hint).await {
            Ok(Some(event)) => event,
            Ok(None) => return,
            Err(_) => {
                self.health.database.store(false, Ordering::Release);
                self.metrics.lookup_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if event.identity() != identity {
            return;
        }
        let connections: Vec<_> = self
            .connections
            .lock()
            .map(|all| all.values().cloned().collect())
            .unwrap_or_default();
        for connection in connections {
            if connection
                .snapshot()
                .is_none_or(|bound| bound.context.identity != identity)
            {
                continue;
            }
            if !self.validate_bound(&connection, Some(event.id)).await {
                continue;
            }
            // Validation may race expiry, disconnect or periodic failure. Inspect the
            // current bound state once more while queuing this nonblocking packet.
            if let Ok(bound) = connection.bound.lock() {
                if !connection.closed.load(Ordering::Acquire)
                    && bound.as_ref().is_some_and(|b| {
                        b.context.identity == identity && b.context.expires_at > Utc::now()
                    })
                    && connection.socket.emit("notification", &event).is_ok()
                {
                    self.metrics.delivered.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

async fn connected(socket: SocketRef, shared: Shared) {
    if shared.cancel.is_cancelled() {
        let _ = socket.disconnect();
        return;
    }
    let request_id = socket
        .req_parts()
        .extensions
        .get::<RequestId>()
        .map_or_else(Uuid::new_v4, |id| id.0);
    if let Some(engine) = socket.req_parts().extensions.get::<EngineAdmission>() {
        if let Ok(mut engines) = shared.engines.lock() {
            engines.retain(|_, engine| engine.strong_count() > 0);
            engines.insert(socket.id.to_string(), Arc::downgrade(&engine.0));
        }
    }
    let (expiry, expires) = watch::channel(None);
    let connection = Arc::new(Connection {
        socket: socket.clone(),
        request_id,
        attempted: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        bound: Mutex::new(None),
        expiry,
        changed: Notify::new(),
        cancel: shared.cancel.child_token(),
    });
    let admitted = shared
        .connections
        .lock()
        .map(|mut all| {
            if all.len() >= shared.config.max_connections {
                false
            } else {
                all.insert(socket.id.to_string(), connection.clone());
                true
            }
        })
        .unwrap_or(false);
    if !admitted {
        let _ = socket.disconnect();
        return;
    }
    shared.metrics.connections.fetch_add(1, Ordering::Relaxed);
    let disconnected = Arc::downgrade(&connection);
    let on_disconnect = shared.clone();
    socket.on_disconnect(move || {
        if let Some(connection) = disconnected.upgrade() {
            on_disconnect.retire(&connection, false);
        }
        async {}
    });
    let authenticate = shared.clone();
    let auth_connection = Arc::downgrade(&connection);
    socket.on("authenticate", move |TryData::<serde_json::Value>(data)| {
        // Consume in receive order before Socketioxide spawns the returned future.
        let connection = auth_connection.upgrade();
        let first = connection.as_ref().is_some_and(|connection| !connection.attempted.swap(true, Ordering::AcqRel));
        let token = data.ok().as_ref().and_then(authentication_token);
        if !first || token.is_none() { if let Some(connection) = connection.as_ref() { authenticate.retire(connection, true); } }
        let shared = authenticate.clone();
        async move {
            let Some(connection) = connection else { return; };
            let Some(token) = token.filter(|_| first && !connection.closed.load(Ordering::Acquire)) else { return; };
            let result = tokio::select! {
                biased;
                _ = connection.cancel.cancelled() => return,
                result = shared.platform.validate(&token, None, Some(connection.request_id)) => result,
            };
            match result {
                Ok(context) => { if !shared.bind(&connection, token, context) { shared.retire(&connection, true); } },
                Err(_) => shared.retire(&connection, true),
            }
        }
    });
    shared
        .tracker
        .spawn(shared.clone().expiry_watch(connection.clone(), expires));
    shared.tracker.spawn(shared.clone().monitor(connection));
}

async fn guard(State(shared): State<Shared>, mut request: Request, next: Next) -> Response {
    if let Some(ConnectInfo(control)) = request.extensions().get::<ConnectInfo<Control>>() {
        control.detach();
    }
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::new_v4);
    request.extensions_mut().insert(RequestId(id));
    let path = request.uri().path();
    let prefix = shared.config.socket_path.trim_end_matches('/');
    let is_socket = path.starts_with(prefix);
    if is_socket {
        // Engine.IO matches all prefix collisions. Reject those before the
        // engine so they cannot bypass Origin or admission control.
        if path != prefix
            && !path
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/'))
        {
            return StatusCode::NOT_FOUND.into_response();
        }
        let mut origins = request.headers().get_all(header::ORIGIN).iter();
        let allowed = origins
            .next()
            .is_some_and(|origin| shared.config.allowed_origins.contains(origin))
            && origins.next().is_none();
        if !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
        let origin = request.headers().get(header::ORIGIN).cloned();
        if request.method() == Method::OPTIONS {
            let mut response = StatusCode::NO_CONTENT.into_response();
            if let Some(origin) = origin {
                response
                    .headers_mut()
                    .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            }
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            response.headers_mut().insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("content-type, x-request-id"),
            );
            return response;
        }
        let has_sid = match existing_sid(request.uri().query()) {
            Ok(existing) => existing,
            Err(()) => return StatusCode::BAD_REQUEST.into_response(),
        };
        let initial_polling = !has_sid
            && request.method() == Method::GET
            && request
                .uri()
                .query()
                .unwrap_or_default()
                .split('&')
                .any(|part| part == "transport=polling");
        if !has_sid {
            let Ok(permit) = shared.admission.clone().try_acquire_owned() else {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            };
            let group = Group::default();
            if let Some(ConnectInfo(control)) = request.extensions().get::<ConnectInfo<Control>>() {
                group.register(control);
            }
            request
                .extensions_mut()
                .insert(EngineAdmission(Arc::new(EngineSession {
                    _permit: permit,
                    group,
                })));
        } else {
            let sid = request
                .uri()
                .query()
                .unwrap_or_default()
                .split('&')
                .find_map(|pair| pair.strip_prefix("sid="));
            let engine = sid.and_then(|sid| shared.engines.lock().ok()?.get(sid)?.upgrade());
            if let (Some(engine), Some(ConnectInfo(control))) =
                (engine, request.extensions().get::<ConnectInfo<Control>>())
            {
                engine.group.register(control);
                // A retired polling SID must not enter an Engine handler: its
                // GET could otherwise wait for a heartbeat after IO revocation.
                if control.is_closed() {
                    return StatusCode::GONE.into_response();
                }
            }
        }
        let initial_engine = request.extensions().get::<EngineAdmission>().cloned();
        let mut response = next.run(request).await;
        if initial_polling {
            let (parts, body) = response.into_parts();
            let Ok(Ok(bytes)) =
                tokio::time::timeout(shared.config.auth_wait, axum::body::to_bytes(body, 8192))
                    .await
            else {
                if let Some(engine) = initial_engine {
                    engine.0.group.close();
                }
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            };
            if let (Some(engine), Some(open)) = (initial_engine, bytes.strip_prefix(b"0")) {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(open) {
                    if let Some(sid) = value
                        .get("sid")
                        .and_then(serde_json::Value::as_str)
                        .filter(|sid| sid.parse::<socketioxide::socket::Sid>().is_ok())
                    {
                        if let Ok(mut engines) = shared.engines.lock() {
                            engines.retain(|_, engine| engine.strong_count() > 0);
                            engines.insert(sid.to_owned(), Arc::downgrade(&engine.0));
                        }
                    }
                }
            }
            response = Response::from_parts(parts, axum::body::Body::from(bytes));
        }
        if let Some(origin) = origin {
            response
                .headers_mut()
                .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        }
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Origin"));
        if let Ok(id) = HeaderValue::from_str(&id.to_string()) {
            response.headers_mut().insert("x-request-id", id);
        }
        return response;
    }
    let mut response = next.run(request).await;
    if let Ok(id) = HeaderValue::from_str(&id.to_string()) {
        response.headers_mut().insert("x-request-id", id);
    }
    response
}

fn existing_sid(query: Option<&str>) -> Result<bool, ()> {
    let mut values = query
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.strip_prefix("sid="));
    match values.next() {
        None => Ok(false),
        Some(value)
            if values.next().is_none() && value.parse::<socketioxide::socket::Sid>().is_ok() =>
        {
            Ok(true)
        }
        Some(_) => Err(()),
    }
}

async fn readiness(State(shared): State<Shared>) -> Response {
    let status = if !shared.cancel.is_cancelled() && shared.health.healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(shared.health.snapshot())).into_response()
}

async fn metrics_endpoint(
    State(shared): State<Shared>,
    headers: axum::http::HeaderMap,
) -> Response {
    let allowed = shared.config.metrics_token.as_ref().is_some_and(|secret| {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|header| header.strip_prefix("Bearer ") == Some(secret.as_str()))
    });
    if !allowed {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    (
        [
            (
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        shared
            .metrics
            .render(&shared.queue.snapshot(), &shared.health),
    )
        .into_response()
}

async fn shutdown_signal() {
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    // Production container stdin is normally /dev/null. EOF shutdown is an
    // explicit test-driver capability; signals remain the production lifecycle.
    let _stdin_guard = if std::env::var("NOTIFICATION_SHUTDOWN_ON_STDIN").as_deref() == Ok("1") {
        std::thread::spawn(move || {
            use std::io::Read;
            let mut stdin = std::io::stdin().lock();
            let mut bytes = [0; 256];
            while stdin.read(&mut bytes).unwrap_or(0) > 0 {}
            let _ = eof_tx.send(());
        });
        None
    } else {
        Some(eof_tx)
    };
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = eof_rx => {}, _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    tokio::select! { _ = eof_rx => {}, _ = tokio::signal::ctrl_c() => {} }
}

/// Load explicit configuration, start the bounded notification service, and wait
/// for Ctrl+C or SIGTERM (stdin EOF only when explicitly enabled for tests).
/// Diagnostics never include credentials.
pub async fn run_from_env() -> io::Result<()> {
    let config = Arc::new(
        Config::from_env().map_err(|_| io::Error::other("invalid notification configuration"))?,
    );
    let metrics = Arc::new(Metrics::default());
    let health = Arc::new(Health::default());
    let platform = Platform::new(&config, metrics.clone())
        .map_err(|_| io::Error::other("platform client configuration failed"))?;
    let store = Store::connect(&config)
        .await
        .map_err(|_| io::Error::other("database client configuration failed"))?;
    let (queue, runner) = scheduler::channel(Limits {
        max_partitions: config.max_partitions,
        per_partition_queue: config.per_partition_queue,
        total_queue: config.total_queue,
        concurrency: config.max_inflight,
    })
    .map_err(io::Error::other)?;
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let shared = Shared {
        admission: Arc::new(Semaphore::new(config.max_connections)),
        config: config.clone(),
        platform,
        store,
        metrics,
        health,
        queue,
        connections: Arc::new(Mutex::new(HashMap::new())),
        engines: Arc::new(Mutex::new(HashMap::new())),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    };
    let (layer, io) = SocketIo::builder()
        .req_path(config.socket_path.trim_end_matches('/').to_owned())
        .connect_timeout(config.auth_wait)
        .max_buffer_size(32)
        .max_payload(8192)
        .ws_max_message_size(8192)
        .ws_max_frame_size(8192)
        .build_layer();
    let connect_state = shared.clone();
    io.ns("/", async move |socket: SocketRef| {
        connected(socket, connect_state).await
    });
    let app = Router::new()
        .route(
            "/health/live",
            get(|| async { Json(json!({"status":"UP"})) }),
        )
        .route("/health", get(readiness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(metrics_endpoint))
        .with_state(shared.clone())
        .layer(layer)
        .layer(middleware::from_fn_with_state(shared.clone(), guard));
    let listener = TcpListener::bind((config.host, config.port))
        .await
        .map_err(|_| io::Error::other("notification listener bind failed"))?;
    let worker = shared.clone();
    let schedule = tokio::spawn(runner.run(move |identity, hint| {
        let worker = worker.clone();
        async move { worker.deliver(identity, hint).await }
    }));
    let store = shared.store.clone();
    let sender = shared.queue.clone();
    let stop = cancel.clone();
    let status = shared.health.clone();
    let counters = shared.metrics.clone();
    let listening = tokio::spawn(async move { store.listen(sender, stop, status, counters).await });
    let probe = shared.clone();
    tracker.spawn(async move {
        let mut ticks=tokio::time::interval(probe.config.health_probe);
        loop { tokio::select! {
            biased;
            _=probe.cancel.cancelled()=>return,
            _=ticks.tick()=> {
                let (database,platform)=tokio::join!(probe.store.probe(),probe.platform.probe());
                probe.health.database.store(database,Ordering::Release);probe.health.platform.store(platform,Ordering::Release);
            }
        }}
    });
    println!(
        "{}",
        json!({"event":"ready","service":"notification-ws","port":listener.local_addr()?.port()})
    );
    let shutdown = shared.clone();
    let close_io = io.clone();
    let listener = ControlledListener::new(
        listener,
        config.max_connections.saturating_mul(4).saturating_add(16),
    )?;
    let transport_shutdown = listener.shutdown_handle();
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<Control>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        shutdown.cancel.cancel();
        shutdown.queue.close();
        let connections: Vec<_> = shutdown
            .connections
            .lock()
            .map(|all| all.values().cloned().collect())
            .unwrap_or_default();
        for connection in connections {
            shutdown.retire(&connection, true);
        }
        // Namespace/Engine shutdown and the bounded flush window progress
        // together. Global TCP revocation also wakes peers still waiting for
        // their first complete HTTP head, which Hyper cannot drain gracefully.
        // join! awaits both futures; there is no detached close task or timer.
        tokio::join!(close_io.close(), async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            transport_shutdown.close();
        });
    })
    .await;
    cancel.cancel();
    shared.queue.close();
    io.close().await;
    tracker.close();
    tracker.wait().await;
    let _ = schedule.await;
    let _ = listening.await;
    shared.store.close().await;
    serve.map_err(|_| io::Error::other("notification HTTP server stopped unexpectedly"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_or_duplicate_sid_cannot_skip_engine_admission() {
        assert_eq!(existing_sid(Some("EIO=4&transport=polling")), Ok(false));
        for query in ["sid=", "sid=invalid", "sid=&sid=invalid"] {
            assert_eq!(existing_sid(Some(query)), Err(()));
        }
        let id = socketioxide::socket::Sid::new().to_string();
        assert_eq!(existing_sid(Some(&format!("EIO=4&sid={id}"))), Ok(true));
        assert_eq!(existing_sid(Some(&format!("sid={id}&sid={id}"))), Err(()));
    }
}
