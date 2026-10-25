//! Phase 4: real notification binary, PostgreSQL LISTEN and Engine.IO polling.
//! Run explicitly with an owned loopback IDENTITY_PHASE04_TEST_DATABASE_URL and
//! CI_TEST_POSTGRES_OWNER=ci-..., then `--test identity_isolation -- --ignored`.
//! This fixture owns only its random database. It does not import platform code
//! or change the Notification 1 public acknowledgement/event contracts.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    future::{Future, IntoFuture},
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{SecondsFormat, Utc};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use sqlx::{postgres::PgConnectOptions, PgPool};
use tokio::{net::TcpListener, task::JoinHandle};
use uuid::Uuid;

const ORIGIN: &str = "http://127.0.0.1:19404";
const METRICS: &str = "phase04_metrics_YVn9gF2qD8sL4wR7kB6tX3mJ5aH0cZ1e";
const PROOF_PATH: &str = "/api/internal/notifications/context-proof";

#[derive(Clone, Copy)]
struct Recipient {
    user: Uuid,
    tenant: Uuid,
    organization: Uuid,
    membership: Uuid,
    member_type: &'static str,
    version: i64,
}

impl Recipient {
    fn employee() -> Self {
        Self {
            user: Uuid::now_v7(),
            tenant: Uuid::now_v7(),
            organization: Uuid::now_v7(),
            membership: Uuid::now_v7(),
            member_type: "EMPLOYEE",
            version: 1,
        }
    }

    fn customer(self) -> Self {
        Self {
            membership: Uuid::now_v7(),
            member_type: "CUSTOMER",
            ..self
        }
    }

    fn proof(self) -> Value {
        json!({
            "proofVersion":"1", "contractVersion":"1.0", "userId":self.user,
            "tenantId":self.tenant, "organizationId":self.organization,
            "sessionId":Uuid::now_v7(),
            "expiresAt":(Utc::now()+chrono::Duration::seconds(120))
                .to_rfc3339_opts(SecondsFormat::Millis,true),
            "membershipId":self.membership, "memberType":self.member_type,
            "identityVersion":self.version, "accountVersion":0,
        })
    }

    fn hint(self, id: Uuid) -> Value {
        json!({"hintVersion":"1","id":id,"userId":self.user,"tenantId":self.tenant,
            "organizationId":self.organization,"membershipId":self.membership,
            "memberType":self.member_type,"identityVersion":self.version})
    }

    fn matches(self, proof: &Value) -> bool {
        proof["userId"] == self.user.to_string()
            && proof["tenantId"] == self.tenant.to_string()
            && proof["organizationId"] == self.organization.to_string()
            && proof["membershipId"] == self.membership.to_string()
            && proof["memberType"] == self.member_type
            && proof["identityVersion"] == self.version
    }
}

#[derive(Clone)]
struct Session {
    proof: Value,
    active: bool,
    context_status: Option<u16>,
    delivery_status: Option<u16>,
    delivery_patch: Option<Value>,
}

#[derive(Default)]
struct Authority {
    sessions: HashMap<String, Session>,
    notifications: HashMap<Uuid, Recipient>,
    delivery_calls: usize,
    wrong_path_calls: usize,
}

type Shared = Arc<Mutex<Authority>>;

async fn context_proof(State(shared): State<Shared>, headers: HeaderMap, uri: Uri) -> Response {
    let Some(token) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let mut requested = Url::parse(ORIGIN).unwrap();
    requested.set_query(uri.query());
    let query = requested.query_pairs().collect::<Vec<_>>();
    let notification = match query.as_slice() {
        [] => None,
        [(key, value)] if key.as_ref() == "notificationId" => match Uuid::parse_str(value.as_ref())
        {
            Ok(id) => Some(id),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    let mut state = shared.lock().unwrap();
    if notification.is_some() {
        state.delivery_calls += 1;
    }
    let Some(session) = state.sessions.get(token).cloned() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !session.active {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some(status) = session.context_status {
        return StatusCode::from_u16(status).unwrap().into_response();
    }
    let mut proof = session.proof;
    if let Some(id) = notification {
        if let Some(status) = session.delivery_status {
            return StatusCode::from_u16(status).unwrap().into_response();
        }
        if !state
            .notifications
            .get(&id)
            .is_some_and(|recipient| recipient.matches(&proof))
        {
            return StatusCode::NOT_FOUND.into_response();
        }
        if let Some(patch) = session.delivery_patch {
            for (key, value) in patch.as_object().unwrap() {
                proof[key] = value.clone();
            }
        }
    }
    Json(proof).into_response()
}

async fn wrong_path(State(shared): State<Shared>) -> StatusCode {
    shared.lock().unwrap().wrong_path_calls += 1;
    StatusCode::NOT_FOUND
}

struct OwnedDatabase {
    admin: PgPool,
    pool: PgPool,
    name: String,
    url: String,
}

impl OwnedDatabase {
    async fn create() -> Self {
        let raw = std::env::var("IDENTITY_PHASE04_TEST_DATABASE_URL")
            .expect("explicit owned Phase 4 database URL required");
        let owner = std::env::var("CI_TEST_POSTGRES_OWNER")
            .expect("explicit CI_TEST_POSTGRES_OWNER required");
        assert!(
            owner.starts_with("ci-")
                && owner.len() <= 63
                && owner
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "Phase 4 database owner must be an explicit ci-* identifier"
        );
        let mut url = Url::parse(&raw).expect("valid explicit database URL required");
        assert!(matches!(url.scheme(), "postgres" | "postgresql"));
        assert!(
            !url.username().is_empty(),
            "explicit PostgreSQL administrator username required"
        );
        assert!(
            matches!(
                url.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
            ),
            "Phase 4 fixture only accepts loopback database URLs"
        );
        // A URL query must not silently redirect the loopback connection to a
        // different host/database or load a password file supplied by a caller.
        assert!(url.query().is_none() && url.fragment().is_none());
        assert_eq!(
            url.path(),
            "/postgres",
            "explicit administrator /postgres database required"
        );
        assert!(
            url.port().is_some(),
            "explicit loopback PostgreSQL port required"
        );
        let name = format!("identity_p04_{}", Uuid::now_v7().simple());
        assert!(name.len() < 63 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        let admin = PgPool::connect_with(
            PgConnectOptions::from_str(&raw)
                .expect("valid PostgreSQL connection options")
                .database("postgres")
                .application_name("identity-phase04-owner"),
        )
        .await
        .expect("owned loopback database administrator connection");
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .expect("create random Phase 4 database");
        url.set_path(&format!("/{name}"));
        let pool = match PgPool::connect(url.as_str()).await {
            Ok(pool) => pool,
            Err(_) => {
                sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
                    .execute(&admin)
                    .await
                    .expect("cleanup database after fixture connection failure");
                admin.close().await;
                panic!("connect random Phase 4 database");
            }
        };
        let schema = sqlx::query(
            "CREATE TABLE notifications(\
             id uuid PRIMARY KEY,user_id uuid NOT NULL,tenant_id uuid NOT NULL,\
             organization_id uuid NOT NULL,source_kind text NOT NULL,application_id uuid,\
             publication_id uuid,type text NOT NULL,title text NOT NULL,content text NOT NULL,\
             link text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),\
             recipient_membership_id uuid,recipient_member_type text,recipient_identity_version bigint)",
        )
        .execute(&pool)
        .await;
        if schema.is_err() {
            pool.close().await;
            sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
                .execute(&admin)
                .await
                .expect("cleanup database after fixture schema failure");
            admin.close().await;
            panic!("create isolated notification projection");
        }
        Self {
            admin,
            pool,
            name,
            url: url.to_string(),
        }
    }

    async fn cleanup(self) {
        self.pool.close().await;
        assert!(self.name.starts_with("identity_p04_"));
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .execute(&self.admin)
            .await
            .expect("remove owned Phase 4 database");
        self.admin.close().await;
        println!("IDENTITY_PHASE04_CLEANUP database_removed=true pools_closed=true");
    }
}

async fn owned<F, Fut>(test: F)
where
    F: FnOnce(PgPool, String) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let db = OwnedDatabase::create().await;
    let pool = db.pool.clone();
    let url = db.url.clone();
    let result = tokio::spawn(async move { test(pool, url).await }).await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}

struct Rig {
    child: Child,
    authority_task: Option<JoinHandle<()>>,
    stdout_task: Option<std::thread::JoinHandle<()>>,
    authority: Shared,
    pool: PgPool,
    client: Client,
    origin: String,
    sent_hints: u64,
}

impl Drop for Rig {
    fn drop(&mut self) {
        if let Some(task) = &self.authority_task {
            task.abort();
        }
        // Also runs on assertion panic before the owned database is removed.
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(task) = self.stdout_task.take() {
            let _ = task.join();
        }
    }
}

impl Rig {
    async fn start(pool: PgPool, database_url: String, recheck_ms: u64) -> Self {
        let authority = Shared::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let authority_origin = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route(PROOF_PATH, get(context_proof))
            .fallback(wrong_path)
            .with_state(authority.clone());
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, router).into_future().await.unwrap();
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_saas-notification-ws"));
        command.env_clear();
        for key in ["SYSTEMROOT", "WINDIR", "PATH"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        command
            .env("DATABASE_URL", database_url)
            .env("PLATFORM_API_BASE_URL", authority_origin)
            .env("NOTIFICATION_ENV", "test")
            .env("NOTIFICATION_ALLOWED_ORIGINS", ORIGIN)
            .env("NOTIFICATION_HOST", "127.0.0.1")
            .env("NOTIFICATION_PORT", "0")
            .env("NOTIFICATION_SOCKET_PATH", "/socket.io/")
            .env("NOTIFICATION_AUTH_TIMEOUT_MS", "500")
            .env("NOTIFICATION_AUTH_RECHECK_MS", recheck_ms.to_string())
            .env("NOTIFICATION_AUTH_WAIT_MS", "1500")
            .env("NOTIFICATION_HEALTH_PROBE_MS", "1000")
            .env("NOTIFICATION_DATABASE_RETRY_MS", "100")
            .env("NOTIFICATION_SHUTDOWN_ON_STDIN", "1")
            .env("METRICS_TOKEN", METRICS)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                authority_task.abort();
                let _ = authority_task.await;
                panic!("spawn actual notification binary");
            }
        };
        let stdout = child.stdout.take().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let stdout_task = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    if value["event"] == "ready" {
                        let _ = ready_tx.send(value["port"].as_u64().unwrap());
                    }
                }
            }
        });
        // Construct the drop guard before waiting so failed startup kills the
        // child and releases the authority listener before database cleanup.
        let mut rig = Self {
            child,
            authority_task: Some(authority_task),
            stdout_task: Some(stdout_task),
            authority,
            pool,
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(4))
                .build()
                .unwrap(),
            origin: String::new(),
            sent_hints: 0,
        };
        let port =
            tokio::task::spawn_blocking(move || ready_rx.recv_timeout(Duration::from_secs(10)))
                .await
                .unwrap()
                .expect("notification binary reports its ephemeral listening port");
        rig.origin = format!("http://127.0.0.1:{port}");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let response = rig
                .client
                .get(format!("{}/health/ready", rig.origin))
                .send()
                .await
                .unwrap();
            if response.status().is_success() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "isolated notification dependencies become ready"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        rig
    }

    fn session(&self, token: &str, recipient: Recipient) {
        self.authority.lock().unwrap().sessions.insert(
            token.into(),
            Session {
                proof: recipient.proof(),
                active: true,
                context_status: None,
                delivery_status: None,
                delivery_patch: None,
            },
        );
    }

    async fn insert(&self, recipient: Recipient, owned: bool) -> Uuid {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO notifications(id,user_id,tenant_id,organization_id,source_kind,type,title,content,link,recipient_membership_id,recipient_member_type,recipient_identity_version) VALUES($1,$2,$3,$4,'SYSTEM','INFO','Phase 4 persisted notification','','/profile',$5,$6,$7)")
            .bind(id).bind(recipient.user).bind(recipient.tenant).bind(recipient.organization)
            .bind(owned.then_some(recipient.membership)).bind(owned.then_some(recipient.member_type))
            .bind(owned.then_some(recipient.version)).execute(&self.pool).await.unwrap();
        if owned {
            self.authority
                .lock()
                .unwrap()
                .notifications
                .insert(id, recipient);
        }
        id
    }

    async fn hint(&mut self, value: Value) {
        sqlx::query("SELECT pg_notify('new_notification',$1)")
            .bind(value.to_string())
            .execute(&self.pool)
            .await
            .unwrap();
        self.sent_hints += 1;
    }

    async fn publish(&mut self, recipient: Recipient) -> Uuid {
        let id = self.insert(recipient, true).await;
        self.hint(recipient.hint(id)).await;
        id
    }

    async fn metric(&self, name: &str) -> u64 {
        let body = self
            .client
            .get(format!("{}/metrics", self.origin))
            .bearer_auth(METRICS)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        body.lines()
            .find(|line| line.starts_with(&format!("{name}{{")))
            .and_then(|line| line.split_whitespace().last())
            .unwrap()
            .parse()
            .unwrap()
    }

    async fn idle(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stable = 0;
        loop {
            if self.metric("saas_notification_hints_received_total").await >= self.sent_hints
                && self.metric("saas_notification_pending_hints").await == 0
                && self.metric("saas_notification_active_partitions").await == 0
            {
                stable += 1;
                if stable == 3 {
                    return;
                }
            } else {
                stable = 0;
            }
            assert!(
                Instant::now() < deadline,
                "every emitted database hint is processed within the bound"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    async fn sentinel(&mut self, socket: &mut Polling, recipient: Recipient) {
        self.idle().await;
        let id = self.publish(recipient).await;
        let event = socket.notification().await;
        assert_eq!(
            event["id"],
            id.to_string(),
            "only the allowed sentinel may reach this member connection"
        );
        assert_event(&event, recipient);
    }

    async fn shutdown(&mut self) {
        assert_eq!(
            self.authority.lock().unwrap().wrong_path_calls,
            0,
            "service uses only the private proof endpoint"
        );
        drop(self.child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "real notification service shuts down successfully"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "stdin EOF shuts down all notification resources"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if let Some(task) = self.authority_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.stdout_task.take() {
            task.join()
                .expect("notification stdout reader exits with its child");
        }
    }
}

fn keys(value: &Value) -> BTreeSet<&str> {
    value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect()
}

fn assert_ack(value: &Value, recipient: Recipient) {
    assert_eq!(
        keys(value),
        [
            "contractVersion",
            "userId",
            "tenantId",
            "organizationId",
            "expiresAt"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(value["contractVersion"], "1.0");
    assert_eq!(value["userId"], recipient.user.to_string());
    assert_eq!(value["tenantId"], recipient.tenant.to_string());
    assert_eq!(value["organizationId"], recipient.organization.to_string());
}

fn assert_event(value: &Value, recipient: Recipient) {
    assert_eq!(
        keys(value),
        [
            "contractVersion",
            "id",
            "userId",
            "tenantId",
            "organizationId",
            "applicationId",
            "publicationId",
            "type",
            "title",
            "content",
            "link",
            "createdAt"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(value["contractVersion"], "1.0");
    assert_eq!(value["userId"], recipient.user.to_string());
    assert_eq!(value["tenantId"], recipient.tenant.to_string());
    assert_eq!(value["organizationId"], recipient.organization.to_string());
}

/// Minimal Engine.IO v4 + default Socket.IO namespace polling client. A timed-out
/// GET is never reused: positive sentinels flush negative-delivery assertions, so
/// cancelling a long poll cannot manufacture an overlapping-poll disconnection.
struct Polling {
    client: Client,
    url: String,
    events: VecDeque<(String, Value)>,
    namespace: bool,
    closed: bool,
}

impl Polling {
    async fn open(rig: &Rig) -> Self {
        let initial = format!("{}/socket.io/?EIO=4&transport=polling", rig.origin);
        let body = rig
            .client
            .get(&initial)
            .header("Origin", ORIGIN)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        let open: Value =
            serde_json::from_str(body.strip_prefix('0').expect("Engine.IO open packet")).unwrap();
        let sid = open["sid"].as_str().unwrap();
        let mut socket = Self {
            client: rig.client.clone(),
            url: format!("{initial}&sid={sid}"),
            events: VecDeque::new(),
            namespace: false,
            closed: false,
        };
        socket.post("40".into()).await;
        while !socket.namespace {
            socket.poll().await;
            assert!(!socket.closed, "default namespace connects");
        }
        socket
    }

    async fn post(&self, packet: String) {
        let response = self
            .client
            .post(&self.url)
            .header("Origin", ORIGIN)
            .header("Content-Type", "text/plain;charset=UTF-8")
            .body(packet)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "Engine.IO POST accepted");
    }

    async fn poll(&mut self) {
        let response = self
            .client
            .get(&self.url)
            .header("Origin", ORIGIN)
            .send()
            .await;
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                assert!(
                    matches!(response.status().as_u16(), 400 | 404 | 410),
                    "only a retired Engine session closes polling"
                );
                self.closed = true;
                return;
            }
            Err(error) if !error.is_timeout() => {
                self.closed = true;
                return;
            }
            Err(_) => panic!("Engine.IO poll timed out before expected event or disconnect"),
        };
        let body = response.text().await.unwrap();
        for packet in body.split('\u{1e}') {
            if packet == "2" {
                self.post("3".into()).await;
            } else if packet == "1" || packet == "41" {
                self.closed = true;
            } else if packet.starts_with("40") {
                self.namespace = true;
            } else if let Some(payload) = packet.strip_prefix("42") {
                let value: Value = serde_json::from_str(payload).unwrap();
                self.events
                    .push_back((value[0].as_str().unwrap().into(), value[1].clone()));
            } else {
                assert!(
                    packet == "6" || packet.is_empty(),
                    "known Engine.IO polling packet"
                );
            }
        }
    }

    async fn authenticate(&self, token: &str) {
        self.post(format!("42{}", json!(["authenticate",{"token":token}])))
            .await;
    }

    async fn next(&mut self) -> Option<(String, Value)> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Some(event);
            }
            if self.closed {
                return None;
            }
            self.poll().await;
        }
    }

    async fn accepted(rig: &Rig, token: &str, recipient: Recipient) -> Self {
        let mut socket = Self::open(rig).await;
        socket.authenticate(token).await;
        let (name, ack) = socket.next().await.expect("authenticated acknowledgement");
        assert_eq!(name, "authenticated");
        assert_ack(&ack, recipient);
        socket
    }

    async fn notification(&mut self) -> Value {
        let (name, event) = self
            .next()
            .await
            .expect("allowed persistent notification reaches live polling connection");
        assert_eq!(name, "notification");
        event
    }

    async fn disconnect(&mut self) {
        self.post("1".into()).await;
        self.closed = true;
        self.events.clear();
    }

    async fn rejected(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(6);
        while let Some((name, _)) = self.next().await {
            assert_eq!(
                name, "unauthorized",
                "retired identity receives no notification or renewed acknowledgement"
            );
            assert!(
                Instant::now() < deadline,
                "retired socket closes its Engine transport"
            );
        }
        assert!(self.closed);
    }
}

#[tokio::test]
#[ignore = "requires explicit owned Phase 4 loopback PostgreSQL"]
async fn dual_identity_delivery_local_revocation_replacement_and_reconnect() {
    owned(|pool, url| async move {
        let mut rig = Rig::start(pool, url, 300_000).await;
        let employee = Recipient::employee();
        let customer = employee.customer();
        rig.session("employee", employee);
        rig.session("customer", customer);
        let mut employee_socket = Polling::accepted(&rig, "employee", employee).await;
        let mut customer_socket = Polling::accepted(&rig, "customer", customer).await;
        let employee_id = rig.publish(employee).await;
        rig.idle().await;
        let event = employee_socket.notification().await;
        assert_eq!(event["id"], employee_id.to_string());
        assert_event(&event, employee);
        rig.sentinel(&mut customer_socket, customer).await;
        customer_socket.disconnect().await;
        customer_socket = Polling::accepted(&rig, "customer", customer).await;
        rig.sentinel(&mut customer_socket, customer).await;
        rig.authority
            .lock()
            .unwrap()
            .sessions
            .get_mut("employee")
            .unwrap()
            .active = false;
        rig.publish(employee).await;
        rig.idle().await;
        employee_socket.rejected().await;
        rig.sentinel(&mut customer_socket, customer).await;
        let mut old = Polling::open(&rig).await;
        old.authenticate("employee").await;
        old.rejected().await;

        let replacement = Recipient {
            membership: Uuid::now_v7(),
            version: 2,
            ..employee
        };
        rig.session("replacement", replacement);
        let mut fresh = Polling::accepted(&rig, "replacement", replacement).await;
        rig.publish(employee).await;
        rig.idle().await;
        rig.sentinel(&mut fresh, replacement).await;
        rig.sentinel(&mut customer_socket, customer).await;
        // Account-wide credential revocation rejects every old identity session.
        for session in rig.authority.lock().unwrap().sessions.values_mut() {
            session.active = false;
        }
        rig.publish(replacement).await;
        rig.publish(customer).await;
        rig.idle().await;
        fresh.rejected().await;
        customer_socket.rejected().await;
        for token in ["replacement", "customer"] {
            let mut reconnect = Polling::open(&rig).await;
            reconnect.authenticate(token).await;
            reconnect.rejected().await;
        }
        rig.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires explicit owned Phase 4 loopback PostgreSQL"]
async fn legacy_hints_null_recipient_proofs_and_authority_faults_fail_closed() {
    owned(|pool,url|async move {
        let mut rig=Rig::start(pool,url,300_000).await;let employee=Recipient::employee();
        rig.session("employee",employee);let mut socket=Polling::accepted(&rig,"employee",employee).await;
        let valid=rig.insert(employee,true).await;
        rig.hint(json!({"id":valid,"userId":employee.user,"tenantId":employee.tenant,"organizationId":employee.organization})).await;
        for (field,value) in [("hintVersion",json!("0")),("identityVersion",json!("1")),("identityVersion",json!(1.5)),("identityVersion",json!(0)),("memberType",json!("UNKNOWN"))] {
            let mut hint=employee.hint(valid);hint[field]=value;rig.hint(hint).await;
        }
        let no_proof=rig.insert(employee,false).await;rig.hint(employee.hint(no_proof)).await;
        let wrong=Recipient{membership:Uuid::now_v7(),..employee};rig.hint(wrong.hint(valid)).await;
        rig.idle().await;
        assert_eq!(rig.authority.lock().unwrap().delivery_calls,0,"legacy, invalid and wrong-recipient hints never reach authority delivery checks");
        assert!(rig.metric("saas_notification_hint_drops_total").await>=6);
        rig.sentinel(&mut socket,employee).await;
        rig.authority.lock().unwrap().sessions.get_mut("employee").unwrap().delivery_status=Some(503);
        rig.publish(employee).await;rig.idle().await;
        rig.authority.lock().unwrap().sessions.get_mut("employee").unwrap().delivery_status=None;
        rig.sentinel(&mut socket,employee).await;

        // A successful HTTP response cannot rebind an established socket to a
        // different member/type/version or silently advance its account version.
        for (index,patch) in [json!({"membershipId":Uuid::now_v7()}),json!({"memberType":"CUSTOMER"}),json!({"identityVersion":2}),json!({"accountVersion":1})].into_iter().enumerate() {
            let token=format!("misbound-{index}");rig.session(&token,employee);
            let mut misbound=Polling::accepted(&rig,&token,employee).await;
            rig.authority.lock().unwrap().sessions.get_mut(&token).unwrap().delivery_patch=Some(patch);
            rig.publish(employee).await;rig.idle().await;misbound.rejected().await;
            // The independent original connection still has a valid bound proof.
            let event=socket.notification().await;assert_event(&event,employee);
        }
        // Old public six-field socket-context JSON is deliberately not a private
        // identity proof. Keep it as a negative fixture instead of enriching it.
        let old=json!({"contractVersion":"1.0","userId":employee.user,"tenantId":employee.tenant,"organizationId":employee.organization,"sessionId":Uuid::now_v7(),"expiresAt":(Utc::now()+chrono::Duration::seconds(60)).to_rfc3339()});
        let mut invalid_proofs=vec![old];
        for(field,value)in[("proofVersion",json!("0")),("identityVersion",json!("1")),("identityVersion",json!(1.5)),("identityVersion",json!(0)),("memberType",json!("UNKNOWN")),("accountVersion",json!(-1)),("accountVersion",json!(0.5)),("accountVersion",json!("0")),("unexpected",json!(true))] {
            let mut proof=employee.proof();proof[field]=value;invalid_proofs.push(proof);
        }
        for (index,proof) in invalid_proofs.into_iter().enumerate() {
            let token=format!("invalid-proof-{index}");rig.session(&token,employee);
            rig.authority.lock().unwrap().sessions.get_mut(&token).unwrap().proof=proof;
            let mut invalid=Polling::open(&rig).await;invalid.authenticate(&token).await;invalid.rejected().await;
        }
        rig.session("unavailable-context",employee);
        rig.authority.lock().unwrap().sessions.get_mut("unavailable-context").unwrap().context_status=Some(503);
        let mut unavailable=Polling::open(&rig).await;unavailable.authenticate("unavailable-context").await;unavailable.rejected().await;
        rig.shutdown().await;
    }).await;
}

#[tokio::test]
#[ignore = "requires explicit owned Phase 4 loopback PostgreSQL"]
async fn periodic_member_and_account_proof_changes_close_without_a_hint() {
    owned(|pool, url| async move {
        let mut rig = Rig::start(pool, url, 1000).await;
        let employee = Recipient::employee();
        let customer = employee.customer();
        rig.session("customer", customer);
        let mut customer_socket = Polling::accepted(&rig, "customer", customer).await;
        for (index, patch) in [
            json!({"membershipId":Uuid::now_v7()}),
            json!({"memberType":"CUSTOMER"}),
            json!({"identityVersion":2}),
            json!({"accountVersion":1}),
        ]
        .into_iter()
        .enumerate()
        {
            let token = format!("periodic-{index}");
            rig.session(&token, employee);
            let mut socket = Polling::accepted(&rig, &token, employee).await;
            {
                let mut state = rig.authority.lock().unwrap();
                let proof = &mut state.sessions.get_mut(&token).unwrap().proof;
                for (key, value) in patch.as_object().unwrap() {
                    proof[key] = value.clone();
                }
            }
            socket.rejected().await;
            rig.sentinel(&mut customer_socket, customer).await;
        }
        rig.session("surviving-employee", employee);
        let mut surviving_employee = Polling::accepted(&rig, "surviving-employee", employee).await;
        rig.authority
            .lock()
            .unwrap()
            .sessions
            .get_mut("customer")
            .unwrap()
            .active = false;
        customer_socket.rejected().await;
        let mut reconnect = Polling::open(&rig).await;
        reconnect.authenticate("customer").await;
        reconnect.rejected().await;
        rig.sentinel(&mut surviving_employee, employee).await;
        rig.authority
            .lock()
            .unwrap()
            .sessions
            .get_mut("surviving-employee")
            .unwrap()
            .context_status = Some(503);
        surviving_employee.rejected().await;
        rig.shutdown().await;
    })
    .await;
}
