use crate::{
    config::Config,
    identifiers,
    metrics::{Health, Metrics},
    protocol::{Hint, Identity, MemberType, Notification, StoredDelivery},
    scheduler::Sender,
};
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{
    postgres::{PgConnectOptions, PgListener, PgPoolOptions},
    PgPool, Row,
};
use std::{
    error::Error,
    fmt,
    str::FromStr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const NOTIFICATION_QUERY:&str="SELECT id,user_id,tenant_id,organization_id,recipient_membership_id,recipient_member_type,recipient_identity_version,source_kind,application_id,publication_id,type,title,content,link,created_at FROM notifications WHERE id=$1 AND tenant_id=$2 AND organization_id=$3 AND user_id=$4 AND recipient_membership_id=$5 AND recipient_member_type=$6 AND recipient_identity_version=$7 AND source_kind IN ('APP','SYSTEM')";

pub(crate) enum StoreError {
    Database(sqlx::Error),
    Deadline,
}
impl fmt::Debug for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StoreError(database operation failed)")
    }
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Notification database operation failed")
    }
}
impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Deadline => None,
        }
    }
}
impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Clone)]
pub(crate) struct Store {
    pool: PgPool,
    listen_options: PgConnectOptions,
    retry: Duration,
    heartbeat: Duration,
}

impl Store {
    pub(crate) async fn connect(config: &Config) -> Result<Self, StoreError> {
        // Overwrite application_name after parsing so URL options cannot change
        // ownership markers used by operations and isolated recovery tests.
        let base = PgConnectOptions::from_str(&config.database_url)?;
        let query_options = base
            .clone()
            .application_name("platform-notification-query")
            .options([("statement_timeout", "5000")]);
        let listen_options = base
            .application_name("platform-notification-listen")
            .options([("statement_timeout", "2000")]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .min_connections(0)
            .acquire_timeout(CONNECT_TIMEOUT)
            .connect_lazy_with(query_options);
        Ok(Self {
            pool,
            listen_options,
            retry: config.db_retry,
            heartbeat: config.health_probe,
        })
    }

    pub(crate) async fn get(&self, hint: &Hint) -> Result<Option<StoredDelivery>, StoreError> {
        let row = tokio::time::timeout(
            QUERY_TIMEOUT,
            sqlx::query(NOTIFICATION_QUERY)
                .bind(hint.id)
                .bind(hint.identity.tenant_id)
                .bind(hint.identity.organization_id)
                .bind(hint.identity.user_id)
                .bind(hint.identity.membership_id)
                .bind(hint.identity.member_type.as_str())
                .bind(hint.identity.identity_version)
                .fetch_optional(&self.pool),
        )
        .await
        .map_err(|_| StoreError::Deadline)??;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = Record {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            tenant_id: row.try_get("tenant_id")?,
            organization_id: row.try_get("organization_id")?,
            recipient_membership_id: row.try_get("recipient_membership_id")?,
            recipient_member_type: row.try_get("recipient_member_type")?,
            recipient_identity_version: row.try_get("recipient_identity_version")?,
            source_kind: row.try_get("source_kind")?,
            application_id: row.try_get("application_id")?,
            publication_id: row.try_get("publication_id")?,
            kind: row.try_get("type")?,
            title: row.try_get("title")?,
            content: row.try_get("content")?,
            link: row.try_get("link")?,
            created_at: row.try_get("created_at")?,
        };
        Ok(record.notification(hint))
    }

    pub(crate) async fn probe(&self) -> bool {
        tokio::time::timeout(QUERY_TIMEOUT,sqlx::query(
            "SELECT id,user_id,tenant_id,organization_id,recipient_membership_id,recipient_member_type,recipient_identity_version,source_kind,application_id,publication_id,type,title,content,link,created_at FROM notifications WHERE source_kind IN ('APP','SYSTEM') LIMIT 0"
        ).execute(&self.pool)).await.is_ok_and(|result|result.is_ok())
    }

    pub(crate) async fn listen(
        &self,
        sender: Sender<Identity, Hint>,
        cancel: CancellationToken,
        health: Arc<Health>,
        metrics: Arc<Metrics>,
    ) {
        let mut failures = 0_u32;
        loop {
            health.listen.store(false, Ordering::Release);
            if cancel.is_cancelled() {
                return;
            }
            // A fresh one-connection pool prevents SQLx's implicit reconnect
            // from hiding DOWN or bypassing our configured jittered backoff.
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .min_connections(0)
                .max_lifetime(None)
                .idle_timeout(None)
                .acquire_timeout(CONNECT_TIMEOUT)
                .connect_lazy_with(self.listen_options.clone());
            let connected = tokio::select! {
                _=cancel.cancelled()=>{pool.close().await;return;},
                result=tokio::time::timeout(CONNECT_TIMEOUT,async {
                    let mut listener=PgListener::connect_with(&pool).await?;
                    listener.eager_reconnect(false);
                    listener.listen("new_notification").await?;
                    Ok::<_,sqlx::Error>(listener)
                })=>result,
            };
            if let Ok(Ok(mut listener)) = connected {
                health.listen.store(true, Ordering::Release);
                failures = 0;
                let mut heartbeat = tokio::time::interval(self.heartbeat);
                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                heartbeat.tick().await;
                loop {
                    tokio::select! {
                        biased;
                        _=cancel.cancelled()=>break,
                        notification=listener.try_recv()=>{
                            match notification {
                                Ok(Some(notification)) if notification.channel()=="new_notification"=>{
                                    metrics.hints_received.fetch_add(1,Ordering::Relaxed);
                                    match Hint::parse(notification.payload()) {
                                        Some(hint)=>{
                                            if !sender.try_enqueue(hint.identity,hint) {metrics.hints_dropped.fetch_add(1,Ordering::Relaxed);}
                                        },
                                        None=>{metrics.hints_dropped.fetch_add(1,Ordering::Relaxed);},
                                    }
                                },
                                Ok(Some(_))=>{},
                                Ok(None)|Err(_)=>break,
                            }
                        },
                        _=heartbeat.tick()=>{
                            let alive=tokio::select! {
                                _=cancel.cancelled()=>false,
                                result=tokio::time::timeout(CONNECT_TIMEOUT,sqlx::query("SELECT 1").execute(&mut listener))=>result.is_ok_and(|result|result.is_ok()),
                            };
                            if !alive {break;}
                        },
                    }
                }
                health.listen.store(false, Ordering::Release);
                drop(listener);
            }
            // Dropping PgListener unregisters LISTEN on its own task. Bound the
            // close wait as well, so a dead peer cannot delay service shutdown.
            let _ = tokio::time::timeout(CONNECT_TIMEOUT, pool.close()).await;
            if cancel.is_cancelled() {
                return;
            }
            metrics.listen_reconnects.fetch_add(1, Ordering::Relaxed);
            // v7's leading bytes encode time, so draw jitter independently
            // from the CSPRNG instead of extracting any UUID prefix.
            let jitter = rand::random::<u8>();
            let delay = retry_delay(self.retry, failures, jitter);
            failures = failures.saturating_add(1);
            tokio::select! {_=cancel.cancelled()=>return,_=tokio::time::sleep(delay)=>{}}
        }
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }
}

fn retry_delay(base: Duration, failures: u32, jitter: u8) -> Duration {
    let scaled = base.as_millis().saturating_mul(1_u128 << failures.min(5));
    // 80%-120% jitter breaks synchronized reconnects; absolute cap stays 30s.
    Duration::from_millis(
        (scaled.saturating_mul(80 + u128::from(jitter) % 41) / 100).clamp(1, 30_000) as u64,
    )
}

struct Record {
    id: Uuid,
    user_id: Uuid,
    tenant_id: Option<Uuid>,
    organization_id: Option<Uuid>,
    recipient_membership_id: Option<Uuid>,
    recipient_member_type: Option<String>,
    recipient_identity_version: Option<i64>,
    source_kind: String,
    application_id: Option<Uuid>,
    publication_id: Option<Uuid>,
    kind: String,
    title: String,
    content: Option<String>,
    link: Option<String>,
    created_at: DateTime<Utc>,
}

impl Record {
    fn notification(self, hint: &Hint) -> Option<StoredDelivery> {
        if !identifiers::valid(self.id)
            || !identifiers::valid(self.user_id)
            || self.tenant_id.is_none_or(|id| !identifiers::valid(id))
            || self
                .organization_id
                .is_none_or(|id| !identifiers::valid(id))
            || self
                .application_id
                .is_some_and(|id| !identifiers::valid(id))
            || self
                .publication_id
                .is_some_and(|id| !identifiers::valid(id))
            || self.id != hint.id
            || self.user_id != hint.identity.user_id
            || self.tenant_id != Some(hint.identity.tenant_id)
            || self.organization_id != Some(hint.identity.organization_id)
            || self.recipient_membership_id != Some(hint.identity.membership_id)
            || self
                .recipient_member_type
                .as_deref()
                .and_then(MemberType::parse)
                != Some(hint.identity.member_type)
            || self.recipient_identity_version != Some(hint.identity.identity_version)
            || self.title.is_empty()
            || !matches!(
                self.kind.as_str(),
                "INFO" | "SUCCESS" | "WARNING" | "SECURITY"
            )
        {
            return None;
        }
        match self.source_kind.as_str() {
            "APP" if self.application_id.is_some() && self.publication_id.is_some() => {}
            "SYSTEM" if self.application_id.is_none() && self.publication_id.is_none() => {}
            _ => return None,
        }
        let notification = Notification {
            contract_version: "1.0",
            id: self.id,
            user_id: self.user_id,
            tenant_id: hint.identity.tenant_id,
            organization_id: hint.identity.organization_id,
            application_id: self.application_id,
            publication_id: self.publication_id,
            kind: self.kind,
            title: self.title,
            content: self.content.unwrap_or_default(),
            link: self.link.unwrap_or_default(),
            created_at: self.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        };
        Some(StoredDelivery {
            recipient: hint.identity,
            notification,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(value: u128) -> Uuid {
        Uuid::from_u128(0x0196_0000_0000_7000_8000_0000_0000_0000 | value)
    }
    fn hint() -> Hint {
        Hint {
            id: id(1),
            identity: Identity {
                user_id: id(2),
                tenant_id: id(3),
                organization_id: id(4),
                membership_id: id(5),
                member_type: MemberType::Employee,
                identity_version: 1,
            },
        }
    }
    fn record() -> Record {
        let hint = hint();
        Record {
            id: hint.id,
            user_id: hint.identity.user_id,
            tenant_id: Some(hint.identity.tenant_id),
            organization_id: Some(hint.identity.organization_id),
            recipient_membership_id: Some(hint.identity.membership_id),
            recipient_member_type: Some(hint.identity.member_type.as_str().into()),
            recipient_identity_version: Some(hint.identity.identity_version),
            source_kind: "SYSTEM".into(),
            application_id: None,
            publication_id: None,
            kind: "INFO".into(),
            title: "Persisted notification".into(),
            content: None,
            link: None,
            created_at: DateTime::from_timestamp(1_900_000_000, 0).unwrap(),
        }
    }
    #[test]
    fn persisted_rows_require_exact_owner_source_and_publication_shape() {
        let event = record().notification(&hint()).unwrap();
        assert_eq!(event.recipient, hint().identity);
        assert_eq!(event.notification.content, "");
        let mut wrong = record();
        wrong.tenant_id = Some(id(99));
        assert!(wrong.notification(&hint()).is_none());
        let mut legacy = record();
        legacy.source_kind = "LEGACY".into();
        assert!(legacy.notification(&hint()).is_none());
        let mut app = record();
        app.source_kind = "APP".into();
        assert!(app.notification(&hint()).is_none());
        let mut system = record();
        system.application_id = Some(id(99));
        assert!(system.notification(&hint()).is_none());
        let mut kind = record();
        kind.kind = "UNKNOWN".into();
        assert!(kind.notification(&hint()).is_none());
        let mut app = record();
        app.source_kind = "APP".into();
        app.application_id = Some(id(8));
        app.publication_id = Some(id(9));
        assert!(app.notification(&hint()).is_some());
        let mut app = record();
        app.source_kind = "APP".into();
        app.application_id = Some(id(8));
        // Explicit v4 persisted-row negative: hint IDs are already valid v7.
        app.publication_id = Some(Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap());
        assert!(app.notification(&hint()).is_none());
    }
    #[test]
    fn archived_rows_and_another_membership_or_generation_never_match_a_hint() {
        let mut legacy = record();
        legacy.recipient_membership_id = None;
        legacy.recipient_member_type = None;
        legacy.recipient_identity_version = None;
        assert!(legacy.notification(&hint()).is_none());
        let mut wrong = record();
        wrong.recipient_membership_id = Some(id(99));
        assert!(wrong.notification(&hint()).is_none());
        let mut wrong = record();
        wrong.recipient_member_type = Some("CUSTOMER".into());
        assert!(wrong.notification(&hint()).is_none());
        let mut wrong = record();
        wrong.recipient_identity_version = Some(2);
        assert!(wrong.notification(&hint()).is_none());
        let mut wrong = record();
        wrong.recipient_membership_id =
            Some(Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap());
        assert!(wrong.notification(&hint()).is_none());
    }
    #[test]
    fn retry_is_jittered_exponential_and_absolutely_bounded() {
        assert_eq!(
            retry_delay(Duration::from_secs(1), 0, 0),
            Duration::from_millis(800)
        );
        assert_eq!(
            retry_delay(Duration::from_secs(1), 0, 40),
            Duration::from_millis(1200)
        );
        assert!(
            retry_delay(Duration::from_secs(1), 4, 0) > retry_delay(Duration::from_secs(1), 0, 0)
        );
        assert_eq!(
            retry_delay(Duration::from_secs(30), u32::MAX, 40),
            Duration::from_secs(30)
        );
    }
    #[test]
    fn notification_dto_exposes_only_the_versioned_public_fields() {
        let value =
            serde_json::to_value(record().notification(&hint()).unwrap().notification).unwrap();
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
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
        assert!(value.get("sourceKind").is_none());
        assert!(value.get("isRead").is_none());
        assert!(value.get("sessionId").is_none());
        assert_eq!(value["contractVersion"], "1.0");
    }
}
