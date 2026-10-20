use crate::{
    config::Config,
    metrics::Metrics,
    protocol::{Context, ContextError},
};
use chrono::Utc;
use reqwest::{header, Client, Response, StatusCode, Url};
use std::{
    error::Error,
    fmt,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

const MAX_RESPONSE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValidationError {
    SessionInvalid,
    DeliveryDenied,
    Unavailable,
    Contract,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Platform session validation failed")
    }
}
impl Error for ValidationError {}

#[derive(Clone)]
pub(crate) struct Platform {
    client: Client,
    context_url: Url,
    deadline: Duration,
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl Platform {
    pub(crate) fn new(config: &Config, metrics: Arc<Metrics>) -> Result<Self, ValidationError> {
        metrics.configure(config);
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .https_only(config.production)
            .connect_timeout(config.auth_timeout)
            .timeout(config.auth_timeout);
        if let Some(pem) = &config.platform_ca_pem {
            let certificates = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|_| ValidationError::Contract)?;
            if certificates.is_empty() {
                return Err(ValidationError::Contract);
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build().map_err(|_| ValidationError::Unavailable)?;
        let context_url = config
            .platform_api_base_url
            .join("/api/notifications/socket-context")
            .map_err(|_| ValidationError::Contract)?;
        Ok(Self {
            client,
            context_url,
            deadline: config.auth_timeout,
            permits: Arc::new(Semaphore::new(config.validation_concurrency)),
            metrics,
        })
    }

    pub(crate) async fn validate(
        &self,
        token: &str,
        notification_id: Option<Uuid>,
        request_id: Option<Uuid>,
    ) -> Result<Context, ValidationError> {
        let started = Instant::now();
        self.metrics
            .validation_count
            .fetch_add(1, Ordering::Relaxed);
        let result = tokio::time::timeout(self.deadline, async {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| ValidationError::Unavailable)?;
            let _inflight = Inflight::new(&self.metrics);
            if token.is_empty()
                || token.len() > 4096
                || token.chars().any(|c| c.is_whitespace() || c.is_control())
            {
                return Err(ValidationError::SessionInvalid);
            }
            let mut url = self.context_url.clone();
            if let Some(id) = notification_id {
                url.query_pairs_mut()
                    .append_pair("notificationId", &id.to_string());
            }
            let authorization = header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| ValidationError::SessionInvalid)?;
            let mut request = self
                .client
                .get(url)
                .header(header::ACCEPT, "application/json")
                .header(header::AUTHORIZATION, authorization);
            if let Some(id) = request_id {
                request = request.header("X-Request-Id", id.to_string());
            }
            let response = request
                .send()
                .await
                .map_err(|_| ValidationError::Unavailable)?;
            if response.status() != StatusCode::OK {
                return Err(classify_status(
                    response.status(),
                    notification_id.is_some(),
                ));
            }
            let value = read_json(response).await?;
            Context::parse(&value, Utc::now()).map_err(|error| match error {
                ContextError::Expired => ValidationError::SessionInvalid,
                ContextError::Invalid => ValidationError::Contract,
            })
        })
        .await
        .unwrap_or(Err(ValidationError::Unavailable));
        self.record(started, result.is_err());
        result
    }

    pub(crate) async fn probe(&self) -> bool {
        let started = Instant::now();
        self.metrics
            .validation_count
            .fetch_add(1, Ordering::Relaxed);
        let result = tokio::time::timeout(self.deadline, async {
            let Ok(_permit) = self.permits.acquire().await else {
                return false;
            };
            let _inflight = Inflight::new(&self.metrics);
            self.client
                .get(self.context_url.clone())
                .header(header::ACCEPT, "application/json")
                .send()
                .await
                .is_ok_and(|response| response.status() == StatusCode::UNAUTHORIZED)
        })
        .await
        .unwrap_or(false);
        self.record(started, !result);
        result
    }

    fn record(&self, started: Instant, failed: bool) {
        self.metrics.validation_micros.fetch_add(
            started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        if failed {
            self.metrics
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct Inflight<'a>(&'a Metrics);
impl<'a> Inflight<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        let inflight = metrics.validation_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        metrics
            .validation_high_water
            .fetch_max(inflight, Ordering::Relaxed);
        Self(metrics)
    }
}
impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        self.0.validation_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

fn classify_status(status: StatusCode, delivery: bool) -> ValidationError {
    if delivery && matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND) {
        ValidationError::DeliveryDenied
    } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        ValidationError::SessionInvalid
    } else {
        ValidationError::Unavailable
    }
}

async fn read_json(mut response: Response) -> Result<serde_json::Value, ValidationError> {
    let announced_length = response.headers().get(header::CONTENT_LENGTH).map(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    });
    if announced_length
        .is_some_and(|length| length.is_none_or(|length| length > MAX_RESPONSE_BYTES as u64))
        || response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        || !response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("application/json"))
            })
    {
        return Err(ValidationError::Contract);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ValidationError::Unavailable)?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(ValidationError::Contract);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| ValidationError::Contract)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delivery_denials_do_not_reclassify_the_whole_session() {
        assert_eq!(
            classify_status(StatusCode::UNAUTHORIZED, true),
            ValidationError::SessionInvalid
        );
        assert_eq!(
            classify_status(StatusCode::FORBIDDEN, true),
            ValidationError::DeliveryDenied
        );
        assert_eq!(
            classify_status(StatusCode::NOT_FOUND, true),
            ValidationError::DeliveryDenied
        );
        assert_eq!(
            classify_status(StatusCode::FORBIDDEN, false),
            ValidationError::SessionInvalid
        );
        assert_eq!(
            classify_status(StatusCode::FOUND, false),
            ValidationError::Unavailable
        );
        assert_eq!(
            classify_status(StatusCode::INTERNAL_SERVER_ERROR, true),
            ValidationError::Unavailable
        );
    }
    #[test]
    fn inflight_guard_releases_counters_on_cancellation_and_preserves_high_water() {
        let metrics = Metrics::default();
        let guard = Inflight::new(&metrics);
        {
            let _second = Inflight::new(&metrics);
            assert_eq!(metrics.validation_high_water.load(Ordering::Relaxed), 2);
        }
        drop(guard);
        assert_eq!(metrics.validation_inflight.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.validation_high_water.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn platform_body_enforces_media_type_declared_size_and_actual_size() {
        let response = |body: String, media: Option<&str>, length: Option<&str>| {
            let mut builder = axum::http::Response::builder();
            if let Some(media) = media {
                builder = builder.header(header::CONTENT_TYPE, media);
            }
            if let Some(length) = length {
                builder = builder.header(header::CONTENT_LENGTH, length);
            }
            Response::from(builder.body(body).unwrap())
        };
        let body = format!("{{}}{}", " ".repeat(MAX_RESPONSE_BYTES - 2));
        assert!(read_json(response(
            body.clone(),
            Some("Application/JSON; charset=utf-8"),
            None
        ))
        .await
        .is_ok());
        assert_eq!(
            read_json(response(format!("{body} "), Some("application/json"), None))
                .await
                .err(),
            Some(ValidationError::Contract)
        );
        assert_eq!(
            read_json(response(
                "{}".into(),
                Some("application/json"),
                Some("4097")
            ))
            .await
            .err(),
            Some(ValidationError::Contract)
        );
        assert_eq!(
            read_json(response("{}".into(), Some("text/html"), None))
                .await
                .err(),
            Some(ValidationError::Contract)
        );
        assert_eq!(
            read_json(response("{}".into(), None, None)).await.err(),
            Some(ValidationError::Contract)
        );
        assert_eq!(
            read_json(response("not-json".into(), Some("application/json"), None))
                .await
                .err(),
            Some(ValidationError::Contract)
        );
    }
}
