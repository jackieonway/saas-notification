use crate::{config::Config, scheduler::Snapshot};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct Health {
    pub(crate) database: AtomicBool,
    pub(crate) listen: AtomicBool,
    pub(crate) platform: AtomicBool,
}

impl Health {
    pub(crate) fn healthy(&self) -> bool {
        self.database.load(Ordering::Acquire)
            && self.listen.load(Ordering::Acquire)
            && self.platform.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot(&self) -> Value {
        let database = self.database.load(Ordering::Acquire);
        let listen = self.listen.load(Ordering::Acquire);
        let platform = self.platform.load(Ordering::Acquire);
        let state = |up| if up { "UP" } else { "DOWN" };
        json!({"status": state(database && listen && platform), "checks": {
            "database":state(database),"listen":state(listen),"platform":state(platform)
        }})
    }
}

/// Fixed series only: neither caller-selected names nor identity labels exist.
#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) connections: AtomicU64,
    pub(crate) authenticated: AtomicU64,
    pub(crate) auth_denials: AtomicU64,
    pub(crate) hints_received: AtomicU64,
    pub(crate) hints_dropped: AtomicU64,
    pub(crate) lookup_errors: AtomicU64,
    pub(crate) delivery_denied: AtomicU64,
    pub(crate) delivered: AtomicU64,
    pub(crate) validation_inflight: AtomicU64,
    pub(crate) validation_high_water: AtomicU64,
    pub(crate) validation_failures: AtomicU64,
    pub(crate) validation_count: AtomicU64,
    pub(crate) validation_micros: AtomicU64,
    pub(crate) listen_reconnects: AtomicU64,
    max_connections: AtomicU64,
    max_partitions: AtomicU64,
    per_partition_queue: AtomicU64,
    total_queue: AtomicU64,
    max_inflight: AtomicU64,
    validation_concurrency: AtomicU64,
}

impl Metrics {
    pub(crate) fn configure(&self, config: &Config) {
        self.max_connections
            .store(config.max_connections as u64, Ordering::Relaxed);
        self.max_partitions
            .store(config.max_partitions as u64, Ordering::Relaxed);
        self.per_partition_queue
            .store(config.per_partition_queue as u64, Ordering::Relaxed);
        self.total_queue
            .store(config.total_queue as u64, Ordering::Relaxed);
        self.max_inflight
            .store(config.max_inflight as u64, Ordering::Relaxed);
        self.validation_concurrency
            .store(config.validation_concurrency as u64, Ordering::Relaxed);
    }
    pub(crate) fn render(&self, scheduler: &Snapshot, health: &Health) -> String {
        let mut output = String::new();
        let mut metric = |name: &str, kind: &str, value: u64| {
            output.push_str(&format!(
                "# TYPE {name} {kind}\n{name}{{service=\"notification-ws\"}} {value}\n"
            ));
        };
        for (name, field) in [
            ("saas_notification_clients", &self.connections),
            (
                "saas_notification_authenticated_clients",
                &self.authenticated,
            ),
            (
                "saas_notification_validation_inflight",
                &self.validation_inflight,
            ),
            (
                "saas_notification_validation_high_water",
                &self.validation_high_water,
            ),
            ("saas_notification_connection_limit", &self.max_connections),
            ("saas_notification_partition_limit", &self.max_partitions),
            (
                "saas_notification_per_partition_queue_limit",
                &self.per_partition_queue,
            ),
            ("saas_notification_total_queue_limit", &self.total_queue),
            ("saas_notification_inflight_limit", &self.max_inflight),
            (
                "saas_notification_validation_limit",
                &self.validation_concurrency,
            ),
        ] {
            metric(name, "gauge", field.load(Ordering::Relaxed));
        }
        for (name, field) in [
            ("saas_notification_auth_denials_total", &self.auth_denials),
            (
                "saas_notification_hints_received_total",
                &self.hints_received,
            ),
            ("saas_notification_hint_drops_total", &self.hints_dropped),
            ("saas_notification_hint_errors_total", &self.lookup_errors),
            (
                "saas_notification_delivery_denied_total",
                &self.delivery_denied,
            ),
            ("saas_notification_delivered_total", &self.delivered),
            // Transport emission acceptance is not proof of user receipt/read.
            ("saas_notification_emit_accepted_total", &self.delivered),
            (
                "saas_notification_validation_failures_total",
                &self.validation_failures,
            ),
            (
                "saas_notification_validation_count_total",
                &self.validation_count,
            ),
            (
                "saas_notification_validation_duration_microseconds_total",
                &self.validation_micros,
            ),
            (
                "saas_notification_listen_reconnects_total",
                &self.listen_reconnects,
            ),
        ] {
            metric(name, "counter", field.load(Ordering::Relaxed));
        }
        for (name, value) in [
            ("saas_notification_pending_hints", scheduler.queued as u64),
            ("saas_notification_partitions", scheduler.partitions as u64),
            (
                "saas_notification_active_partitions",
                scheduler.active as u64,
            ),
            (
                "saas_notification_listen_up",
                u64::from(health.listen.load(Ordering::Acquire)),
            ),
            (
                "saas_database_up",
                u64::from(health.database.load(Ordering::Acquire)),
            ),
            (
                "saas_platform_up",
                u64::from(health.platform.load(Ordering::Acquire)),
            ),
            ("saas_health_up", u64::from(health.healthy())),
        ] {
            metric(name, "gauge", value);
        }
        metric(
            "saas_notification_scheduler_accepted_total",
            "counter",
            scheduler.accepted,
        );
        metric(
            "saas_notification_scheduler_dropped_total",
            "counter",
            scheduler.dropped,
        );
        metric(
            "saas_notification_scheduler_completed_total",
            "counter",
            scheduler.completed,
        );
        metric(
            "saas_notification_scheduler_failed_total",
            "counter",
            scheduler.failed,
        );
        metric(
            "saas_notification_scheduler_cancelled_total",
            "counter",
            scheduler.cancelled,
        );
        metric(
            "saas_notification_queue_wait_microseconds_total",
            "counter",
            scheduler.queue_wait_micros,
        );
        metric(
            "saas_notification_queue_wait_max_microseconds",
            "gauge",
            scheduler.max_queue_wait_micros,
        );
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn health_requires_every_dependency_and_output_has_only_static_series() {
        let health = Health::default();
        assert!(!health.healthy());
        health.database.store(true, Ordering::Release);
        health.platform.store(true, Ordering::Release);
        assert!(!health.healthy());
        health.listen.store(true, Ordering::Release);
        assert!(health.healthy());
        assert_eq!(health.snapshot()["status"], "UP");
        let snapshot = Snapshot {
            partitions: 1,
            queued: 2,
            active: 1,
            accepted: 3,
            dropped: 4,
            completed: 5,
            ..Default::default()
        };
        let output = Metrics::default().render(&snapshot, &health);
        assert!(output.contains("saas_notification_pending_hints{service=\"notification-ws\"} 2"));
        assert!(output
            .lines()
            .filter(|line| !line.starts_with('#'))
            .all(|line| line.contains("{service=\"notification-ws\"}")));
        assert!(!output.contains("tenant"));
        assert!(!output.contains("session"));
    }
}
