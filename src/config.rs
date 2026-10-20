use axum::http::HeaderValue;
use reqwest::Url;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    net::IpAddr,
    time::Duration,
};

/// Explicit process configuration. Secrets deliberately have no Debug implementation.
pub struct Config {
    pub(crate) production: bool,
    pub(crate) database_url: String,
    pub(crate) platform_api_base_url: Url,
    pub(crate) platform_ca_pem: Option<Vec<u8>>,
    pub(crate) allowed_origins: Vec<HeaderValue>,
    pub(crate) host: IpAddr,
    pub(crate) port: u16,
    pub(crate) socket_path: String,
    pub(crate) auth_timeout: Duration,
    pub(crate) recheck: Duration,
    pub(crate) auth_wait: Duration,
    pub(crate) health_probe: Duration,
    pub(crate) db_retry: Duration,
    pub(crate) metrics_token: Option<String>,
    pub(crate) max_connections: usize,
    pub(crate) max_partitions: usize,
    pub(crate) per_partition_queue: usize,
    pub(crate) total_queue: usize,
    pub(crate) max_inflight: usize,
    pub(crate) validation_concurrency: usize,
}

/// Invalid deployment setting, with no configuration value or secret in its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(&'static str);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for ConfigError {}

impl Config {
    /// Load only explicit environment settings; no `.env` or platform JWT key is read.
    pub fn from_env() -> Result<Self, ConfigError> {
        if std::env::var_os("JWT_SECRET").is_some() {
            return Err(ConfigError(
                "JWT_SECRET must not be supplied to notification service",
            ));
        }
        let mut values = HashMap::new();
        for name in [
            "DATABASE_URL",
            "PLATFORM_API_BASE_URL",
            "NOTIFICATION_ALLOWED_ORIGINS",
            "NOTIFICATION_ENV",
            "NODE_ENV",
            "NOTIFICATION_HOST",
            "NOTIFICATION_PORT",
            "NOTIFICATION_SOCKET_PATH",
            "NOTIFICATION_AUTH_TIMEOUT_MS",
            "NOTIFICATION_AUTH_RECHECK_MS",
            "NOTIFICATION_AUTH_WAIT_MS",
            "NOTIFICATION_HEALTH_PROBE_MS",
            "NOTIFICATION_DATABASE_RETRY_MS",
            "METRICS_TOKEN",
            "NOTIFICATION_PLATFORM_CA_FILE",
            "NOTIFICATION_TOTAL_QUEUE",
            "NOTIFICATION_PER_PARTITION_QUEUE",
            "NOTIFICATION_MAX_CONNECTIONS",
            "NOTIFICATION_MAX_PARTITIONS",
            "NOTIFICATION_MAX_INFLIGHT",
            "NOTIFICATION_VALIDATION_CONCURRENCY",
        ] {
            if let Some(value) = std::env::var_os(name) {
                values.insert(
                    name.to_owned(),
                    value
                        .into_string()
                        .map_err(|_| ConfigError("configuration must be Unicode"))?,
                );
            }
        }
        Self::from_map(&values)
    }

    /// Validate a supplied configuration map, independently of ambient process state.
    /// Optional CA files are read during startup configuration only, before serving IO.
    pub fn from_map(env: &HashMap<String, String>) -> Result<Self, ConfigError> {
        if env.contains_key("JWT_SECRET") {
            return Err(ConfigError(
                "JWT_SECRET must not be supplied to notification service",
            ));
        }
        let environment = env
            .get("NOTIFICATION_ENV")
            .or_else(|| env.get("NODE_ENV"))
            .map(String::as_str)
            .unwrap_or("development");
        if !matches!(environment, "production" | "development" | "test") {
            return Err(ConfigError(
                "NOTIFICATION_ENV must be production, development, or test",
            ));
        }
        let production = environment == "production";
        let database_url = env
            .get("DATABASE_URL")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or(ConfigError("DATABASE_URL is required"))?;
        let database = Url::parse(database_url)
            .map_err(|_| ConfigError("DATABASE_URL must be a PostgreSQL URL"))?;
        if !matches!(database.scheme(), "postgres" | "postgresql") || database.host_str().is_none()
        {
            return Err(ConfigError("DATABASE_URL must be a PostgreSQL URL"));
        }
        let platform_raw = env
            .get("PLATFORM_API_BASE_URL")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or(ConfigError("PLATFORM_API_BASE_URL is required"))?;
        let platform_api_base_url = origin(platform_raw, production).map_err(|_|ConfigError("PLATFORM_API_BASE_URL requires an exact HTTPS origin (development permits loopback HTTP)"))?;
        let raw_origins = env
            .get("NOTIFICATION_ALLOWED_ORIGINS")
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or(ConfigError("NOTIFICATION_ALLOWED_ORIGINS is required"))?;
        let mut allowed_origins = Vec::new();
        for raw in raw_origins.split(',').map(str::trim) {
            origin(raw, production).map_err(|_|ConfigError("NOTIFICATION_ALLOWED_ORIGINS requires exact HTTPS origins (development permits loopback HTTP)"))?;
            let header = HeaderValue::from_str(raw).map_err(|_| {
                ConfigError("NOTIFICATION_ALLOWED_ORIGINS contains an invalid header")
            })?;
            if !allowed_origins.contains(&header) {
                allowed_origins.push(header);
            }
        }
        if allowed_origins.len() > 32 {
            return Err(ConfigError(
                "NOTIFICATION_ALLOWED_ORIGINS permits at most 32 origins",
            ));
        }
        let host = env
            .get("NOTIFICATION_HOST")
            .map(String::as_str)
            .unwrap_or("0.0.0.0")
            .parse()
            .map_err(|_| ConfigError("NOTIFICATION_HOST must be an IP bind address"))?;
        let socket_path = env
            .get("NOTIFICATION_SOCKET_PATH")
            .cloned()
            .unwrap_or_else(|| "/socket.io/".into());
        if !valid_path(&socket_path) {
            return Err(ConfigError(
                "NOTIFICATION_SOCKET_PATH must be an explicit absolute path ending with /",
            ));
        }
        let metrics_token = match env.get("METRICS_TOKEN") {
            None if !production => None,
            Some(token) if valid_metrics_token(token) => Some(token.clone()),
            _ => return Err(ConfigError("METRICS_TOKEN requires 32 to 256 random URL-safe characters; placeholders are rejected")),
        };
        let platform_ca_pem = match env.get("NOTIFICATION_PLATFORM_CA_FILE") {
            None => None,
            Some(path) => {
                let mut file = std::fs::File::open(path)
                    .map_err(|_| ConfigError("NOTIFICATION_PLATFORM_CA_FILE could not be read"))?;
                let mut pem = Vec::new();
                std::io::Read::read_to_end(
                    &mut std::io::Read::take(&mut file, 1_048_577),
                    &mut pem,
                )
                .map_err(|_| ConfigError("NOTIFICATION_PLATFORM_CA_FILE could not be read"))?;
                if pem.is_empty() || pem.len() > 1_048_576 {
                    return Err(ConfigError("NOTIFICATION_PLATFORM_CA_FILE must contain at most 1 MiB of PEM certificates"));
                }
                Some(pem)
            }
        };
        let total_queue = integer(env, "NOTIFICATION_TOTAL_QUEUE", 1000, 1, 65536)? as usize;
        let per_partition_queue =
            integer(env, "NOTIFICATION_PER_PARTITION_QUEUE", 32, 1, 4096)? as usize;
        if per_partition_queue > total_queue {
            return Err(ConfigError(
                "NOTIFICATION_PER_PARTITION_QUEUE must not exceed NOTIFICATION_TOTAL_QUEUE",
            ));
        }
        let max_connections = integer(env, "NOTIFICATION_MAX_CONNECTIONS", 256, 1, 4096)? as usize;
        let validation_concurrency =
            integer(env, "NOTIFICATION_VALIDATION_CONCURRENCY", 16, 1, 256)? as usize;
        if validation_concurrency > max_connections {
            return Err(ConfigError(
                "NOTIFICATION_VALIDATION_CONCURRENCY must not exceed NOTIFICATION_MAX_CONNECTIONS",
            ));
        }
        Ok(Self {
            production,
            database_url: database_url.to_owned(),
            platform_api_base_url,
            platform_ca_pem,
            allowed_origins,
            host,
            port: integer(
                env,
                "NOTIFICATION_PORT",
                3003,
                if production { 1 } else { 0 },
                65535,
            )? as u16,
            socket_path,
            auth_timeout: Duration::from_millis(integer(
                env,
                "NOTIFICATION_AUTH_TIMEOUT_MS",
                2000,
                100,
                10000,
            )?),
            recheck: Duration::from_millis(integer(
                env,
                "NOTIFICATION_AUTH_RECHECK_MS",
                30000,
                1000,
                300000,
            )?),
            auth_wait: Duration::from_millis(integer(
                env,
                "NOTIFICATION_AUTH_WAIT_MS",
                10000,
                100,
                60000,
            )?),
            health_probe: Duration::from_millis(integer(
                env,
                "NOTIFICATION_HEALTH_PROBE_MS",
                5000,
                1000,
                60000,
            )?),
            db_retry: Duration::from_millis(integer(
                env,
                "NOTIFICATION_DATABASE_RETRY_MS",
                1000,
                100,
                30000,
            )?),
            metrics_token,
            total_queue,
            per_partition_queue,
            max_connections,
            max_partitions: integer(env, "NOTIFICATION_MAX_PARTITIONS", 128, 1, 4096)? as usize,
            max_inflight: integer(env, "NOTIFICATION_MAX_INFLIGHT", 8, 1, 256)? as usize,
            validation_concurrency,
        })
    }
}

fn integer(
    env: &HashMap<String, String>,
    name: &'static str,
    fallback: u64,
    min: u64,
    max: u64,
) -> Result<u64, ConfigError> {
    let Some(raw) = env.get(name) else {
        return Ok(fallback);
    };
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ConfigError(name));
    }
    let value = raw.parse::<u64>().map_err(|_| ConfigError(name))?;
    if !(min..=max).contains(&value) {
        return Err(ConfigError(name));
    }
    Ok(value)
}

fn origin(raw: &str, production: bool) -> Result<Url, ()> {
    let url = Url::parse(raw).map_err(|_| ())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.origin().ascii_serialization() != raw
        || raw.contains('*')
        || url.host_str().is_none()
    {
        return Err(());
    }
    if url.scheme() == "https" {
        return Ok(url);
    }
    let loopback = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    if url.scheme() == "http" && !production && loopback {
        Ok(url)
    } else {
        Err(())
    }
}

fn valid_path(path: &str) -> bool {
    path.len() > 2
        && path.len() <= 128
        && path.starts_with('/')
        && path.ends_with('/')
        && !path.contains("//")
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
        && !path.split('/').any(|part| matches!(part, "." | ".."))
}

fn valid_metrics_token(token: &str) -> bool {
    let lower = token.to_ascii_lowercase().replace('-', "");
    (32..=256).contains(&token.len())
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        && token.bytes().collect::<HashSet<_>>().len() >= 8
        && ![
            "changeme",
            "replaceme",
            "devsecret",
            "yoursecret",
            "placeholder",
        ]
        .iter()
        .any(|bad| lower.contains(bad))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> HashMap<String, String> {
        [
            (
                "DATABASE_URL",
                "postgresql://fixture@127.0.0.1:5432/fixture",
            ),
            ("PLATFORM_API_BASE_URL", "http://127.0.0.1:3001"),
            ("NOTIFICATION_ALLOWED_ORIGINS", "https://host.example.com"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }
    #[test]
    fn explicit_configuration_enforces_tls_and_rejects_platform_signing_keys() {
        let env = base();
        assert!(Config::from_map(&env).is_ok());
        for key in [
            "DATABASE_URL",
            "PLATFORM_API_BASE_URL",
            "NOTIFICATION_ALLOWED_ORIGINS",
        ] {
            let mut env = env.clone();
            env.remove(key);
            assert!(Config::from_map(&env).is_err());
        }
        for value in [
            "*",
            "null",
            "https://a/",
            "https://user:pass@a",
            "http://outside.example",
            "https://a?x=1",
        ] {
            let mut env = base();
            env.insert("NOTIFICATION_ALLOWED_ORIGINS".into(), value.into());
            assert!(Config::from_map(&env).is_err());
        }
        let mut env = base();
        env.insert("JWT_SECRET".into(), "ignored-is-still-not-allowed".into());
        assert!(Config::from_map(&env).is_err());
        env = base();
        env.insert("NOTIFICATION_ENV".into(), "production".into());
        env.insert(
            "PLATFORM_API_BASE_URL".into(),
            "https://platform.example".into(),
        );
        assert!(Config::from_map(&env).is_err());
        env.insert(
            "METRICS_TOKEN".into(),
            "FixtureOnlyMetricsToken0123456789AbCd".into(),
        );
        assert!(Config::from_map(&env).is_ok());
        env.insert("NOTIFICATION_PORT".into(), "0".into());
        assert!(Config::from_map(&env).is_err());
    }
    #[test]
    fn timers_paths_and_scheduler_budgets_are_bounded() {
        for (name, value) in [
            ("NOTIFICATION_AUTH_TIMEOUT_MS", "99"),
            ("NOTIFICATION_AUTH_RECHECK_MS", "999"),
            ("NOTIFICATION_AUTH_WAIT_MS", "NaN"),
            ("NOTIFICATION_PORT", "65536"),
            ("NOTIFICATION_MAX_INFLIGHT", "0"),
            ("NOTIFICATION_TOTAL_QUEUE", "1"),
        ] {
            let mut env = base();
            env.insert(name.into(), value.into());
            assert!(Config::from_map(&env).is_err());
        }
        for path in [
            "/",
            "/socket.io",
            "/socket//io/",
            "/socket/../io/",
            "/socket/./io/",
            "/socket.io/?x=1",
        ] {
            let mut env = base();
            env.insert("NOTIFICATION_SOCKET_PATH".into(), path.into());
            assert!(Config::from_map(&env).is_err());
        }
        let mut env = base();
        env.insert("NOTIFICATION_PORT".into(), "0".into());
        assert!(Config::from_map(&env).is_ok());
    }

    #[test]
    fn origin_canonicalization_and_validation_capacity_cannot_relax_the_boundary() {
        for value in [
            "https://HOST.example.com",
            "https://host.example.com:443",
            "http://127.0.0.2:3000",
        ] {
            let mut env = base();
            env.insert("NOTIFICATION_ALLOWED_ORIGINS".into(), value.into());
            assert!(Config::from_map(&env).is_err());
        }
        let mut env = base();
        env.insert(
            "NOTIFICATION_ALLOWED_ORIGINS".into(),
            "http://[::1]:3000,http://localhost:3000,http://localhost:3000".into(),
        );
        assert_eq!(Config::from_map(&env).unwrap().allowed_origins.len(), 2);
        env.insert("NOTIFICATION_ENV".into(), "prodution".into());
        assert!(Config::from_map(&env).is_err());
        env.remove("NOTIFICATION_ENV");
        env.insert("NOTIFICATION_MAX_CONNECTIONS".into(), "2".into());
        assert!(Config::from_map(&env).is_err());
        env.insert("NOTIFICATION_VALIDATION_CONCURRENCY".into(), "2".into());
        assert!(Config::from_map(&env).is_ok());
    }
}
