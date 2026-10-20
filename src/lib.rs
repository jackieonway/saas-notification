//! Bounded notification delivery. PostgreSQL inbox rows remain authoritative.
mod config;
mod metrics;
mod platform;
mod protocol;
mod scheduler;
mod server;
mod store;
mod transport;

pub use server::run_from_env;
