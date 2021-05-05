//! This crate exists to coordinate versions of `opentelemetry`, `tracing` and `prometheus`
//! so that we can manage their updates in a single crate.

// Export these crates publicly so we can have a single reference
pub use opentelemetry;

pub use tracing;
pub use tracing::instrument;

// These are somewhat unfortunate but are used for testing
pub use opentelemetry_prometheus;
pub use prometheus;
