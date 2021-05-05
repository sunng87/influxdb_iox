//! This crate exists to coordinate versions of `opentelemetry`, `tracing`,
//! `prometheus` and related crates so that we can manage their updates in a
//! single crate.

// Export these crates publicly so we can have a single reference
pub use tracing;
pub use opentelemetry;

#[cfg(feature = "exporters")]
mod exporters {
    pub use tracing::instrument;
    pub use tracing_opentelemetry;
    pub use tracing_subscriber;
    pub use env_logger;
    pub use opentelemetry_jaeger;
    pub use opentelemetry_otlp;
    pub use opentelemetry_prometheus;
    pub use prometheus;
}

#[cfg(feature = "exporters")]
pub use exporters::*;
