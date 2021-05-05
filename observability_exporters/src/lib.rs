//! This crate exists to coordinate versions of `opentelemetry`, `prometheus` and `tracing`
//! exporters so that we can manage their updates in a single crate.

// Export these crates publicly so we can have a single reference
pub use env_logger;
pub use observability_deps::*;
pub use opentelemetry_jaeger;
pub use opentelemetry_otlp;
pub use tracing_opentelemetry;
pub use tracing_subscriber;
