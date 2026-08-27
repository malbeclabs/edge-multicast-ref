/// Errors this crate can produce.
#[derive(thiserror::Error, Debug)]
pub enum MetricsError {
    /// A venue tried to register a series under the reserved `dz_publisher_`
    /// prefix. That prefix is the normative contract; a venue-specific series
    /// must not shadow it.
    #[error("venue metric name {0:?} begins with the reserved `dz_publisher_` prefix")]
    ReservedNamePrefix(String),

    /// The underlying Prometheus registration failed (e.g. a duplicate
    /// descriptor).
    #[error(transparent)]
    Prometheus(#[from] prometheus::Error),
}

pub type Result<T> = std::result::Result<T, MetricsError>;
