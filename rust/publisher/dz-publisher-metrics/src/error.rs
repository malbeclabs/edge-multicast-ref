/// Errors this crate can produce.
#[derive(thiserror::Error, Debug)]
pub enum MetricsError {
    /// A venue tried to register a series under the reserved `dz_publisher_`
    /// prefix. That prefix is the normative contract; a venue-specific series
    /// must not shadow it.
    #[error("venue metric name {0:?} begins with the reserved `dz_publisher_` prefix")]
    ReservedNamePrefix(String),

    /// A venue tried to register a series carrying `venue` or `source_id`
    /// as a label of its own. This crate applies both as constant labels,
    /// so the series would render with a repeated label name and the
    /// Prometheus text parser would reject the whole scrape.
    #[error("venue metric {metric:?} carries the reserved label name {label:?}")]
    ReservedLabelName { metric: String, label: String },

    /// The underlying Prometheus registration failed (e.g. a duplicate
    /// descriptor).
    #[error(transparent)]
    Prometheus(#[from] prometheus::Error),
}

pub type Result<T> = std::result::Result<T, MetricsError>;
