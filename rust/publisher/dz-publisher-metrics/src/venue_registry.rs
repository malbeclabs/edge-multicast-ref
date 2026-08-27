use prometheus::core::Collector;
use prometheus::Registry;

use crate::error::{MetricsError, Result};

const RESERVED_PREFIX: &str = "dz_publisher_";

/// A second registry for series specific to one publisher's own venue.
///
/// The normative set lives in [`crate::PublisherMetrics`] and is reached
/// through its typed accessors, never through this registry. This registry
/// exists for whatever a venue's own integration needs that the normative
/// set does not cover, and it refuses any collector whose fully-qualified
/// name begins with `dz_publisher_` so a venue cannot shadow, rename, or
/// otherwise reinterpret a normative series.
pub struct VenueRegistry {
    registry: Registry,
}

impl VenueRegistry {
    pub(crate) fn new() -> Self {
        Self {
            registry: Registry::new(),
        }
    }

    /// Registers a venue-specific collector.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::ReservedNamePrefix`] if any metric the
    /// collector describes begins with `dz_publisher_`, and
    /// [`MetricsError::Prometheus`] if the underlying registration fails
    /// (for example, a duplicate descriptor).
    pub fn register(&self, collector: Box<dyn Collector>) -> Result<()> {
        for desc in collector.desc() {
            if desc.fq_name.starts_with(RESERVED_PREFIX) {
                return Err(MetricsError::ReservedNamePrefix(desc.fq_name.clone()));
            }
        }
        self.registry.register(collector)?;
        Ok(())
    }

    pub(crate) fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}
