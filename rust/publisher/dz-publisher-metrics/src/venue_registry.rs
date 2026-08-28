use std::collections::HashMap;

use prometheus::core::Collector;
use prometheus::proto::LabelPair;
use prometheus::Registry;

use crate::error::{MetricsError, Result};

const RESERVED_PREFIX: &str = "dz_publisher_";

/// The label names this crate applies to every series as constant labels.
/// A venue collector that carries either one produces a sample with a
/// repeated label name once `gather` adds ours, which the Prometheus text
/// parser rejects for the whole scrape.
const RESERVED_LABELS: [&str; 2] = ["venue", "source_id"];

pub(crate) fn is_reserved_name(name: &str) -> bool {
    name.starts_with(RESERVED_PREFIX)
}

/// A second registry for series specific to one publisher's own venue.
///
/// The normative set lives in [`crate::PublisherMetrics`] and is reached
/// through its typed accessors, never through this registry. This registry
/// exists for whatever a venue's own integration needs that the normative
/// set does not cover, and it refuses any collector whose fully-qualified
/// name begins with `dz_publisher_` so a venue cannot shadow, rename, or
/// otherwise reinterpret a normative series.
///
/// Series registered here carry the same `venue` and `source_id` constant
/// labels as the normative set: a venue metric that omitted them would be
/// indistinguishable from the same metric on another publisher scraped into
/// the same Prometheus, and any dashboard row selecting `{venue="..."}`
/// would silently match nothing.
pub struct VenueRegistry {
    registry: Registry,
}

impl VenueRegistry {
    pub(crate) fn new(const_labels: &HashMap<String, String>) -> Self {
        Self {
            registry: Registry::new_custom(None, Some(const_labels.clone()))
                .expect("a registry with valid constant labels"),
        }
    }

    /// Registers a venue-specific collector.
    ///
    /// # Errors
    ///
    /// Returns [`MetricsError::ReservedNamePrefix`] if any metric the
    /// collector describes begins with `dz_publisher_`,
    /// [`MetricsError::ReservedLabelName`] if it carries `venue` or
    /// `source_id` as a label of its own, and [`MetricsError::Prometheus`]
    /// if the underlying registration fails (for example, a duplicate
    /// descriptor).
    pub fn register(&self, collector: Box<dyn Collector>) -> Result<()> {
        for desc in collector.desc() {
            if is_reserved_name(&desc.fq_name) {
                return Err(MetricsError::ReservedNamePrefix(desc.fq_name.clone()));
            }
            // This registry applies venue and source_id as constant labels.
            // A collector carrying either name itself renders as
            // `venue_widget_total{venue="other",venue="ours"}`, and the
            // text parser rejects a sample with a repeated label name -
            // failing the entire scrape, not just this series.
            let own_labels = desc
                .variable_labels
                .iter()
                .map(String::as_str)
                .chain(desc.const_label_pairs.iter().map(LabelPair::name));
            for label in own_labels {
                if RESERVED_LABELS.contains(&label) {
                    return Err(MetricsError::ReservedLabelName {
                        metric: desc.fq_name.clone(),
                        label: label.to_string(),
                    });
                }
            }
        }
        self.registry.register(collector)?;
        Ok(())
    }

    pub(crate) fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}
