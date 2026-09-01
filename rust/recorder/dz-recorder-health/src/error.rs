//! What building the health tier can refuse.
//!
//! Every variant here is a construction-time misconfiguration. There is no
//! error on the per-datagram path: a datagram this tier cannot read is counted,
//! not returned, because the observer runs on the drain thread and a `Result`
//! there would be a decision the drain thread has to stop and make.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HealthError {
    /// The feed was not among the ones the metric set was built with, so none
    /// of its series were pre-created. Refused rather than accepted: an
    /// observer on an undeclared feed emits series that first appear after the
    /// traffic they describe, which is exactly the failure pre-creation exists
    /// to prevent.
    #[error(
        "no feed named `{feed}` was declared to the metric set, so none of its series exist yet"
    )]
    UnknownFeed { feed: String },
    /// A map that can hold nothing tracks nothing, and would report a clean
    /// feed of no datagrams.
    #[error("max_instances must be at least 1")]
    NoInstanceBudget,
}
