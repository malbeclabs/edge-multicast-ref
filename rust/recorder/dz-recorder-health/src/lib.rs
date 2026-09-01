//! The health tier: an `Observer` that reads the 24-byte datagram header and
//! nothing else.
//!
//! It exists because a pure archive-and-forget recorder is undeployable. If the
//! only output is objects in a bucket, a recorder whose socket died, whose
//! interface flapped, whose disk filled or whose group membership lapsed looks
//! exactly like a quiet feed until somebody reads the bucket. Feed health is a
//! minutes-scale question and an archive is an hours-scale answer.
//!
//! It is affordable precisely because it is feed-agnostic: continuity,
//! reordering, duplication, reset accounting, send-to-receive latency, heartbeat
//! cadence and the size cap are all decidable from the header alone, with no
//! per-feed crate linked in and no per-instrument state. Anything needing a
//! message walk, an instrument's own sequence, a book or reference data is
//! offline work, permanently.
//!
//! The sequence rules it decides on are
//! [`SequenceTracker`](dz_recorder_core::SequenceTracker)'s, in
//! `dz-recorder-core` and not here: the offline analysis tier decides on the
//! same rules, and it must be able to reach them without linking a metrics
//! registry and a Prometheus exposition into a loader.
#![forbid(unsafe_code)]

pub mod error;
pub mod instance;
pub mod metrics;
pub mod observer;

pub use error::HealthError;
pub use instance::SourceLabel;
pub use metrics::{
    DeclaredLengthMismatch, DeclaredLengthViolation, FeedSeries, HealthMetrics,
    HealthMetricsConfig, LatencyDropReason, RecvTimestampKind, UnreadableReason,
    HEARTBEAT_INTERVAL_BUCKETS, OTHER_VALUE, SEND_TO_RECV_BUCKETS,
};
pub use observer::{CaptureDeltas, HealthObserver, InstanceLimits};
