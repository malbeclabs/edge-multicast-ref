//! The types every other recorder crate speaks.
//!
//! This crate decodes nothing. It links `dz-edge-core` for [`PortRole`] and
//! `MAX_DATAGRAM_SIZE` and for nothing else, because a decoder anywhere in the
//! record path is a message the archive never holds: the evidence needed to
//! diagnose the bug is what the bug destroyed.
//!
//! It also owns the rules two tiers must agree about: [`SequenceTracker`] and
//! [`CaptureDropScope`] are here, beside the [`ChannelInstance`] they are keyed
//! on and the [`drop_delta`] they qualify, so that the live health tier and the
//! offline analysis tier share one implementation instead of holding a copy
//! each and a test to keep the copies honest.
//!
//! [`PortRole`]: dz_edge_core::PortRole
//! [`drop_delta`]: RecordedDatagram::drop_delta
#![forbid(unsafe_code)]

pub mod config;
pub mod datagram;
pub mod error;
pub mod identity;
pub mod sequence;
pub mod traits;

pub use config::{
    ArchiveConfig, CaptureConfig, CaptureMode, Compression, ConfigError, FeedConfig, HealthConfig,
    MetricsConfig, RecorderConfig, ETHERNET_IPV4_UDP_HEADER_SIZE, MAX_LINK_HEADER_SIZE,
};
pub use datagram::{CaptureDropScope, ChannelInstance, RecordedDatagram, RecvTsKind};
pub use error::{SinkError, SourceError};
pub use identity::RecorderIdentity;
pub use sequence::{SequenceOutcome, SequenceTracker, MAX_FORWARD_JUMP, REORDER_WINDOW};
pub use traits::{CompletedSegment, Observer, Sink, Source};
