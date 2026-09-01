//! The archive: pcapng, one Enhanced Packet Block per datagram, rotated,
//! compressed, hashed and manifested.
//!
//! Two rules govern everything here. The archive is **self-describing** — the
//! recorder's identity, its configuration and its own drop counts live in
//! pcapng blocks, not in a sidecar that will one day not travel with the
//! object. And the write path **never blocks**: when staging fills, the oldest
//! segment is deleted and counted, because losing bounded history is
//! recoverable and contaminating live data is not.
#![forbid(unsafe_code)]

pub mod compress;
pub mod manifest;
pub mod object_key;
pub mod rotate;
pub mod staging;
pub mod writer;

pub use compress::{Compression, Published};
pub use manifest::{CoverageTracker, InstanceCoverage, JoinedRole, SegmentManifest};
pub use rotate::{ArchiveWriter, ArchiveWriterConfig};
pub use staging::{SegmentObject, StagingWatermark};
pub use writer::{
    role_index, CaptureDropScope, LinkHeaders, RoleJoin, SegmentStats, SegmentWriter,
    SegmentWriterConfig, ALL_ROLES, LINK_HEADER_LEN, MAX_LINK_HEADER_LEN,
};
