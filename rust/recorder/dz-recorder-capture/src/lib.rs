//! Live capture as a [`Source`](dz_recorder_core::Source).
//!
//! Two modes behind the one trait. `AF_PACKET` on the arrival interface is the
//! default, because it records what the network delivered rather than what one
//! socket survived. Socket mode is the fallback where `CAP_NET_RAW` is
//! unavailable; it synthesises the IP and UDP headers and records the fact, so
//! that no reader mistakes a synthesised field for a captured one.
//!
//! Both modes report their own losses. Without that, every gap we caused is
//! charged to the publisher.
//!
//! `AF_PACKET` mode is behind the `afpacket` feature, because it needs
//! `libpcap-dev` at build time and everything that does not touch it must still
//! build and test where that is absent.
#![forbid(unsafe_code)]

#[cfg(feature = "afpacket")]
pub mod afpacket;
pub mod device;
pub mod rejoin;
pub mod socket;

pub use device::{device_address, DeviceAddressError};
pub use rejoin::Rejoiner;
pub use socket::{
    bind_multicast, bind_or_retry, Arrival, ArrivalMetadata, BindPlan, CaptureStats,
    OverflowTracker, PendingLoss, PortBinding, SocketSource, SocketSourceConfig, SourceGate,
    SourceKey, SourceVerdict, Synthesiser, Waited,
};

#[cfg(feature = "afpacket")]
pub use afpacket::{
    bpf_filter_for, datalink_refusal, AfPacketSource, AfPacketSourceConfig, AfPacketStats,
    FeedFilter, FrameSkip, Linktype, ParsedFrame, Precision, RingAccounting, RingDelta,
    PARSED_DATALINK,
};
