//! What a sequence space is keyed on, and where one transmitter sends.

use std::fmt;
use std::net::Ipv4Addr;

use dz_edge_core::PortRole;

/// The identity of one channel instance: the only correct key for anything
/// that owns a sequence space.
///
/// The glossary keys a channel instance on `(source IP address, Channel ID,
/// destination port)`, and every component earns its place:
///
/// - **Source IP address.** Two publishers may serve the same `Channel ID` to
///   the same group and port, each advancing its own series. Keyed any less
///   finely, the two interleave into one counter that goes backwards on every
///   alternation.
/// - **`Channel ID`.** The shard. Nothing else in this crate means "channel".
/// - **Destination port.** The port roles are separate instances with
///   independent series, which is why every `dz_publisher_egress_*` family
///   carries `port_role` and why nothing may aggregate across it. A message
///   emitted on the snapshot port does not consume a number from the mktdata
///   series.
///
/// [`dz_edge_core::ChannelSequence`] deliberately carries the *state* a channel
/// instance owns without its identity, because the codec knows nothing of
/// sockets, addresses or ports. This is the other half, and it is why the
/// sequencer lives in this crate rather than in the codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelInstance {
    pub source: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
}

impl ChannelInstance {
    #[must_use]
    pub const fn new(source: Ipv4Addr, channel_id: u8, dst_port: u16) -> Self {
        Self {
            source,
            channel_id,
            dst_port,
        }
    }
}

impl fmt::Display for ChannelInstance {
    /// For a log line and for an error message. Not a metric label: `source`
    /// and `dst_port` are per-deployment values and would multiply every
    /// series, which is why the metric families carry `port_role` and
    /// `channel_id` and nothing else.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "channel {} from {} to port {}",
            self.channel_id, self.source, self.dst_port
        )
    }
}

/// Where one transmitter sends from and to.
///
/// Carried as one value so that the identity the sequencer numbers under and
/// the identity the socket actually sends from cannot disagree. They are the
/// same three components a subscriber sees, and a sequencer keyed on a source
/// address the socket is not bound to is numbering a channel instance that does
/// not exist: the series looks dense here and arrives on the wire under another
/// identity entirely.
///
/// [`crate::MulticastTransmitter::endpoint`] hands this back, and
/// [`crate::ChannelEgress`] takes it, so the composer inherits the socket's
/// identity rather than being told it a second time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressEndpoint {
    /// Which of the three ports this transmitter serves. One socket per port
    /// role; see [`crate::MulticastTransmitter`].
    pub port_role: PortRole,
    /// The address the socket is bound to, derived from the route rather than
    /// read from configuration. See [`crate::EgressPolicy`].
    pub source: Ipv4Addr,
    /// The destination port for this port role. The group is the socket's and
    /// is not part of the channel-instance key: the specification mandates one
    /// group with two destination ports, so the port is what separates the
    /// roles.
    pub dst_port: u16,
}

impl EgressEndpoint {
    #[must_use]
    pub const fn new(port_role: PortRole, source: Ipv4Addr, dst_port: u16) -> Self {
        Self {
            port_role,
            source,
            dst_port,
        }
    }

    /// The channel instance this endpoint serves a given `Channel ID` under.
    #[must_use]
    pub const fn instance(&self, channel_id: u8) -> ChannelInstance {
        ChannelInstance::new(self.source, channel_id, self.dst_port)
    }
}
