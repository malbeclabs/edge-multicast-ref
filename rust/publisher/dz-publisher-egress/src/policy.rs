//! Route-derived egress policy: which address this publisher sends from, and
//! why nothing here pins an interface.
//!
//! # `IP_MULTICAST_IF` stays unset
//!
//! The tempting improvement is `set_multicast_if_v4`, so that a host with
//! several interfaces sends its multicast out of the intended one. It is
//! written down as a roadmap item in one place and as an outage in another, and
//! the outage is the one that decides it: the kernel resolves the option to an
//! **interface index** at `setsockopt` time, the tunnel interface is destroyed
//! and recreated with a new index on every re-provision, and the socket is then
//! bound to an index that no longer exists — returning `ENODEV` on every send,
//! forever, with no event to tell anyone it happened.
//!
//! What the option is wanted for is achieved instead by binding the socket to
//! the source address the route already resolves to. The kernel does the
//! interface selection per datagram, from the routing table, which is the thing
//! that actually changes when the tunnel is re-provisioned.
//!
//! # The address is a lease, not a host identity
//!
//! The other half of the same lesson: a publisher that read its source address
//! from configuration met a tunnel address that had moved, found the configured
//! address no longer existed, and crash-looped tens of thousands of times over
//! two days. So the address is discovered, not configured, and
//! [`EgressPolicy::pin`] exists only as an operator's override for a host where
//! discovery is wrong — an escape hatch, never the normal path.
//!
//! Discovery still needs a check, because a wrong answer is silent: a source
//! address from the wrong interface produces datagrams that are well formed,
//! carry a dense sequence series, and are read by every subscriber as a
//! *different channel instance* from the one they were told to expect.
//! [`EgressPolicy::expected_prefix`] is that check, and it is an invariant
//! asserted at startup rather than a value used.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

/// The default multicast TTL. One hop: the group is delivered on the attached
/// segment and the network's own last-mile carries it from there.
pub const DEFAULT_TTL: u8 = 1;

/// An IPv4 prefix, for stating an invariant about a discovered address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ipv4Prefix {
    network: u32,
    len: u8,
}

impl Ipv4Prefix {
    /// Parse `a.b.c.d/len`.
    ///
    /// The host bits are not required to be zero and are masked off: an
    /// operator who writes the tunnel's own address with the pool's prefix
    /// length has said exactly what they meant, and refusing it would be
    /// pedantry that costs a startup.
    ///
    /// # Errors
    ///
    /// [`PrefixError`] for anything that is not one address, one `/`, and a
    /// length in `0..=32`.
    pub fn parse(text: &str) -> Result<Self, PrefixError> {
        let (addr, len) = text.split_once('/').ok_or(PrefixError::NoLength)?;
        let addr: Ipv4Addr = addr.parse().map_err(|_| PrefixError::NotAnAddress)?;
        let len: u8 = len.parse().map_err(|_| PrefixError::NotALength)?;
        if len > 32 {
            return Err(PrefixError::NotALength);
        }
        Ok(Self {
            network: u32::from(addr) & Self::mask(len),
            len,
        })
    }

    #[must_use]
    pub const fn contains(&self, addr: Ipv4Addr) -> bool {
        addr.to_bits() & Self::mask(self.len) == self.network
    }

    /// `/0` is every address, and shifting a `u32` by 32 is undefined, so the
    /// all-zero mask is spelled out rather than computed.
    const fn mask(len: u8) -> u32 {
        if len == 0 {
            0
        } else {
            u32::MAX << (32 - len)
        }
    }
}

impl std::fmt::Display for Ipv4Prefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", Ipv4Addr::from_bits(self.network), self.len)
    }
}

/// Why a prefix could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PrefixError {
    #[error("a prefix needs a `/` and a length")]
    NoLength,
    #[error("the part before the `/` is not an IPv4 address")]
    NotAnAddress,
    #[error("the part after the `/` is not a prefix length in 0..=32")]
    NotALength,
}

/// How the kernel is asked which source address reaches a destination.
///
/// A trait for one reason: the answer depends on the host's routing table, and
/// a test that needs a route to a multicast group is a test that does not run
/// in CI. Everything policy *decides* — the override, the invariant, the
/// refusal of an unusable answer — is decided against this and is therefore
/// decided in a test.
pub trait RouteLookup {
    /// The source address the routing table resolves for `destination`.
    ///
    /// # Errors
    ///
    /// Whatever the host said. No route to the group is the common one, and it
    /// is a startup failure rather than something to retry per datagram.
    fn source_for(&self, destination: SocketAddrV4) -> io::Result<Ipv4Addr>;
}

/// The routing table, asked the way the send path itself asks it.
///
/// A UDP socket is bound to the wildcard address, connected to the destination,
/// and its local address read back. `connect` on a datagram socket sends
/// nothing: it makes the kernel perform the route lookup and pick the source
/// address, which is precisely the decision being asked about — so the answer
/// is the one the send path would have used, not a reimplementation of the
/// kernel's route selection that can disagree with it.
///
/// This is also why it is not a parse of the routing table: a routing table
/// says which interface, and the source address is then the interface's, which
/// takes another interface enumeration and another rule for an interface with
/// several addresses. The kernel already has both.
pub struct KernelRoute;

impl RouteLookup for KernelRoute {
    fn source_for(&self, destination: SocketAddrV4) -> io::Result<Ipv4Addr> {
        let probe = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
        probe.connect(destination)?;
        match probe.local_addr()? {
            std::net::SocketAddr::V4(addr) => Ok(*addr.ip()),
            std::net::SocketAddr::V6(addr) => Err(io::Error::other(format!(
                "an IPv4 destination resolved to the IPv6 source {addr}"
            ))),
        }
    }
}

/// The `[egress]` section: how the source address is chosen, and the TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressPolicy {
    /// An operator's override of route discovery, for a host where discovery
    /// is wrong. Still held to `expected_prefix`: an operator who pins an
    /// address outside the prefix they declared has contradicted themselves in
    /// one file, and a startup is the cheapest place to find out.
    pub pin: Option<Ipv4Addr>,
    /// An invariant on the address, not a source of one. See the module docs.
    pub expected_prefix: Option<Ipv4Prefix>,
    /// The multicast TTL. See [`DEFAULT_TTL`].
    pub ttl: u8,
}

impl Default for EgressPolicy {
    /// Discovery, no invariant, one hop. A policy that states nothing is the
    /// policy of a host whose route is right, which is the normal case.
    fn default() -> Self {
        Self {
            pin: None,
            expected_prefix: None,
            ttl: DEFAULT_TTL,
        }
    }
}

impl EgressPolicy {
    /// The address to bind to for `destination`.
    ///
    /// Called once per transmitter at startup, and again only when a socket is
    /// re-opened. Not on the send path: a route lookup per datagram would put
    /// a socket syscall in front of every send to defend against a change that
    /// happens when a tunnel is re-provisioned, which is a restart-scale event.
    ///
    /// # Errors
    ///
    /// [`PolicyError`]. Every variant is a refusal to start rather than a
    /// value to carry on with, because each one means the datagrams this
    /// publisher would emit arrive under an identity nobody is subscribed to.
    pub fn resolve_source(
        &self,
        destination: SocketAddrV4,
        route: &dyn RouteLookup,
    ) -> Result<Ipv4Addr, PolicyError> {
        let source = match self.pin {
            Some(pinned) => pinned,
            None => route
                .source_for(destination)
                .map_err(|source| PolicyError::NoRoute {
                    destination,
                    source,
                })?,
        };
        // The wildcard address is what a socket that was never routed reports,
        // and binding to it hands the source-address choice back to the kernel
        // per datagram. That is not merely unpinned: the channel instance a
        // subscriber tracks is keyed on the source address, so a publisher
        // whose datagrams change source mid-run is read as two publishers
        // alternating, each seeing the other's gaps.
        if source.is_unspecified() {
            return Err(PolicyError::Unspecified { destination });
        }
        if let Some(prefix) = self.expected_prefix {
            if !prefix.contains(source) {
                return Err(PolicyError::OutsideExpectedPrefix {
                    found: source,
                    expected: prefix,
                });
            }
        }
        Ok(source)
    }
}

/// Why a source address was refused.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("no route to {destination}: {source}")]
    NoRoute {
        destination: SocketAddrV4,
        #[source]
        source: io::Error,
    },

    #[error("the route to {destination} resolved no source address")]
    Unspecified { destination: SocketAddrV4 },

    /// The discovered address is outside the declared prefix.
    ///
    /// The invariant an operator asked for, doing its job. Reported with both
    /// values because the interesting case is not that they differ but *how*:
    /// an address from the wrong pool means the wrong interface, and an address
    /// from no pool at all means the tunnel is not up yet.
    #[error("egress source {found} is outside the expected prefix {expected}")]
    OutsideExpectedPrefix {
        found: Ipv4Addr,
        expected: Ipv4Prefix,
    },
}
