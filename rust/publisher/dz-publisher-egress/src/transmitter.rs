//! Transmitter discipline: one socket per port role, and what it is allowed to
//! do to the caller.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::num::NonZeroU8;
use std::time::{Duration, Instant};

use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};

use crate::error::SinkError;
use crate::instance::EgressEndpoint;
use crate::policy::{EgressPolicy, PolicyError, RouteLookup};
use crate::sink::{DatagramSink, FailureScope};

/// How long a transmitter's route may stay down before its failure stops
/// being transient.
///
/// The same span as the runtime's default `idle_guard`, and for the same
/// reason: long enough for a tunnel daemon restart, which took five seconds on
/// the host this was measured on, and short enough that an interface which
/// came back under a different address — and so never serves this socket
/// again — ends the process within a minute rather than leaving it counting
/// failures forever. The supervisor's restart is what re-derives the address.
pub const MAX_ROUTE_DOWN: Duration = Duration::from_secs(60);

/// The one socket operation egress needs, behind a trait so the layers above
/// it are tested with no privileges and no network.
///
/// Deliberately narrower than a socket: no bind, no options, no address. Those
/// are decided once, when the socket is opened, and a trait that exposed them
/// would let a caller re-decide the interface selection this crate exists to
/// keep unset.
pub trait DatagramSocket {
    /// Send one datagram to the address this socket was opened for.
    ///
    /// # Errors
    ///
    /// [`SinkError::WouldBlock`] for a full send buffer, which must never
    /// block, [`SinkError::RouteDown`] for a route that has gone, and
    /// [`SinkError::Socket`] for anything else.
    fn send(&self, datagram: &[u8]) -> Result<(), SinkError>;
}

/// A real UDP socket, opened under the discipline the design settled on.
///
/// Four decisions, all made here so that nothing above can unmake them:
///
/// - **Bound to the route-derived source address.** The channel instance a
///   subscriber tracks is keyed on that address, so it is pinned by the bind
///   rather than left to per-datagram route selection. [`EgressPolicy`] is
///   where the address comes from.
/// - **`IP_MULTICAST_IF` unset.** See [`crate::policy`] for the outage that
///   settles this.
/// - **Connected.** The destination is fixed at open time, so the send path
///   carries no address and cannot send this port role's datagrams to another
///   role's port.
/// - **Non-blocking.** A full send buffer must be a counted loss, not a parked
///   publish loop: the datagram already has a number, and the messages queueing
///   behind a blocked send are for every other instrument this publisher
///   serves.
pub struct KernelSocket {
    socket: UdpSocket,
}

impl KernelSocket {
    /// Open a socket for one port role's destination.
    ///
    /// # Errors
    ///
    /// The bind, the TTL, or the connect. A bind that fails because the source
    /// address does not exist is the tunnel-address-moved failure, and it is
    /// reported here rather than survived: the address is re-derived by opening
    /// a new socket, which is a decision for whatever supervises this
    /// publisher.
    ///
    /// `ttl` is a [`NonZeroU8`] because a hop count of zero is accepted by
    /// `set_multicast_ttl_v4` and carried by no interface. See
    /// [`EgressPolicy::ttl`].
    pub fn open(source: Ipv4Addr, destination: SocketAddrV4, ttl: NonZeroU8) -> io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(source, 0))?;
        socket.set_multicast_ttl_v4(u32::from(ttl.get()))?;
        // Left at the kernel default, which is on. A subscriber co-located
        // with the publisher — a health checker, a local parser — receives the
        // group through it, and disabling it would make a publisher that is
        // demonstrably transmitting look silent to anything on the same host.
        // Stated rather than omitted, so that it reads as a decision.
        socket.set_nonblocking(true)?;
        socket.connect(destination)?;
        Ok(Self { socket })
    }
}

impl DatagramSocket for KernelSocket {
    fn send(&self, datagram: &[u8]) -> Result<(), SinkError> {
        match self.socket.send(datagram) {
            Ok(sent) if sent == datagram.len() => Ok(()),
            // A datagram socket does not partially send: it takes the whole
            // datagram or none of it. If this is ever reached, the datagram on
            // the wire is truncated and its declared length disagrees with its
            // size, which every subscriber reads as a malformed datagram. Not
            // reported as success.
            Ok(sent) => Err(SinkError::Socket(io::Error::other(format!(
                "sent {sent} of {} bytes",
                datagram.len()
            )))),
            Err(error) => Err(classify(error)),
        }
    }
}

/// What a failed send on a [`KernelSocket`] means.
///
/// Measured in a network namespace against a socket bound as this crate binds
/// one: the interface deleted gives `ENETUNREACH` on every send, the interface
/// restored under the same address makes the same socket send again, and
/// restored under a different address gives `ENETUNREACH` for good. The other
/// three kinds are what the same event can surface as on another route shape.
fn classify(error: io::Error) -> SinkError {
    match error.kind() {
        io::ErrorKind::WouldBlock => SinkError::WouldBlock,
        io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::NetworkDown
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::AddrNotAvailable => SinkError::RouteDown(error),
        _ => SinkError::Socket(error),
    }
}

/// One port role's transmitter: a socket, the identity it sends under, and
/// what its failure costs.
///
/// **One per port role.** The roles are separate channel instances with
/// independent sequence series, and one socket serving two of them could send
/// a snapshot datagram to the mktdata port — where its `Sequence Number`
/// belongs to a series subscribers track separately, so it lands as a
/// duplicate of a live datagram and is discarded, and the snapshot is simply
/// never delivered.
pub struct MulticastTransmitter<S: DatagramSocket> {
    name: &'static str,
    socket: S,
    endpoint: EgressEndpoint,
    scope: FailureScope,
    max_route_down: Duration,
    /// When the current run of [`SinkError::RouteDown`] began, and when its
    /// latest refusal was.
    route_down: Option<(Instant, Instant)>,
}

impl<S: DatagramSocket> MulticastTransmitter<S> {
    /// Wrap an already-opened socket.
    ///
    /// `endpoint` must describe what `socket` actually sends: it is what the
    /// sequencer keys the series on. See [`EgressEndpoint`].
    #[must_use]
    pub const fn new(
        name: &'static str,
        socket: S,
        endpoint: EgressEndpoint,
        scope: FailureScope,
    ) -> Self {
        Self {
            name,
            socket,
            endpoint,
            scope,
            max_route_down: MAX_ROUTE_DOWN,
            route_down: None,
        }
    }

    /// Replace [`MAX_ROUTE_DOWN`], for a test that cannot wait a minute.
    #[must_use]
    pub const fn with_max_route_down(mut self, max_route_down: Duration) -> Self {
        self.max_route_down = max_route_down;
        self
    }

    /// The identity this transmitter sends under, to hand to the composer so
    /// that the numbering and the socket cannot disagree.
    #[must_use]
    pub const fn endpoint(&self) -> EgressEndpoint {
        self.endpoint
    }
}

impl MulticastTransmitter<KernelSocket> {
    /// Resolve the source address, open the socket, and wrap it.
    ///
    /// The whole startup path for one port role, in one call, so that a
    /// publisher cannot perform three quarters of it: the address comes from
    /// the policy, the socket is opened under the discipline above, and the
    /// endpoint the sequencer will key on is taken from the address that was
    /// actually bound.
    ///
    /// # Errors
    ///
    /// [`OpenError`]: the source address was refused, or the socket would not
    /// open.
    pub fn open(
        name: &'static str,
        policy: &EgressPolicy,
        destination: SocketAddrV4,
        port_role: PortRole,
        scope: FailureScope,
        route: &dyn RouteLookup,
    ) -> Result<Self, OpenError> {
        let source = policy.resolve_source(destination, route)?;
        let socket = KernelSocket::open(source, destination, policy.ttl)
            .map_err(|source| OpenError::Socket { source })?;
        Ok(Self::new(
            name,
            socket,
            EgressEndpoint::new(port_role, source, destination.port()),
            scope,
        ))
    }
}

impl<S: DatagramSocket> DatagramSink for MulticastTransmitter<S> {
    fn name(&self) -> &str {
        self.name
    }

    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError> {
        // The last gate before the wire. `DatagramBuilder` clamps its capacity
        // so it cannot compose one this long, but a builder is not the only
        // thing that can reach a sink, and an over-cap datagram is the defect
        // that has already reached production once — from a configuration key,
        // in a build whose builder did not clamp. Refused here rather than
        // truncated: truncating writes a declared length that disagrees with
        // the bytes, turning a size violation into a malformed datagram.
        if datagram.len() > MAX_DATAGRAM_SIZE {
            return Err(SinkError::TooLarge {
                len: datagram.len(),
            });
        }
        match self.socket.send(datagram) {
            Ok(()) => {
                self.route_down = None;
                Ok(())
            }
            Err(SinkError::RouteDown(error)) => {
                let now = Instant::now();
                // A run whose latest refusal is older than the window is over,
                // even with no send in between to say so: a port that sends
                // rarely must not carry one outage's clock into the next.
                let since = match self.route_down {
                    Some((since, last)) if now.duration_since(last) < self.max_route_down => since,
                    _ => now,
                };
                self.route_down = Some((since, now));
                if now.duration_since(since) >= self.max_route_down {
                    Err(SinkError::Socket(error))
                } else {
                    Err(SinkError::RouteDown(error))
                }
            }
            Err(error) => Err(error),
        }
    }

    fn failure_scope(&self) -> FailureScope {
        self.scope
    }
}

/// Why a transmitter could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Policy {
        #[from]
        source: PolicyError,
    },
    #[error("the egress socket would not open: {source}")]
    Socket {
        #[source]
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The errno the measurement found, and the kinds beside it, are a route
    /// that may come back; anything else about the socket is not.
    #[test]
    fn a_route_error_is_a_route_down_and_nothing_else_is() {
        for errno in [101, 100, 113, 99] {
            // ENETUNREACH, ENETDOWN, EHOSTUNREACH, EADDRNOTAVAIL on Linux.
            let error = classify(io::Error::from_raw_os_error(errno));
            assert!(matches!(error, SinkError::RouteDown(_)), "{errno}: {error}");
            assert!(error.is_transient());
        }
        let refused = classify(io::Error::from_raw_os_error(1));
        assert!(matches!(refused, SinkError::Socket(_)), "EPERM: {refused}");
        assert!(matches!(
            classify(io::Error::from(io::ErrorKind::WouldBlock)),
            SinkError::WouldBlock
        ));
    }

    use crate::policy::DEFAULT_TTL;

    /// A route that resolves the loopback address, so that the host's own
    /// routing table decides nothing here. See [`RouteLookup`].
    struct LoopbackRoute;

    impl RouteLookup for LoopbackRoute {
        fn source_for(&self, _destination: SocketAddrV4) -> io::Result<Ipv4Addr> {
            Ok(Ipv4Addr::LOCALHOST)
        }
    }

    /// The hop count a policy carries is the hop count the socket holds.
    ///
    /// Opened through [`MulticastTransmitter::open`], which is the whole path
    /// the runtime takes: the policy's value reaches `set_multicast_ttl_v4`
    /// through one argument, and a transmitter that passed a constant instead
    /// would send one hop while its document states 64. Read back off the
    /// kernel rather than inferred from a datagram arriving, because a
    /// subscriber on this host's own segment receives either way. Two values,
    /// so that a hop count hard-coded anywhere on that path fails here.
    ///
    /// Both of them are `NonZeroU8`, because that is the only thing the
    /// signature accepts — a zero reaching `set_multicast_ttl_v4` from a
    /// hand-composed policy is a compile error, and the compile-failure
    /// assertion for it is on [`EgressPolicy::ttl`].
    ///
    /// A real socket, and hermetic: the source address is the loopback one and
    /// the destination is a group in MCAST-TEST-NET, which the kernel resolves
    /// over `lo`. Nothing is sent, no group is joined, and no privilege is
    /// needed.
    #[test]
    fn the_hop_count_a_policy_carries_is_the_one_the_socket_holds() {
        let destination = SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 9), 41_003);

        for ttl in [DEFAULT_TTL, NonZeroU8::new(64).expect("a routed hop count")] {
            let policy = EgressPolicy {
                pin: None,
                expected_prefix: None,
                ttl,
            };

            let transmitter = MulticastTransmitter::open(
                "mktdata",
                &policy,
                destination,
                PortRole::Mktdata,
                FailureScope::Process,
                &LoopbackRoute,
            )
            .expect("a loopback source and a documentation group");

            assert_eq!(
                transmitter
                    .socket
                    .socket
                    .multicast_ttl_v4()
                    .expect("the kernel reports the option it was set"),
                u32::from(ttl.get()),
                "the socket holds the hop count the policy stated"
            );
        }
    }
}
