//! Transmitter discipline: one socket per port role, and what it is allowed to
//! do to the caller.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};

use crate::error::SinkError;
use crate::instance::EgressEndpoint;
use crate::policy::{EgressPolicy, PolicyError, RouteLookup};
use crate::sink::{DatagramSink, FailureScope};

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
    /// block, and [`SinkError::Socket`] for anything else.
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
    pub fn open(source: Ipv4Addr, destination: SocketAddrV4, ttl: u8) -> io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(source, 0))?;
        socket.set_multicast_ttl_v4(u32::from(ttl))?;
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
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(SinkError::WouldBlock),
            Err(error) => Err(SinkError::Socket(error)),
        }
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
        }
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
        self.socket.send(datagram)
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
