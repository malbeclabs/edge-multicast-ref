//! Egress: what turns messages into datagrams and puts them on the wire.
//!
//! Everything between a wire message and the socket, once, for every venue.
//! What a venue supplies is the message; what this crate owns is
//! `Sequence Number`, `Reset Count`, the port role a message is allowed on, the
//! mandated datagram cap, the source address, the socket's discipline, and
//! every `dz_publisher_egress_*` series. A publisher that reaches a multicast
//! socket any other way has re-decided all of them, and each one has been
//! decided wrong somewhere already.
//!
//! # The shape
//!
//! ```text
//!   messages ──▶ ChannelEgress ──▶ dyn DatagramSink ──┬──▶ MulticastTransmitter ──▶ socket
//!                 (one per            (Tee, when      └──▶ … a second destination
//!                  port role)          there is more
//!                                      than one)
//! ```
//!
//! - [`ChannelEgress`] composes datagrams through [`dz_edge_core::DatagramBuilder`]
//!   for one port role, numbers them, and hands them on. It never blocks and
//!   never queues.
//! - [`DatagramSink`] is the boundary. It is a trait so that the composing and
//!   the numbering are tested with no socket, and so that a second destination
//!   is added without touching the code that owns `Sequence Number`.
//! - [`MulticastTransmitter`] is one port role's socket, opened under the
//!   discipline in [`transmitter`]. [`Tee`] is the fan-out, and absorbs a
//!   member's failure rather than ending a send.
//! - [`Sequencer`] holds a series per channel instance; [`EraStore`] holds the
//!   `Reset Count` that survives a restart.
//! - [`EgressPolicy`] decides the source address, from the route rather than
//!   from configuration, and refuses one that fails the operator's stated
//!   invariant.
//!
//! # What is not here
//!
//! Heartbeats, `EndOfSession`, the definition cycle and the manifest cadence
//! are all questions of *when*, which is a property of whatever owns the clock
//! rather than of the send path. They reach the wire through
//! [`ChannelEgress::push`] like any other message. Configuration loading is the runtime's; [`EgressPolicy`] is the
//! parsed shape of one section, not a parser.
//!
//! # A message's metric label is passed, not derived
//!
//! [`ChannelEgress::push`] takes the [`EgressMessageType`](dz_publisher_metrics::EgressMessageType)
//! a message is counted under, alongside the message. It would be better
//! carried by the message type itself, and it cannot be: the label vocabulary
//! is owned by the metrics crate, the message types by one codec crate per feed
//! spec, and a trait tying them together can be implemented in neither — this
//! crate cannot implement it for a foreign type, and depending on every feed's
//! codec crate to do so would defeat the reason there is one crate per feed
//! spec. The alternative that does work is an associated
//! `EgressMessageType` on `AppMessage`, which is a change to the codec.
//!
//! # Vocabulary
//!
//! `datagram`, never `frame`, for our own traffic. `era` for a `Reset Count`
//! generation. `channel` for the `Channel ID` shard and nothing else — the
//! three ports are *port roles*, spelled `mktdata`, `refdata` and `snapshot`.

#![forbid(unsafe_code)]

pub mod egress;
pub mod era;
pub mod error;
pub mod instance;
pub mod policy;
pub mod reference;
pub mod sequencer;
pub mod sink;
pub mod transmitter;

pub use egress::ChannelEgress;
pub use era::{EraError, EraStore, FIRST_ERA};
pub use error::{EgressError, SinkError};
pub use instance::{ChannelInstance, EgressEndpoint};
pub use policy::{
    EgressPolicy, Ipv4Prefix, KernelRoute, PolicyError, PrefixError, RouteLookup, DEFAULT_TTL,
};
pub use reference::ReferenceStream;
pub use sequencer::Sequencer;
pub use sink::{DatagramSink, FailureScope, Tee};
pub use transmitter::{DatagramSocket, KernelSocket, MulticastTransmitter, OpenError};
