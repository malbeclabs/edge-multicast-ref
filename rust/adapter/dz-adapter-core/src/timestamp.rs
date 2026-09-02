//! Which of the venue's own clocks an event's `source_ts_ns` was read from.

/// Which venue clock the `source_ts_ns` on an adapter's events comes from.
///
/// **These four variants are a metric label**, the same arrangement as
/// [`ParseError`](crate::ParseError) and
/// [`DisconnectReason`](crate::DisconnectReason):
/// `dz_publisher_venue_to_recv_latency_seconds{timestamp_kind}` is a histogram
/// per kind, and the runtime cannot observe it without being told which one an
/// adapter read. The four values are the whole of that label's vocabulary, so
/// an adapter that declares one gets the right child series and cannot invent a
/// fifth that no panel groups by.
///
/// The same taxonomy is declared a second time, as a label enum, in the metrics
/// crate. Two copies is the cost of this crate depending on nothing: a venue
/// must not inherit a Prometheus client to say which of its own clocks it read.
/// They are held to each other by a test over a dev-dependency —
/// `dz-ingress-core`'s `tests/label_taxonomies.rs`, which maps this enum onto
/// the label enum with an exhaustive match in **both** directions, so a variant
/// added on either side fails to compile rather than producing a series nobody
/// groups by. That is the arrangement `ParseError` already sits in, and it is
/// safe for the same reason: neither copy is generated from the other, so the
/// check fails when either one moves.
///
/// # Not [`StampSource`], and the distinction is the point
///
/// `dz-ingress-core`'s `StampSource` says which of **our** clocks stamped an
/// arrival — the kernel, or the transport once the read returned. This one says
/// which of the **venue's** clocks produced a timestamp inside the payload.
/// They are the two halves of one interval and are never the same reading. The
/// transport crate deliberately did not spell its own enum like this label, for
/// exactly the reason that would matter here: two taxonomies sharing a name is
/// how one gets recorded under the other's label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueTimestampKind {
    /// The venue stamped the message when its own edge received it.
    ExchangeRecv,
    /// The venue's matching engine stamped the event it produced. The earliest
    /// reading a venue usually publishes, and the one a venue-to-receive
    /// interval is most meaningful against.
    MatchingEngine,
    /// The venue stamped the message as its gateway sent it out.
    GatewaySend,
    /// The timestamp is a block's own, for a venue whose events are settled on
    /// a chain rather than in a matching engine.
    BlockTime,
}

impl VenueTimestampKind {
    /// Every variant, in the order the metrics crate declares them.
    ///
    /// Used by the test that holds the two taxonomies to each other, so that
    /// adding a variant here without adding the label there fails a build
    /// rather than producing a kind no dashboard groups by.
    pub const ALL: [Self; 4] = [
        Self::ExchangeRecv,
        Self::MatchingEngine,
        Self::GatewaySend,
        Self::BlockTime,
    ];

    /// The label value observations against this clock are recorded under.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExchangeRecv => "exchange_recv",
            Self::MatchingEngine => "matching_engine",
            Self::GatewaySend => "gateway_send",
            Self::BlockTime => "block_time",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_a_distinct_token() {
        let mut tokens: Vec<&str> = VenueTimestampKind::ALL.iter().map(|k| k.as_str()).collect();
        tokens.sort_unstable();
        let count = tokens.len();
        tokens.dedup();
        assert_eq!(tokens.len(), count, "two kinds share a label value");
    }
}
