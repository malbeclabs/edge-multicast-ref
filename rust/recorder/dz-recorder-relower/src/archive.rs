//! The upstream side's I/O, behind a trait.
//!
//! The multicast side already has one: [`Source`](dz_recorder_core::Source) is
//! how every offline tier reads an archive back, and this crate uses it
//! unchanged. The upstream side needs its own, because what a transport yields
//! is a payload and not a datagram — no address, no port role, no sequence
//! number, and a receive stamp taken on the publisher's host rather than on a
//! subscriber's.
//!
//! Both are traits for one reason: **no test of this crate may need a
//! filesystem, a privilege or a network.** A comparison is a pure function of
//! two byte streams, and the moment reading one of them requires opening
//! something, the tool that decides whether a publisher is correct can only be
//! tested where that something exists.

use dz_adapter_core::{ConnectionId, Payload};
use dz_recorder_core::SourceError;

/// One archived upstream payload, exactly as the transport yielded it.
///
/// The fields are [`Payload`]'s, because that is what the adapter will be handed
/// and a second shape here would be a second definition of it. What an archive
/// must carry for a re-lowering to be possible is therefore precisely this: the
/// bytes, the receive stamp, and which connection delivered them.
///
/// The venue's own timestamp is *not* here, and must not be: it is a field
/// inside the bytes, and it reaches the wire through the event the adapter
/// produces. An archive that carried it separately would let a re-lowering read
/// it from the wrong place.
#[derive(Debug, Clone, Copy)]
pub struct ArchivedPayload<'a> {
    /// The upstream's bytes, verbatim. Nothing here may normalise them: a
    /// payload the adapter refuses is evidence, and repairing it destroys the
    /// evidence.
    pub bytes: &'a [u8],
    /// When the transport received it, on the publisher's host.
    pub recv_ts_ns: u64,
    /// Which connection delivered it. An adapter whose mapping depends on the
    /// connection — one upstream for depth, another for trades — reproduces
    /// nothing offline without it.
    pub connection: ConnectionId,
}

impl<'a> ArchivedPayload<'a> {
    /// The payload as the adapter is handed it.
    #[must_use]
    pub const fn as_payload(&self) -> Payload<'a> {
        Payload {
            bytes: self.bytes,
            recv_ts_ns: self.recv_ts_ns,
            connection: self.connection,
        }
    }
}

/// A stream of archived upstream payloads, **in the order the transport yielded
/// them**.
///
/// The ordering is not a convenience. An adapter keeps a book, and a lowering
/// keeps `Per-Instrument Seq`; both are functions of the order the events
/// arrived in, so a payload archive replayed out of order re-lowers a different
/// stream and every depth join key is wrong. An implementation that cannot
/// guarantee receive order cannot be used here, and should say so rather than
/// approximate it.
///
/// `Ok(None)` is the end of the window. [`SourceError`] is `dz-recorder-core`'s,
/// deliberately: a second error taxonomy for the same two failures — the read
/// broke, or the archive is not readable — is how the two come to be reported
/// differently for the same cause.
pub trait PayloadArchive {
    /// The next payload, or `Ok(None)` at the end of the window.
    ///
    /// # Errors
    ///
    /// [`SourceError`] when the archive cannot be read further. The caller stops
    /// and reports: see
    /// [`RelowerError::PayloadArchive`](crate::RelowerError::PayloadArchive) for
    /// why a partial window must not be compared.
    fn next(&mut self) -> Result<Option<ArchivedPayload<'_>>, SourceError>;
}

/// An in-memory [`PayloadArchive`]: the reference implementation, and the one
/// the tests use.
///
/// Whatever loads a real window — a file, an object store, the Unix socket the
/// tee writes — decodes into this or implements the trait itself. Holding the
/// window in memory is what an offline tier can afford and the record path
/// cannot — the same split the datagram side already makes between a borrowed
/// [`RecordedDatagram`](dz_recorder_core::RecordedDatagram) and the owned form
/// the replay crate hands an iterator.
#[derive(Debug, Clone, Default)]
pub struct PayloadLog {
    entries: Vec<Entry>,
    at: usize,
}

#[derive(Debug, Clone)]
struct Entry {
    bytes: Vec<u8>,
    recv_ts_ns: u64,
    connection: ConnectionId,
}

impl PayloadLog {
    /// An empty log.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            at: 0,
        }
    }

    /// Append one payload. Order is receive order, and this is where it is
    /// fixed.
    pub fn push(&mut self, bytes: &[u8], recv_ts_ns: u64, connection: ConnectionId) {
        self.entries.push(Entry {
            bytes: bytes.to_vec(),
            recv_ts_ns,
            connection,
        });
    }

    /// How many payloads the window holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the window holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Replay from the beginning again.
    ///
    /// Useful for running two adapters over one window; the archive is
    /// immutable, so this cannot change what the second one sees.
    pub fn rewind(&mut self) {
        self.at = 0;
    }
}

impl PayloadArchive for PayloadLog {
    fn next(&mut self) -> Result<Option<ArchivedPayload<'_>>, SourceError> {
        let entry = match self.entries.get(self.at) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        self.at += 1;
        Ok(Some(ArchivedPayload {
            bytes: &entry.bytes,
            recv_ts_ns: entry.recv_ts_ns,
            connection: entry.connection,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_log_replays_in_the_order_it_was_written() {
        // The property the trait's contract rests on, asserted on the reference
        // implementation: an adapter's book and the lowering's sequence are both
        // functions of this order.
        let conn = ConnectionId::new("mktdata");
        let mut log = PayloadLog::new();
        log.push(b"first", 10, conn);
        log.push(b"second", 20, conn);

        let mut seen = Vec::new();
        while let Some(payload) = log.next().expect("in-memory reads cannot fail") {
            seen.push((payload.bytes.to_vec(), payload.recv_ts_ns));
        }
        assert_eq!(
            seen,
            vec![(b"first".to_vec(), 10), (b"second".to_vec(), 20)]
        );
    }

    #[test]
    fn a_rewound_log_replays_the_same_window() {
        let conn = ConnectionId::new("mktdata");
        let mut log = PayloadLog::new();
        log.push(b"only", 1, conn);
        assert!(log.next().expect("in-memory").is_some());
        assert!(log.next().expect("in-memory").is_none());
        log.rewind();
        assert_eq!(
            log.next().expect("in-memory").map(|p| p.bytes.to_vec()),
            Some(b"only".to_vec())
        );
    }
}
