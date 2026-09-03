//! The reference stream: a second copy of every datagram, to a local socket.
//!
//! # What it is for
//!
//! A subscriber-site archive can say *this datagram never arrived*. It cannot
//! say whether it was never sent. This sink is the other half of that question:
//! byte-identical copies of every datagram a publisher composed, delivered to a
//! consumer on the publisher host — a recorder — so the two archives can be
//! diffed on `(source, Channel ID, destination port, Sequence Number)`. Network
//! loss, reordering, MTU drops and one-way latency become measured rather than
//! inferred.
//!
//! It sees what the publisher decided to send and nothing upstream of that, so
//! a mapping defect is faithfully reproduced on both sides. That limit is the
//! reason the offline re-lowering exists as well, and neither replaces the
//! other.
//!
//! # Two rules, and they decide the whole shape
//!
//! **It never blocks the send path, and its failure is never propagated.** A
//! reference stream that can stall the feed it measures is worse than no
//! reference stream. So the socket is non-blocking and every failure is
//! [`SinkError`], which [`Tee`](crate::Tee) counts and absorbs; the sink
//! declares [`FailureScope::Channel`], so a dead consumer costs the copy and
//! not the process.
//!
//! # Why a datagram socket and not a stream
//!
//! `SOCK_DGRAM` on a Unix path preserves message boundaries, so one datagram in
//! is one datagram out and there is no framing to invent, agree on, or get
//! wrong. A stream socket would need a length prefix that both ends
//! implemented, and the format a publisher writes here has to be readable by a
//! recorder that was not built alongside it.
//!
//! **Unconnected sends, deliberately.** The socket is bound to nothing and each
//! datagram is addressed to the consumer's path, so a consumer that has not
//! started yet, or that has restarted, costs the datagrams it was not there for
//! and nothing else. A connected socket would make the first send after a
//! consumer restart fail permanently on a stale peer.

use std::io;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};

use dz_edge_core::MAX_DATAGRAM_SIZE;

use crate::error::SinkError;
use crate::sink::{DatagramSink, FailureScope};

/// A copy of every datagram, to a Unix datagram socket.
#[derive(Debug)]
pub struct ReferenceStream {
    name: &'static str,
    socket: UnixDatagram,
    destination: PathBuf,
}

impl ReferenceStream {
    /// Open the sink that copies to `destination`.
    ///
    /// The consumer's socket does not have to exist yet: nothing is connected
    /// and nothing is resolved here, so a publisher starting before its
    /// recorder is the ordinary case rather than a startup failure. What can
    /// fail is creating our own unbound socket, which is a broken host.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when an unbound datagram socket cannot be created or made
    /// non-blocking.
    pub fn open(name: &'static str, destination: &Path) -> Result<Self, io::Error> {
        let socket = UnixDatagram::unbound()?;
        // The reason this sink can promise not to block the send path. Without
        // it a consumer that stops reading parks the publisher in `sendto`,
        // which is the one thing a reference stream must never be able to do.
        socket.set_nonblocking(true)?;
        Ok(Self {
            name,
            socket,
            destination: destination.to_path_buf(),
        })
    }

    /// The path copies are addressed to.
    #[must_use]
    pub fn destination(&self) -> &Path {
        &self.destination
    }
}

impl DatagramSink for ReferenceStream {
    fn name(&self) -> &str {
        self.name
    }

    fn send(&mut self, datagram: &[u8]) -> Result<(), SinkError> {
        // Checked here as it is at the socket, and for the same reason: a sink
        // is reachable by something that is not a builder, and a copy that is
        // longer than the cap is not a copy of anything the wire carried.
        if datagram.len() > MAX_DATAGRAM_SIZE {
            return Err(SinkError::TooLarge {
                len: datagram.len(),
            });
        }
        match self.socket.send_to(datagram, &self.destination) {
            Ok(_) => Ok(()),
            // The consumer is slow or its buffer is full. **Transient**, so the
            // member stays in the fan-out: a recorder that fell behind for a
            // burst has not gone away, and dropping it would silently end the
            // reference stream for the life of the process.
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(SinkError::WouldBlock),
            // Every other failure — no such path, nobody listening, permission
            // — is not transient, and the member is dropped. That is the
            // designed outcome and it is why the scope below is `Channel`:
            // `Tee::live` and `Tee::dropped` are where the silence is visible.
            Err(error) => Err(SinkError::Socket(error)),
        }
    }

    /// [`FailureScope::Channel`], and this is the load-bearing line of the file.
    ///
    /// A failure here costs a consumer of a copy and nothing else. Ending the
    /// process over it would turn an auxiliary outage into a feed outage, which
    /// is exactly the trade the tee exists to refuse.
    fn failure_scope(&self) -> FailureScope {
        FailureScope::Channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_datagram_arrives_whole_and_unaltered() {
        let dir = std::env::temp_dir().join(format!("dz-reference-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let path = dir.join("tee.sock");
        let _ = std::fs::remove_file(&path);
        let consumer = UnixDatagram::bind(&path).expect("bind the consumer");

        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        // Two sends, because message boundaries are the property this socket
        // type was chosen for: a stream would deliver these as four bytes.
        sink.send(b"aa").expect("the first copy");
        sink.send(b"bbb").expect("the second copy");

        let mut buf = [0u8; 64];
        let n = consumer.recv(&mut buf).expect("the first datagram");
        assert_eq!(&buf[..n], b"aa");
        let n = consumer.recv(&mut buf).expect("the second datagram");
        assert_eq!(&buf[..n], b"bbb");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn no_consumer_is_a_failure_the_tee_can_absorb_and_not_a_panic() {
        let path = std::env::temp_dir().join(format!("dz-absent-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        // A publisher whose recorder is not running must still publish. The
        // error is what `Tee` counts; what matters is that a send happened at
        // all and returned.
        let error = sink.send(b"x").expect_err("nothing is listening");
        assert!(matches!(error, SinkError::Socket(_)));
        assert_eq!(sink.failure_scope(), FailureScope::Channel);
    }

    #[test]
    fn an_over_cap_datagram_is_refused_rather_than_copied_short() {
        let path = std::env::temp_dir().join(format!("dz-cap-{}.sock", std::process::id()));
        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        let too_long = vec![0u8; MAX_DATAGRAM_SIZE + 1];
        assert!(matches!(
            sink.send(&too_long),
            Err(SinkError::TooLarge { .. })
        ));
    }
}
