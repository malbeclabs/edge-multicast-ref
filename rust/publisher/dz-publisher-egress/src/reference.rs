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
//!
//! That promise is only kept if the *errno* for an absent consumer is treated as
//! transient, and it is the one part of this file that is easy to get wrong: an
//! unconnected `sendto` to a path with no socket file returns `ENOENT`, and one
//! to a path whose socket nobody is bound to returns `ECONNREFUSED`. Both are
//! [`SinkError::ConsumerAbsent`], which [`Tee`](crate::Tee) counts and keeps the
//! member for. Folded into the general socket failure they are non-transient,
//! the member is dropped on its first send, nothing restores it, and the
//! reference stream ends for the life of the process in exactly the case this
//! module says costs only the datagrams that were missed.

use std::io;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};

use dz_edge_core::MAX_DATAGRAM_SIZE;

use crate::error::SinkError;
use crate::sink::{DatagramSink, FailureScope};

/// The longest destination path a Unix socket address can carry.
///
/// `sockaddr_un.sun_path` is 108 bytes and the address is NUL-terminated, so
/// 107 bytes of path is the most that fits. Checked at
/// [`open`](ReferenceStream::open) rather than met at the first send, because
/// there it is a startup error naming the path and here it would be one
/// `EINVAL` per datagram forever — a publisher that came up cleanly, reports
/// itself healthy, and copies nothing.
const MAX_DESTINATION_LEN: usize = 107;

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
    /// fail is creating our own unbound socket, which is a broken host — and a
    /// destination too long to be addressed at all, which is a configuration
    /// mistake and not a state that can improve.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when an unbound datagram socket cannot be created or made
    /// non-blocking, and [`io::ErrorKind::InvalidInput`] for a destination
    /// longer than [`MAX_DESTINATION_LEN`].
    pub fn open(name: &'static str, destination: &Path) -> Result<Self, io::Error> {
        // Before the socket, because it is the one failure here that is about
        // what was configured rather than about the host.
        let len = destination.as_os_str().len();
        if len > MAX_DESTINATION_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a destination of {len} bytes cannot be addressed: a Unix socket path holds \
                     at most {MAX_DESTINATION_LEN} bytes"
                ),
            ));
        }
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
            // **The consumer is not there.** `ENOENT` is a recorder that has
            // not started yet — the startup order this module is built for —
            // and `ECONNREFUSED` is one that restarted and left its socket file
            // behind. **Transient**, so the member stays in the fan-out and
            // copies resume the moment the recorder binds. Counted as anything
            // else, the first send of a publisher that came up first ends the
            // reference stream for the life of the process.
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                Err(SinkError::ConsumerAbsent(error))
            }
            // Every other failure — a permission that will not change, a path
            // that is not a socket — is not transient, and the member is
            // dropped. That is the designed outcome and it is why the scope
            // below is `Channel`; the runtime names the dropped member between
            // ticks, which is where the silence becomes visible.
            Err(error) => Err(SinkError::Socket(error)),
        }
    }

    /// [`FailureScope::Channel`], and this is one of the two load-bearing lines
    /// of the file.
    ///
    /// A failure here costs a consumer of a copy and nothing else. Ending the
    /// process over it would turn an auxiliary outage into a feed outage, which
    /// is exactly the trade the tee exists to refuse. The other line is the
    /// `ConsumerAbsent` arm above: this one decides what a *dropped* member
    /// costs, and that one decides whether an absent recorder drops the member
    /// at all.
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
    fn no_consumer_is_a_transient_failure_the_tee_can_absorb_and_not_a_panic() {
        let path = std::env::temp_dir().join(format!("dz-absent-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        // A publisher whose recorder is not running must still publish. The
        // error is what `Tee` counts; what matters is that a send happened at
        // all and returned — and that it is **transient**, because a publisher
        // starting before its recorder is the ordinary order and a member
        // dropped on its first send is never restored.
        let error = sink.send(b"x").expect_err("nothing is listening");
        assert!(matches!(error, SinkError::ConsumerAbsent(_)), "{error}");
        assert!(
            error.is_transient(),
            "an absent recorder must not end the reference stream: {error}"
        );
        assert_eq!(sink.failure_scope(), FailureScope::Channel);
    }

    #[test]
    fn a_stale_socket_file_nobody_is_bound_to_is_transient_too() {
        // The other half of a recorder restart. The file is left behind, so the
        // path resolves and the send is refused — `ECONNREFUSED` rather than
        // `ENOENT`, and dropping the member over it would end the copies for
        // the life of the process.
        let dir = std::env::temp_dir().join(format!("dz-stale-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let path = dir.join("tee.sock");
        let _ = std::fs::remove_file(&path);
        {
            let _bound = UnixDatagram::bind(&path).expect("bind the consumer");
        }
        // Dropped without unlinking, which is what a killed recorder leaves.
        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        let error = sink.send(b"x").expect_err("nobody is bound to the path");
        assert!(matches!(error, SinkError::ConsumerAbsent(_)), "{error}");
        assert!(error.is_transient(), "{error}");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn a_recorder_that_starts_late_gets_every_datagram_after_it_binds() {
        // The property the whole transient decision exists for, asserted end to
        // end: the first send is refused because nothing is listening, and the
        // second arrives — through the same sink, which a fan-out that had
        // dropped the member would no longer be offering anything.
        let dir = std::env::temp_dir().join(format!("dz-late-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        let path = dir.join("tee.sock");
        let _ = std::fs::remove_file(&path);

        let mut sink = ReferenceStream::open("tee", &path).expect("an unbound socket");
        assert!(matches!(
            sink.send(b"missed"),
            Err(SinkError::ConsumerAbsent(_))
        ));

        let consumer = UnixDatagram::bind(&path).expect("the recorder starts");
        sink.send(b"kept").expect("the copy after it started");
        let mut buf = [0u8; 64];
        let n = consumer.recv(&mut buf).expect("the datagram");
        assert_eq!(&buf[..n], b"kept");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn a_destination_too_long_to_address_is_refused_at_open() {
        // At `open`, not at the first `send_to`. A path over the `sun_path`
        // limit fails identically on every datagram forever, so met at the send
        // it is a publisher that started cleanly and copies nothing.
        let path = std::path::PathBuf::from("/tmp").join("x".repeat(MAX_DESTINATION_LEN));
        let error = ReferenceStream::open("tee", &path).expect_err("over the limit");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains(&MAX_DESTINATION_LEN.to_string()),
            "the refusal names the limit: {error}"
        );
    }

    #[test]
    fn a_destination_at_the_limit_opens() {
        // The boundary, from the other side, so the check is the limit and not
        // an off-by-one below it.
        let path = std::path::PathBuf::from(format!("/{}", "x".repeat(MAX_DESTINATION_LEN - 1)));
        assert_eq!(path.as_os_str().len(), MAX_DESTINATION_LEN);
        ReferenceStream::open("tee", &path).expect("exactly at the limit");
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
