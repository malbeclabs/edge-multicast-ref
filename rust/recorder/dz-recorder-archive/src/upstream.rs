//! The second archive shape: a venue's own upstream messages, length-delimited.
//!
//! The rest of this crate writes pcapng, one Enhanced Packet Block per
//! datagram. That shape cannot hold what arrives where a venue's own bytes do:
//! there are no link headers to record, no destination group, no port role, no
//! capture-handle drop count, and no datagram boundaries to preserve. What
//! there is, per message, is the bytes exactly as they arrived, the connection
//! that delivered them, and a receive stamp — which is `Payload`'s own field
//! set, because that is what the adapter will be handed and a second shape
//! would be a second definition of it.
//!
//! # This is not the normalized-event record encoding, and that is the point
//!
//! `dz-recorder-relower` already reads a window of upstream payloads back
//! through `PayloadArchive`, and the offline comparison it performs has a
//! record encoding of its own for the **normalized events** either side of the
//! diff. That encoding is deliberately not reused here.
//!
//! It sits *downstream* of a venue's decode. An archive of normalized events is
//! an archive of what one build of one adapter made of the venue's bytes, so a
//! mapping defect found next month cannot be re-examined against it: the
//! evidence has already been through the thing under suspicion. Keeping raw
//! bytes is what makes a re-derivation with a corrected adapter possible, which
//! is the whole reason the offline re-lowering exists on the datagram side.
//!
//! The two coexist and neither is the other's substitute — raw upstream bytes
//! as the evidence, normalized events as the reference a re-lowering diffs
//! against. `UPSTREAM-OBJECT-FORMAT.md` beside this crate states the layout and
//! says which is which, so that the answer is written down rather than inferred
//! from whichever reader somebody opens first.
//!
//! # Nothing here is a capture
//!
//! A capture is a receive path over a socket that observes datagrams, counts
//! what the handle dropped and records link headers. A venue-side recording is
//! none of the three, so the word is not used for it anywhere in this module.
//!
//! # What is reused rather than written twice
//!
//! [`Compression`], [`RotationPolicy`](crate::rotate::RotationPolicy),
//! [`object_key`](crate::object_key::object_key) and
//! [`seal`](crate::compress::seal) are the archive tier's own policy for how an
//! object is compressed, when a segment rotates, where the object lands and how
//! it is digested. A second set of answers to those four questions is how two
//! archives in one repository come to disagree about retention, so this module
//! declares none of them: what it adds is the record layout and nothing else.
//! The manifest beside the object is written through the same
//! `write_and_sync` the pcapng side publishes with, so there is one place the
//! `sync_all` before the rename can be forgotten rather than two.
//!
//! The object key and its `sha256` are what a derivation is idempotent on, so
//! they are produced by [`publish`] at publication and are **not** derived at
//! read time: a reader that hashed the bytes it had just decompressed would be
//! answering a different question from the one the manifest answers, and a
//! truncated object would still have a digest.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use dz_recorder_core::{RecvTsKind, SinkError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compress::{seal, write_and_sync, Compression};
use crate::object_key::object_key;

/// The eight bytes at the front of every upstream object.
///
/// A magic and not a bare version number: the two archive shapes land in one
/// object store under one shipper, so a reader handed the wrong one has to say
/// so rather than read a pcapng section header block as a record length.
pub const UPSTREAM_MAGIC: [u8; 8] = *b"DZUPSTRM";

/// The layout this module writes and the only one it reads.
///
/// Stated in the object rather than inferred from a file name, because a name
/// is a shipper's to change and the bytes are not.
pub const UPSTREAM_FORMAT_VERSION: u16 = 1;

/// The largest message the format admits, at 8 MiB.
///
/// A bound and not a convenience. The length prefix is read out of a file that
/// may be damaged, and an unbounded length is an allocation an attacker — or a
/// half-written segment — chooses. Eight mebibytes is above the largest thing a
/// venue's own transport delivers in one piece: a full book response over a
/// polled transport is the big case, and it is measured in hundreds of
/// kilobytes.
pub const MAX_UPSTREAM_MESSAGE_BYTES: u32 = 8 << 20;

/// The most connections one object may declare.
///
/// A connection is declared at startup by the venue's own binary, so the set is
/// small and known before a byte is written. The cap is here for the same
/// reason the message cap is: the count is read out of a file that may be
/// damaged.
pub const MAX_UPSTREAM_CONNECTIONS: u16 = 256;

/// The extension an upstream object lands under, before compression.
///
/// Not `pcapng`, and that is checked by a test rather than left to a reader: an
/// object store holding both shapes has the name as its only cheap
/// discriminator, and a shipper that put one under the other's extension would
/// hand a pcapng reader bytes it cannot refuse gracefully.
pub const UPSTREAM_OBJECT_EXTENSION: &str = "dzus";

/// The extension of the object that lands, compression included.
#[must_use]
pub fn upstream_object_extension(compression: Compression) -> String {
    format!("{UPSTREAM_OBJECT_EXTENSION}{}", compression.suffix())
}

/// One upstream connection, as the object header declares it.
///
/// The receive-stamp kind is here rather than on every record because it is a
/// property of the connection and not of a message: a transport stamps every
/// payload the same way — the kernel does it or the transport does — which is
/// what `Payload` says in refusing to carry the distinction per payload.
/// Repeating it per record would be a third copy of that taxonomy and a place
/// for two of them to disagree inside one object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamConnection {
    /// The venue's own label for the connection, as its binary declared it.
    ///
    /// **Unique within one object's header**, which
    /// [`UpstreamSegmentWriter::open`] refuses to break and
    /// [`UpstreamObjectReader::open`] refuses to read past: the name is the
    /// whole of a connection's identity here, so two entries sharing one make
    /// every record on the second attributable to the first.
    pub name: String,
    /// How this connection's receive stamps were obtained.
    pub recv_ts_kind: RecvTsKindLabel,
}

impl UpstreamConnection {
    /// A connection declared with the stamp kind its transport produces.
    #[must_use]
    pub fn new(name: impl Into<String>, recv_ts_kind: RecvTsKind) -> Self {
        Self {
            name: name.into(),
            recv_ts_kind: RecvTsKindLabel::of(recv_ts_kind),
        }
    }
}

/// [`RecvTsKind`] as the object encodes it and the manifest spells it, which is
/// `dz-recorder-core`'s own enumeration and never a second one.
///
/// A byte in the object header and a token in the manifest, both from
/// [`dz_recorder_core::RecvTsKindLabel`] — the same type the `recv_ts_kind`
/// column holds on the publisher side. Re-exported rather than restated: two
/// enumerations with one meaning is how an archive comes to hold a kernel stamp
/// under a name that means something else, and how a rename on one side leaves a
/// query across the two archives silently returning the rows of one of them.
pub use dz_recorder_core::RecvTsKindLabel;

/// An upstream object could not be read as one.
///
/// **Every variant names the object.** A derivation runs over objects a shipper
/// moved, so the one thing an operator needs from any of these is which object
/// to go and look at — and a reader that reported only *unexpected end of file*
/// would send somebody to read every object in the partition.
#[derive(Debug, Error)]
pub enum UpstreamFormatError {
    #[error("{object_key} does not begin with an upstream object header")]
    NotAnUpstreamObject { object_key: String },

    #[error("{object_key} states format version {version}, and this reader knows {known}")]
    UnsupportedVersion {
        object_key: String,
        version: u16,
        known: u16,
    },

    /// The object ends in the middle of something.
    ///
    /// **A refusal and never a short read**, which is the whole reason this
    /// variant carries a count: a reader that stopped quietly at a half-written
    /// record would hand a derivation fewer messages than were written and
    /// nothing anywhere would say so. The rows that came out would describe a
    /// venue that went quiet at the moment the segment was cut.
    #[error(
        "{object_key} ends inside {what} after {messages_read} messages: \
         {wanted} bytes wanted, {got} present"
    )]
    Truncated {
        object_key: String,
        what: &'static str,
        messages_read: u64,
        wanted: u64,
        got: u64,
    },

    #[error(
        "{object_key} declares a {len}-byte message after {messages_read} messages, \
         over the {MAX_UPSTREAM_MESSAGE_BYTES}-byte bound"
    )]
    MessageTooLarge {
        object_key: String,
        messages_read: u64,
        len: u32,
    },

    #[error(
        "{object_key} declares {count} connections, over the {MAX_UPSTREAM_CONNECTIONS} bound"
    )]
    TooManyConnections { object_key: String, count: u16 },

    #[error(
        "{object_key} attributes the message after {messages_read} to connection \
         {index}, and its header declares {declared}"
    )]
    UnknownConnection {
        object_key: String,
        messages_read: u64,
        index: u16,
        declared: usize,
    },

    #[error("{object_key} declares a receive-stamp kind this reader does not know: {byte}")]
    UnknownRecvTsKind { object_key: String, byte: u8 },

    #[error("{object_key} declares a connection name that is not UTF-8")]
    ConnectionNameNotUtf8 { object_key: String },

    /// Two header entries share a name.
    ///
    /// **A record indexed to either one is attributable to neither.** A
    /// derivation resolves a recorded connection back to the caller's declared
    /// set by name — `VenueObjectId::connection` is a name lookup, because a
    /// `ConnectionId` is a `&'static str` and a name read out of a file cannot
    /// become one — so two entries with one name make that lookup ambiguous and
    /// every record on the second entry is attributed to the first. The two
    /// entries may even declare different receive-stamp kinds, and then the
    /// `recv_ts_kind` a row is checked against is the wrong one.
    ///
    /// Both indices are named, because *the name is a duplicate* sends somebody
    /// to read the header to find out which two entries it is.
    #[error("{object_key} declares the connection name {name:?} at entries {first} and {second}")]
    DuplicateConnectionName {
        object_key: String,
        name: String,
        first: usize,
        second: usize,
    },

    #[error("reading {object_key}: {source}")]
    Io {
        object_key: String,
        #[source]
        source: io::Error,
    },
}

/// One archived upstream message, borrowed from the reader's own buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamMessage<'a> {
    /// The connection that delivered it, as the object's header names it.
    pub connection: &'a str,
    /// How this connection's receive stamps were obtained.
    pub recv_ts_kind: RecvTsKind,
    /// When the transport received it, on the host that was recording.
    pub recv_ts_ns: u64,
    /// The venue's bytes, verbatim. Nothing here normalises them: a message the
    /// adapter refuses is evidence, and repairing it destroys the evidence.
    pub bytes: &'a [u8],
}

/// The open segment: upstream messages appended in the order they arrived.
///
/// Generic over the writer for the reason the pcapng segment writer is — the
/// tests write into a `Vec<u8>` and need neither a filesystem nor a privilege
/// to hold the format to its own layout.
#[derive(Debug)]
pub struct UpstreamSegmentWriter<W: Write> {
    inner: W,
    connections: Vec<UpstreamConnection>,
    bytes_written: u64,
    message_count: u64,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
}

impl<W: Write> UpstreamSegmentWriter<W> {
    /// Opens a segment and writes the header, declaring the connections whose
    /// messages it may carry.
    ///
    /// The connections are declared up front because they already are: a
    /// `ConnectionId` is a `&'static str` precisely so that a transport's
    /// connections can be named at startup and pre-created as metric labels. A
    /// table in the header therefore costs nothing and spells each name once
    /// rather than once per message.
    ///
    /// **The names have to be distinct**, and this is where that is fixed. The
    /// name is the whole of a connection's identity in this format: a record
    /// carries an index into the table below, and everything above the reader
    /// resolves that index to a name and the name back to the caller's own
    /// `ConnectionId`. Two entries with one name therefore produce records
    /// nothing can attribute — the second entry's records are attributed to the
    /// first connection, and the two entries may declare different
    /// receive-stamp kinds while they do it. Refused here rather than in the
    /// reader alone, because the object that lands is the only copy of the
    /// window it holds and a header nobody can read it back through cannot be
    /// repaired afterwards.
    ///
    /// # Errors
    ///
    /// [`SinkError::Encode`] when more than [`MAX_UPSTREAM_CONNECTIONS`] are
    /// declared, two of them share a name, or a name is longer than a `u16` can
    /// state; [`SinkError::Io`] when the header cannot be written.
    pub fn open(mut inner: W, connections: &[UpstreamConnection]) -> Result<Self, SinkError> {
        let count = u16::try_from(connections.len()).map_err(|_| {
            SinkError::Encode(format!(
                "{} connections declared, over the {MAX_UPSTREAM_CONNECTIONS} bound",
                connections.len()
            ))
        })?;
        if count > MAX_UPSTREAM_CONNECTIONS {
            return Err(SinkError::Encode(format!(
                "{count} connections declared, over the {MAX_UPSTREAM_CONNECTIONS} bound"
            )));
        }
        if let Some((first, second)) = first_duplicate_name(connections) {
            return Err(SinkError::Encode(format!(
                "the connection name {:?} is declared at entries {first} and {second}",
                connections[second].name
            )));
        }

        let mut header = Vec::with_capacity(16 + connections.len() * 16);
        header.extend_from_slice(&UPSTREAM_MAGIC);
        header.extend_from_slice(&UPSTREAM_FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&count.to_le_bytes());
        for connection in connections {
            let name_len = u16::try_from(connection.name.len()).map_err(|_| {
                SinkError::Encode(format!(
                    "the connection name {:?} is longer than a u16 can state",
                    connection.name
                ))
            })?;
            header.extend_from_slice(&name_len.to_le_bytes());
            header.push(connection.recv_ts_kind.as_byte());
            header.extend_from_slice(connection.name.as_bytes());
        }
        inner.write_all(&header).map_err(SinkError::Io)?;

        Ok(Self {
            inner,
            connections: connections.to_vec(),
            bytes_written: header.len() as u64,
            message_count: 0,
            start_ns: None,
            end_ns: None,
        })
    }

    /// Appends one message, exactly as it arrived.
    ///
    /// `connection` indexes the header's table. Order is receive order and this
    /// is where it is fixed: an adapter keeps a book, so an object replayed out
    /// of order re-derives a different book.
    ///
    /// # Errors
    ///
    /// [`SinkError::Encode`] when the connection is not one this segment
    /// declared, or the message is over [`MAX_UPSTREAM_MESSAGE_BYTES`];
    /// [`SinkError::Io`] when the record cannot be written.
    pub fn write_message(
        &mut self,
        connection: u16,
        recv_ts_ns: u64,
        bytes: &[u8],
    ) -> Result<(), SinkError> {
        if usize::from(connection) >= self.connections.len() {
            return Err(SinkError::Encode(format!(
                "connection {connection} is not one of the {} this segment declared",
                self.connections.len()
            )));
        }
        let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        if len > MAX_UPSTREAM_MESSAGE_BYTES {
            return Err(SinkError::Encode(format!(
                "a {} byte message, over the {MAX_UPSTREAM_MESSAGE_BYTES} byte bound",
                bytes.len()
            )));
        }

        self.inner
            .write_all(&connection.to_le_bytes())
            .map_err(SinkError::Io)?;
        self.inner
            .write_all(&recv_ts_ns.to_le_bytes())
            .map_err(SinkError::Io)?;
        self.inner
            .write_all(&len.to_le_bytes())
            .map_err(SinkError::Io)?;
        self.inner.write_all(bytes).map_err(SinkError::Io)?;

        self.bytes_written += RECORD_HEADER_LEN as u64 + u64::from(len);
        self.message_count += 1;
        self.start_ns = Some(self.start_ns.map_or(recv_ts_ns, |at| at.min(recv_ts_ns)));
        self.end_ns = Some(self.end_ns.map_or(recv_ts_ns, |at| at.max(recv_ts_ns)));
        Ok(())
    }

    /// Bytes this segment has put on disk, which is what the rotation policy is
    /// asked about.
    #[must_use]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Messages appended so far.
    #[must_use]
    pub const fn message_count(&self) -> u64 {
        self.message_count
    }

    /// The earliest receive stamp in the segment, or `None` while it holds
    /// nothing.
    ///
    /// `None` and never zero: zero is a stamp, and an empty segment does not
    /// have one. The pcapng side spends no `segment_seq` on an empty rotation
    /// for the same reason — a window nobody observed is not a window.
    #[must_use]
    pub const fn start_ns(&self) -> Option<u64> {
        self.start_ns
    }

    /// The latest receive stamp in the segment, or `None` while it holds
    /// nothing.
    #[must_use]
    pub const fn end_ns(&self) -> Option<u64> {
        self.end_ns
    }

    /// The connections this segment declared.
    #[must_use]
    pub fn connections(&self) -> &[UpstreamConnection] {
        &self.connections
    }

    /// Flushes and hands the writer back.
    ///
    /// # Errors
    ///
    /// [`SinkError::Io`] when the buffered bytes cannot be written. The partial
    /// object is the only copy of the window it holds, so a caller keeps it
    /// rather than removing it — which is what the pcapng side's own rotation
    /// does with a segment it could not close.
    pub fn finish(mut self) -> Result<W, SinkError> {
        self.inner.flush().map_err(SinkError::Io)?;
        Ok(self.inner)
    }
}

/// `connection` + `recv_ts_ns` + `len`.
const RECORD_HEADER_LEN: usize = 2 + 8 + 4;

/// The first pair of entries that share a name, as `(first, second)`.
///
/// One function for the writer's refusal and the reader's, so that the two
/// cannot come to disagree about what a duplicate is. Quadratic over a table
/// the format bounds at [`MAX_UPSTREAM_CONNECTIONS`], which is a set a venue's
/// own binary declares at startup — a hash map here would allocate to answer a
/// question about at most 256 short strings, once per object.
fn first_duplicate_name(connections: &[UpstreamConnection]) -> Option<(usize, usize)> {
    connections.iter().enumerate().find_map(|(second, entry)| {
        connections[..second]
            .iter()
            .position(|earlier| earlier.name == entry.name)
            .map(|first| (first, second))
    })
}

/// An upstream object read back, one message at a time.
///
/// Holds the object key so that every refusal can name it. The key rather than
/// a path: what a derivation is idempotent on is `(object key, sha256)`, so the
/// key is what an operator has in hand when a row looks wrong, and a local path
/// is a detail of whichever process fetched it.
#[derive(Debug)]
pub struct UpstreamObjectReader<R: Read> {
    inner: R,
    object_key: String,
    connections: Vec<UpstreamConnection>,
    buffer: Vec<u8>,
    messages_read: u64,
    /// Set once a refusal has been returned, so that a caller that keeps asking
    /// is told the same thing rather than resuming mid-object.
    refused: bool,
}

impl<R: Read> UpstreamObjectReader<R> {
    /// Reads the header and prepares to read messages.
    ///
    /// # Errors
    ///
    /// [`UpstreamFormatError`], naming the object: the magic is not ours, the
    /// format version is one this build does not know, the header is truncated,
    /// or it declares more connections than the bound admits.
    pub fn open(object_key: impl Into<String>, mut inner: R) -> Result<Self, UpstreamFormatError> {
        let object_key = object_key.into();

        let mut fixed = [0u8; 12];
        read_exact(&mut inner, &mut fixed, &object_key, "the object header", 0)?;
        if fixed[..8] != UPSTREAM_MAGIC {
            return Err(UpstreamFormatError::NotAnUpstreamObject { object_key });
        }
        let version = u16::from_le_bytes([fixed[8], fixed[9]]);
        if version != UPSTREAM_FORMAT_VERSION {
            return Err(UpstreamFormatError::UnsupportedVersion {
                object_key,
                version,
                known: UPSTREAM_FORMAT_VERSION,
            });
        }
        let count = u16::from_le_bytes([fixed[10], fixed[11]]);
        if count > MAX_UPSTREAM_CONNECTIONS {
            return Err(UpstreamFormatError::TooManyConnections { object_key, count });
        }

        let mut connections = Vec::with_capacity(usize::from(count));
        for _ in 0..count {
            let mut entry = [0u8; 3];
            read_exact(
                &mut inner,
                &mut entry,
                &object_key,
                "a connection declaration",
                0,
            )?;
            let name_len = usize::from(u16::from_le_bytes([entry[0], entry[1]]));
            let Some(recv_ts_kind) = RecvTsKindLabel::from_byte(entry[2]) else {
                return Err(UpstreamFormatError::UnknownRecvTsKind {
                    object_key,
                    byte: entry[2],
                });
            };
            let mut name = vec![0u8; name_len];
            read_exact(&mut inner, &mut name, &object_key, "a connection name", 0)?;
            let Ok(name) = String::from_utf8(name) else {
                return Err(UpstreamFormatError::ConnectionNameNotUtf8 { object_key });
            };
            connections.push(UpstreamConnection { name, recv_ts_kind });
        }
        // The invariant the writer above refuses to break, checked again on the
        // way in: a derivation runs over objects a shipper moved, and the build
        // that wrote one is not the build reading it. An index that resolves to
        // an ambiguous name is worse than a refusal, because the rows it
        // produces name a connection they did not arrive on.
        if let Some((first, second)) = first_duplicate_name(&connections) {
            return Err(UpstreamFormatError::DuplicateConnectionName {
                name: connections[second].name.clone(),
                object_key,
                first,
                second,
            });
        }

        Ok(Self {
            inner,
            object_key,
            connections,
            buffer: Vec::new(),
            messages_read: 0,
            refused: false,
        })
    }

    /// The object key every refusal names.
    #[must_use]
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    /// The connections this object declares, in the order its records index
    /// them.
    #[must_use]
    pub fn connections(&self) -> &[UpstreamConnection] {
        &self.connections
    }

    /// Messages handed out so far.
    #[must_use]
    pub const fn messages_read(&self) -> u64 {
        self.messages_read
    }

    /// The next message, or `Ok(None)` at a **clean** end of the object.
    ///
    /// Clean means the object ended exactly where a record ended. Anything else
    /// is [`UpstreamFormatError::Truncated`] and never `Ok(None)`: a silent stop
    /// at a half-written record turns a segment somebody cut into a venue that
    /// went quiet, and the rows either way are indistinguishable.
    ///
    /// # Errors
    ///
    /// [`UpstreamFormatError`], naming the object.
    pub fn next_message(&mut self) -> Result<Option<UpstreamMessage<'_>>, UpstreamFormatError> {
        if self.refused {
            return Err(UpstreamFormatError::Truncated {
                object_key: self.object_key.clone(),
                what: "a record this reader has already refused",
                messages_read: self.messages_read,
                wanted: 0,
                got: 0,
            });
        }

        let mut header = [0u8; RECORD_HEADER_LEN];
        match read_some(&mut self.inner, &mut header) {
            Err(source) => {
                self.refused = true;
                return Err(UpstreamFormatError::Io {
                    object_key: self.object_key.clone(),
                    source,
                });
            }
            // The one place an end of file is an answer rather than a refusal:
            // no byte of a record header had been read, so the object ended
            // where the previous record did.
            Ok(0) => return Ok(None),
            Ok(got) if got < RECORD_HEADER_LEN => {
                self.refused = true;
                return Err(UpstreamFormatError::Truncated {
                    object_key: self.object_key.clone(),
                    what: "a record header",
                    messages_read: self.messages_read,
                    wanted: RECORD_HEADER_LEN as u64,
                    got: got as u64,
                });
            }
            Ok(_) => {}
        }

        let index = u16::from_le_bytes([header[0], header[1]]);
        let recv_ts_ns = u64::from_le_bytes(
            header[2..10]
                .try_into()
                .expect("range width matches the target array"),
        );
        let len = u32::from_le_bytes(
            header[10..14]
                .try_into()
                .expect("range width matches the target array"),
        );

        if len > MAX_UPSTREAM_MESSAGE_BYTES {
            self.refused = true;
            return Err(UpstreamFormatError::MessageTooLarge {
                object_key: self.object_key.clone(),
                messages_read: self.messages_read,
                len,
            });
        }
        let Some(connection) = self.connections.get(usize::from(index)) else {
            self.refused = true;
            return Err(UpstreamFormatError::UnknownConnection {
                object_key: self.object_key.clone(),
                messages_read: self.messages_read,
                index,
                declared: self.connections.len(),
            });
        };

        self.buffer.clear();
        self.buffer.resize(len as usize, 0);
        match read_some(&mut self.inner, &mut self.buffer) {
            Err(source) => {
                self.refused = true;
                return Err(UpstreamFormatError::Io {
                    object_key: self.object_key.clone(),
                    source,
                });
            }
            Ok(got) if got < self.buffer.len() => {
                self.refused = true;
                return Err(UpstreamFormatError::Truncated {
                    object_key: self.object_key.clone(),
                    what: "a message body",
                    messages_read: self.messages_read,
                    wanted: u64::from(len),
                    got: got as u64,
                });
            }
            Ok(_) => {}
        }

        self.messages_read += 1;
        Ok(Some(UpstreamMessage {
            connection: &connection.name,
            recv_ts_kind: connection.recv_ts_kind.kind(),
            recv_ts_ns,
            bytes: &self.buffer,
        }))
    }
}

/// What a reader can answer about an upstream object without opening it.
///
/// The same arrangement as the pcapng side's [`SegmentManifest`](crate::SegmentManifest):
/// every field is computed from state the writer already held while the segment
/// was open, and the key, the digest and the byte count are filled in at
/// publication. Nothing here re-reads the object — a manifest produced by
/// reading the object back would be a second decode of the same bytes, and the
/// only thing it could add is a second opportunity to disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamManifest {
    /// The layout the object is written in, restated where a reader deciding
    /// whether it can read the object at all will look.
    pub format_version: u16,
    pub site: String,
    pub recorder: String,
    pub env: String,
    /// The feed specification's name — never a venue. It is the first partition
    /// of the object key, so it is the recorder's to state and not a shipper's
    /// to infer.
    pub feed: String,
    /// Which observation point this recording is, as `site` names a host.
    ///
    /// Carried in the manifest so that a derivation is handed it rather than
    /// composing it from two other fields in a way each caller could compose
    /// differently.
    pub observation: String,
    /// The connections the object declares, in the order its records index
    /// them.
    pub connections: Vec<UpstreamConnection>,
    pub segment_seq: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    pub message_count: u64,
    /// Filled in at publication.
    pub object_key: String,
    /// Filled in at publication. Hex, lower case, of the bytes that landed.
    pub sha256: String,
    /// Filled in at publication.
    pub byte_count: u64,
}

impl UpstreamManifest {
    /// The manifest as the file beside the object holds it.
    ///
    /// # Errors
    ///
    /// [`SinkError::Encode`] when the manifest cannot be serialised.
    pub fn to_json(&self) -> Result<String, SinkError> {
        serde_json::to_string_pretty(self).map_err(|e| SinkError::Encode(e.to_string()))
    }
}

/// A published upstream object and the manifest that describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedUpstreamObject {
    /// Where the object landed.
    pub path: PathBuf,
    /// The manifest, with the key, the digest and the byte count filled in.
    pub manifest: UpstreamManifest,
}

/// Compresses, digests and publishes one closed upstream segment.
///
/// Reuses [`seal`](crate::compress::seal) — the archive tier's own
/// compress-and-hash — and [`object_key`], its own layout, so that the two
/// archive shapes cannot come to disagree about either. What is added is the
/// name: [`upstream_object_extension`] rather than the pcapng one.
///
/// The digest is of the object **that lands**, compression included, because
/// integrity and idempotent reprocessing key on `(object key, sha256)`.
///
/// # Errors
///
/// [`SinkError`] when the segment cannot be read, the object cannot be written,
/// or the manifest cannot be serialised or moved. The segment is left where it
/// is on failure: a partial publication that also removed the segment
/// would destroy the only copy of the window.
pub fn publish(
    source: &Path,
    completed_dir: &Path,
    draft: UpstreamManifest,
    compression: Compression,
) -> Result<PublishedUpstreamObject, SinkError> {
    fs::create_dir_all(completed_dir).map_err(SinkError::Io)?;

    let file_name = format!(
        "{}-{}-{}.{}",
        draft.start_ns,
        draft.end_ns,
        draft.segment_seq,
        upstream_object_extension(compression)
    );
    let manifest_name = format!("{file_name}.manifest.json");

    let object_tmp = completed_dir.join(format!(".{file_name}.tmp"));
    let manifest_tmp = completed_dir.join(format!(".{manifest_name}.tmp"));

    let (byte_count, sha256) = seal(source, &object_tmp, compression)?;

    let mut manifest = draft;
    manifest.object_key = object_key(
        &manifest.feed,
        &manifest.env,
        &manifest.site,
        &manifest.recorder,
        manifest.start_ns,
        &file_name,
    );
    manifest.byte_count = byte_count;
    manifest.sha256 = crate::compress::hex(&sha256);

    let json = manifest.to_json()?;
    write_and_sync(&manifest_tmp, json.as_bytes())?;

    // The manifest lands first and the object second, as the pcapng side does
    // it, so a manifest that survives a failed object move is cleaned up rather
    // than left as a row pointing at nothing.
    let manifest_path = completed_dir.join(&manifest_name);
    fs::rename(&manifest_tmp, &manifest_path).map_err(SinkError::Io)?;
    let path = completed_dir.join(&file_name);
    if let Err(e) = fs::rename(&object_tmp, &path) {
        let _ = fs::remove_file(&manifest_path);
        let _ = fs::remove_file(&object_tmp);
        return Err(SinkError::Io(e));
    }

    Ok(PublishedUpstreamObject { path, manifest })
}

/// Fills `buf` or says how much was there, without treating a short read as an
/// end.
///
/// `Read::read` is allowed to return fewer bytes than asked for at any time —
/// over a pipe, over a decompressor, at a filesystem boundary — so a single
/// call is not a length check. Looping until either the buffer is full or the
/// reader is genuinely at its end is what makes the difference between a clean
/// end and a truncation observable at all.
fn read_some<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn read_exact<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
    object_key: &str,
    what: &'static str,
    messages_read: u64,
) -> Result<(), UpstreamFormatError> {
    let got = read_some(reader, buf).map_err(|source| UpstreamFormatError::Io {
        object_key: object_key.to_owned(),
        source,
    })?;
    if got < buf.len() {
        return Err(UpstreamFormatError::Truncated {
            object_key: object_key.to_owned(),
            what,
            messages_read,
            wanted: buf.len() as u64,
            got: got as u64,
        });
    }
    Ok(())
}
