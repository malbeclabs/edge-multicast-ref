//! The archived object a derivation reads, behind a trait.
//!
//! **Archived objects are where this starts, and that is deliberate.** A
//! venue-side observation needs a receive path, and the two transports a venue
//! would use for one — a session transport and a polled one — are each their own
//! design. Deriving from objects rather than from a socket is what restores
//! `(object key, sha256)` idempotence, makes the object the batch boundary, and
//! keeps the bytes so that a mapping defect found next month can be
//! re-examined with a corrected adapter. It also means nothing here waits on
//! either transport.
//!
//! The trait is what keeps every test of this crate free of a filesystem, a
//! privilege and a network, exactly as `PayloadArchive` does for the offline
//! re-lowering.

use dz_adapter_core::ConnectionId;
use dz_recorder_archive::open_sealed;
use dz_recorder_archive::upstream::{UpstreamFormatError, UpstreamMessage, UpstreamObjectReader};
use std::io::Read;

/// Everything a derivation needs to know about an object other than its bytes.
///
/// **The key and the digest are handed in and never computed here.** They are
/// what a re-derivation is idempotent on, so they come from the manifest the
/// publication wrote: a derivation that hashed the bytes it had just
/// decompressed would be answering a different question, and a truncated object
/// would still have a digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueObjectId {
    /// The Hive-partitioned key the object landed under.
    pub object_key: String,
    /// The digest of the object that landed, from its manifest.
    pub object_sha256: String,
    /// Which observation point this recording is.
    pub observation: String,
    pub env: String,
    /// The feed specification whose instruments this recording covers.
    pub feed: String,
    /// The connections the caller's own transport declares, by the labels it
    /// declared them under.
    ///
    /// **The caller's and not the object's**, and that is the whole reason this
    /// field exists. `Payload::connection` is a `ConnectionId`, which is a
    /// `&'static str` because it is declared at startup and used as a metric
    /// label — so a name read out of a file cannot become one. The derivation
    /// resolves each recorded name against this set, and a recorded name that
    /// is not in it is a refusal naming the object rather than a payload
    /// attributed to nothing.
    pub connections: Vec<ConnectionId>,
}

impl VenueObjectId {
    /// The declared connection with this name, or `None`.
    ///
    /// **A name-only lookup, and it is unambiguous because the format makes the
    /// name unique.** `UpstreamSegmentWriter::open` refuses to declare one name
    /// twice and `UpstreamObjectReader::open` refuses to read a header that
    /// does, so a recorded name resolves to at most one declared connection.
    /// Without that, this would silently attribute the second entry's records
    /// to the first connection — which is why the refusal is in the format and
    /// not a check here: a caller that resolved a duplicate to *the first one*
    /// would be producing rows, not an error.
    #[must_use]
    pub fn connection(&self, name: &str) -> Option<ConnectionId> {
        self.connections
            .iter()
            .copied()
            .find(|declared| declared.as_str() == name)
    }
}

/// An archived object of upstream messages, in the order the transport yielded
/// them.
///
/// **The ordering is a requirement and not a convenience.** An adapter keeps a
/// book, so an object replayed out of order re-derives a different book and
/// every row that comes out of it describes a market that never happened. An
/// implementation that cannot guarantee receive order cannot be used here, and
/// should say so rather than approximate it.
pub trait VenueObject {
    /// What the object is, as its manifest states it.
    fn id(&self) -> &VenueObjectId;

    /// The archive format the object is written in, **as the object itself
    /// states it**.
    ///
    /// The value read out of the object's own header, never a reader's own
    /// constant and never a manifest's claim. It is what the `format_version`
    /// column holds, and that column exists to expose a reader that derived a
    /// window at one version from an object written at another — which an
    /// implementation answering with its build's constant makes impossible to
    /// see.
    fn format_version(&self) -> u16;

    /// The connections the object's own header declares, in the order its
    /// records index them.
    fn declared_connections(&self) -> Vec<String>;

    /// The next message, or `Ok(None)` at a clean end of the object.
    ///
    /// # Errors
    ///
    /// [`UpstreamFormatError`], naming the object. A truncated object is a
    /// refusal and never a short read: a silent stop turns a segment somebody
    /// cut into a venue that went quiet.
    fn next_message(&mut self) -> Result<Option<UpstreamMessage<'_>>, UpstreamFormatError>;
}

/// An object read out of the archive.
#[derive(Debug)]
pub struct ArchivedVenueObject<R: Read> {
    id: VenueObjectId,
    reader: UpstreamObjectReader<R>,
}

impl<R: Read> ArchivedVenueObject<R> {
    /// Opens an archived object over bytes that **are already** an upstream
    /// object.
    ///
    /// The bytes are handed to the reader as they are, so this is the
    /// constructor for a caller that has already decoded — and for a test over
    /// a segment it wrote itself. A published object is compressed by default,
    /// and reaching for this with the bytes of one produces
    /// [`UpstreamFormatError::NotAnUpstreamObject`], because the first eight
    /// bytes of a compressed object are a zstd frame magic and not `DZUPSTRM`.
    /// [`open_published`](ArchivedVenueObject::open_published) is the one that
    /// takes the object as it landed.
    ///
    /// # Errors
    ///
    /// [`UpstreamFormatError`] when the bytes are not an upstream object of a
    /// version this build reads.
    pub fn open(id: VenueObjectId, bytes: R) -> Result<Self, UpstreamFormatError> {
        let reader = UpstreamObjectReader::open(id.object_key.clone(), bytes)?;
        Ok(Self { id, reader })
    }
}

impl<'a> ArchivedVenueObject<Box<dyn Read + 'a>> {
    /// Opens the object **as it landed**, decoding it as its own key names it.
    ///
    /// The constructor for the ordinary path: `publish` compresses by default
    /// and puts the suffix on the key, so an object fetched out of the store is
    /// a `.dzus.zst` more often than a `.dzus` and a reader handed the raw bytes
    /// of one refuses them as *not an upstream object*.
    ///
    /// **The key decides, and the archive tier answers.** The suffix is what
    /// the compressor wrote, `dz_recorder_archive::open_sealed` is where that
    /// is read, and the manifest's key is the thing a caller already holds — so
    /// nothing here has a second opinion about what `.zst` means, exactly as
    /// nothing here has one about how an object is compressed.
    ///
    /// # Errors
    ///
    /// [`UpstreamFormatError::Io`], naming the object, when the frame header
    /// cannot be read; then [`UpstreamFormatError`] as
    /// [`open`](ArchivedVenueObject::open) states it.
    pub fn open_published<R: Read + 'a>(
        id: VenueObjectId,
        bytes: R,
    ) -> Result<Self, UpstreamFormatError> {
        let decoded =
            open_sealed(&id.object_key, bytes).map_err(|source| UpstreamFormatError::Io {
                object_key: id.object_key.clone(),
                source,
            })?;
        Self::open(id, decoded)
    }
}

impl<R: Read> VenueObject for ArchivedVenueObject<R> {
    fn id(&self) -> &VenueObjectId {
        &self.id
    }

    fn format_version(&self) -> u16 {
        // The object's **own header**, through the reader that read it — not
        // this build's constant, and not a manifest beside the object that
        // could disagree with the bytes.
        //
        // The two are equal for every object this build admits, because `open`
        // refuses any other version. Answering with the constant anyway is the
        // defect: the day a build reads a version it did not write, it would
        // stamp its own number on every row of the older object and the one
        // disagreement the `format_version` column exists to expose would be
        // the one thing it could never show.
        self.reader.format_version()
    }

    fn declared_connections(&self) -> Vec<String> {
        self.reader
            .connections()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    fn next_message(&mut self) -> Result<Option<UpstreamMessage<'_>>, UpstreamFormatError> {
        self.reader.next_message()
    }
}
