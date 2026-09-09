//! `Reset Count` across restarts: the era store.
//!
//! The datagram header's `Reset Count` is how a publisher tells its
//! subscribers *forget what I told you*. A subscriber that sees it change
//! drops the book it had cached, the reference data it had accumulated and the
//! snapshot context it was assembling, and re-syncs from scratch. It is the
//! resolved shape for announcing a reset — the alternative, a message type
//! emitted at startup, is a thing three feeds answer three different ways and
//! two of them reserve the type ID it would need.
//!
//! Which makes this the whole point: **a publisher whose sequence series
//! restarts at 0 without its era changing has told subscribers nothing.** They
//! keep the stale book and apply fresh deltas onto it, and read the sequence
//! going backwards as reordering rather than as a restart. Since the series
//! restarts on every process start, the era must advance on every process
//! start, and that means it has to survive one.
//!
//! The era is keyed on the feed specification **and the shard**, because that
//! pair is what a subscriber is bound to. One process may publish many shards
//! of one specification, and an era drawn from one file per specification is an
//! era decided by how many other shards happen to be configured and in what
//! order — see [`EraStore`].

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use dz_edge_core::{Feed, ResetCount};

/// The era a channel instance that has no persisted history advertises.
///
/// **Not zero.** `ResetCount(0)` is what a channel that has never reset
/// advertises, and a publisher's first datagram has already reset any
/// subscriber that was listening to a previous incarnation of this feed. One is
/// also what a newly enabled feed advertises, which is the reason the store is
/// keyed per channel instance: a channel instance that has never published must
/// not inherit an era from one that has published for months, or its first
/// datagram claims a history it does not have. That argument was first made
/// about feeds and holds for a shard of a feed word for word.
pub const FIRST_ERA: u8 = 1;

/// The token every era file starts with. A version, so that a later format can
/// be told from a corrupt file of this one.
const FORMAT_TAG: &str = "era-v1";

/// Which shard of a feed specification an era belongs to.
///
/// One `[[feed]]` block is one shard's view of one specification, and that pair
/// owns an era. The block's three port roles share it, because a restart is one
/// event for the whole feed and every series it carries restarts together —
/// splitting them would be keying on the wrong thing.
///
/// The default shard and a named one are separate constructors rather than one
/// string, because they do not produce the same file name and the difference
/// is not decorative:
///
/// | Shard | Era file |
/// |---|---|
/// | [`Shard::DEFAULT`] | `<spec>.era` |
/// | [`Shard::named`] | `<spec>.<shard>.era` |
///
/// **A caller holding the default shard's name must pass [`Shard::DEFAULT`],
/// never [`Shard::named`] with that name.** A renamed era file reads as *no*
/// era file, which resolves to [`FIRST_ERA`]: a publisher that has been running
/// for months on era 7 would restart on era 1 and announce nothing, because a
/// subscriber detects a reset by inequality against what it last saw. That is
/// the silent corruption the corrupt-file refusal below exists to prevent,
/// delivered instead by an upgrade meant to be safe. It is also why the
/// configuration refuses a block that spells the default shard's name
/// explicitly: two spellings of one shard would be two era files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard<'a> {
    /// Absent for the default shard, whose file carries no shard component.
    name: Option<&'a str>,
}

impl<'a> Shard<'a> {
    /// The shard a document that names none resolves to. Its era file is
    /// `<spec>.era`, the name it carried before any document could name a
    /// shard, so an existing deployment's era is read rather than restarted.
    pub const DEFAULT: Self = Self { name: None };

    /// A shard the document named. Its era file is `<spec>.<shard>.era`, and it
    /// has no history the first time it is configured — [`FIRST_ERA`] is the
    /// right answer for a channel instance nothing has ever published under.
    ///
    /// The name becomes a path component, so it is checked where the path is
    /// built; see [`EraError::UnsafeShardName`].
    #[must_use]
    pub const fn named(name: &'a str) -> Self {
        Self { name: Some(name) }
    }

    /// The shard a resolved configuration names, given the token that
    /// configuration spells the default shard with.
    ///
    /// **This is what a caller holding a resolved shard name wants**, and
    /// [`Shard::named`] is what it wants only when it knows the name is not the
    /// default one. A document that names no shard resolves to the default
    /// shard's *token*, so the natural-looking `Shard::named(feed.shard)` hands
    /// every existing deployment `<spec>.default.era`, which reads as no file
    /// and restarts a publisher on era 7 at era 1. The token stays the caller's
    /// because it is one constant at the adapter boundary and a second copy
    /// here would be a second thing to spell differently.
    #[must_use]
    pub fn resolve(name: &'a str, default_token: &str) -> Self {
        if name == default_token {
            Self::DEFAULT
        } else {
            Self::named(name)
        }
    }
}

/// The persisted era of each channel instance this host publishes.
///
/// One small file per channel instance under a state directory, written before
/// that instance's first datagram and never touched again while the process
/// runs. The key is the pair a subscriber is bound to — a feed specification
/// and a [`Shard`] — so that each instance's era advances by exactly one per
/// start, its progression is its own history, and reordering the configuration
/// document means nothing. Keyed on the specification alone, one process
/// publishing N shards draws N different eras from one counter per start: which
/// era an instance gets is decided by its position in the document, the stride
/// changes when a shard is added or removed, and a channel instance is
/// eventually handed an era it has already published under — a restart no
/// subscriber is ever told about.
///
/// # Failure modes are the contract
///
/// The store exists to make a restart visible, so what it does when it cannot
/// tell is part of what it is for:
///
/// - **No file.** This channel instance has never published from this state
///   directory, so no subscriber holds an era for it, so nothing can collide:
///   the era is [`FIRST_ERA`]. This is also what a wiped state directory looks
///   like, and the collision that case risks is real but unresolvable — a store
///   with no record cannot distinguish *never ran* from *lost its memory*. An
///   operator who moves or clears the state directory has restarted the feed's
///   history and needs to know it.
/// - **A file that will not parse.** [`EraError::Corrupt`], and the publisher
///   does not start. This is the case where guessing is worst: a file exists,
///   so an era *was* in use, and picking one risks re-advertising the era
///   subscribers already hold state under. A subscriber's barrier fires on a
///   *change*, so re-using the previous era after a restart means no subscriber
///   ever drops its stale book — the exact silent corruption this file exists
///   to prevent, arrived at by the store that was meant to prevent it. Refusing
///   is loud, and an operator can repair it with one line or clear the
///   directory deliberately.
/// - **The write fails.** Also a refusal to start, and for the same reason: an
///   era that was not persisted is an era that will be re-used by the next
///   start.
///
/// A file that is *renamed* is the first of those three and not the second,
/// which is why the default shard keeps `<spec>.era`; see [`Shard`].
///
/// The file is written and `fsync`ed, and the directory `fsync`ed after the
/// rename, *before* the era is handed out. A crash between publishing under an
/// era and recording it would re-use that era on the next start, which is the
/// failure above.
pub struct EraStore {
    dir: PathBuf,
}

impl EraStore {
    /// Open, creating the state directory if it is not there.
    ///
    /// # Errors
    ///
    /// [`EraError::Io`] if the directory cannot be created.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, EraError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|source| EraError::Io {
            path: dir.clone(),
            source,
        })?;
        Ok(Self { dir })
    }

    /// Advance this shard's era of this feed, persist it, and return it.
    ///
    /// Called once per channel instance at startup, before that instance's
    /// first datagram, and the same value is handed to all three of its port
    /// roles. The value is persisted before it is returned, so a crash
    /// immediately after this call cannot leave the next start re-using it.
    ///
    /// # Errors
    ///
    /// [`EraError`]. Every variant is a refusal to start; see [`EraStore`].
    pub fn begin_era<F: Feed>(&self, shard: Shard<'_>) -> Result<ResetCount, EraError> {
        let path = self.path_for(F::NAME, shard)?;
        let era = match read_era(&path)? {
            None => FIRST_ERA,
            // **The era after 255 is 0, and that is not a lie.** The
            // specification anticipates this exact wrap and settles it: a
            // subscriber detects a reset by testing its last-seen value for
            // *inequality*, "any change, including the 255 to 0 wrap, is a
            // reset; never compare for ordering". So 0 is not a claim about
            // history a subscriber could be misled by - it is only ever read
            // against what that subscriber last saw on that channel instance.
            //
            // Skipping 0 was tried here first, on the reasoning that 0 is the
            // value a channel advertises before it has ever reset. That
            // reasoning reads the field as ordered, which is the one thing the
            // specification forbids - and it would have made this store's
            // sequence disagree with `ChannelSequence::begin_era`, which wraps.
            // Two era sequences for one channel is worse than either.
            Some(previous) => previous.wrapping_add(1),
        };
        self.write_era(&path, era)?;
        Ok(ResetCount(era))
    }

    /// This shard's persisted era of this feed without advancing it, or `None`
    /// for a channel instance with no history here. For a diagnostic, and for a
    /// check mode.
    ///
    /// It answers for one channel instance rather than for a specification,
    /// which is the question a diagnostic is actually asking: with several
    /// shards of one specification in one process, a per-specification answer
    /// describes none of them.
    ///
    /// # Errors
    ///
    /// [`EraError::Corrupt`] or [`EraError::Io`], as [`Self::begin_era`].
    pub fn persisted_era<F: Feed>(&self, shard: Shard<'_>) -> Result<Option<ResetCount>, EraError> {
        Ok(read_era(&self.path_for(F::NAME, shard)?)?.map(ResetCount))
    }

    /// The file a channel instance's era lives in.
    ///
    /// Both the feed name and the shard name become path components, so both
    /// are checked rather than trusted. `Feed::NAME` is a compile-time constant
    /// in the codec crates today, but a name that is not one path component is
    /// a directory traversal from a constant nobody thought of as one, and the
    /// check costs nothing at startup. The shard name arrives from the
    /// configuration document, which checks it at load; the check here is the
    /// second line of defence, kept because this is the function that builds
    /// the path.
    ///
    /// The default shard contributes no component, so its file keeps the name
    /// it has always had — see [`Shard`] for what a rename would cost.
    fn path_for(&self, name: &'static str, shard: Shard<'_>) -> Result<PathBuf, EraError> {
        if !is_one_path_component(name) {
            return Err(EraError::UnsafeFeedName { name });
        }
        let file = match shard.name {
            None => format!("{name}.era"),
            Some(shard) if is_one_path_component(shard) => format!("{name}.{shard}.era"),
            Some(shard) => {
                return Err(EraError::UnsafeShardName {
                    name: shard.to_owned(),
                })
            }
        };
        Ok(self.dir.join(file))
    }

    /// Write via a temporary file and a rename, so that a crash mid-write
    /// leaves either the previous era or the new one — never a half-written
    /// file, which is the [`EraError::Corrupt`] refusal above and would need an
    /// operator to start the publisher at all.
    fn write_era(&self, path: &Path, era: u8) -> Result<(), EraError> {
        let tmp = path.with_extension("era.tmp");
        let write = |path: &Path| -> io::Result<()> {
            let mut file = File::create(path)?;
            file.write_all(format!("{FORMAT_TAG} {era}\n").as_bytes())?;
            file.sync_all()
        };
        write(&tmp).map_err(|source| EraError::Io {
            path: tmp.clone(),
            source,
        })?;
        fs::rename(&tmp, path).map_err(|source| EraError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        // The rename itself has to reach the disk. Without this, a power loss
        // after a clean-looking startup leaves the directory entry pointing at
        // the *previous* era, and the next start re-uses an era that has
        // already published.
        File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|source| EraError::Io {
                path: self.dir.clone(),
                source,
            })
    }
}

/// One lowercase path component, at most 64 bytes of `[a-z0-9-]`.
///
/// The same rule for both halves of the file name, because a traversal is a
/// traversal whichever component carries it, and two rules would be two
/// opportunities to write the looser one.
fn is_one_path_component(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Read a channel instance's era file: `None` for absent, `Err` for present and
/// unreadable.
///
/// Read as bytes rather than as a string so that a file of arbitrary bytes is
/// diagnosed as corrupt — which is a refusal an operator must repair — instead
/// of as an I/O error, which reads like something a retry might fix.
fn read_era(path: &Path) -> Result<Option<u8>, EraError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(EraError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let corrupt = |what: &'static str| EraError::Corrupt {
        path: path.to_path_buf(),
        what,
    };
    let text = std::str::from_utf8(&bytes).map_err(|_| corrupt("not UTF-8"))?;
    let mut fields = text.split_whitespace();
    if fields.next() != Some(FORMAT_TAG) {
        return Err(corrupt("does not begin with the format tag"));
    }
    let era: u8 = fields
        .next()
        .ok_or_else(|| corrupt("holds no era"))?
        .parse()
        .map_err(|_| corrupt("holds an era that is not a number in 0..=255"))?;
    if fields.next().is_some() {
        return Err(corrupt("holds more than the format tag and an era"));
    }
    // A persisted 0 is ordinary: it is what a channel on its 256th era
    // recorded, and the next one after it is 1. Refusing it - which this did at
    // first - would turn a wrap into a refusal to start, once every 256
    // restarts, on a publisher that had done nothing wrong.
    Ok(Some(era))
}

/// Why an era could not be read or recorded. Every variant is a refusal to
/// start; see [`EraStore`].
#[derive(Debug, thiserror::Error)]
pub enum EraError {
    /// A feed name that is not one safe path component.
    #[error("feed name {name:?} is not a single lowercase path component")]
    UnsafeFeedName { name: &'static str },

    /// A shard name that is not one safe path component. Owned rather than
    /// borrowed because a shard name comes from a document, not from a
    /// constant.
    #[error("shard name {name:?} is not a single lowercase path component")]
    UnsafeShardName { name: String },

    /// The file is there and does not say what era this channel instance is in.
    #[error("the era file {path:?} is corrupt: it {what}")]
    Corrupt { path: PathBuf, what: &'static str },

    #[error("{path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}
