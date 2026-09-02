//! What is persisted, and what refusing to read it prevents.

use std::collections::HashMap;

use dz_edge_refdata::SYMBOL_LEN;

/// The tag every record starts with, so a file that is not one of ours is told
/// apart from one of ours that is damaged.
const FORMAT_TAG: &str = "dz-refdata-state";

/// The record layout this build reads and writes.
///
/// A version rather than a guess: a later layout is refused by name, and a
/// refusal at startup is the only safe answer to a state file this build cannot
/// read — see [`StateRecord::decode`].
const FORMAT_VERSION: u32 = 1;

/// One instrument's persisted identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// The `Instrument ID` minted for this symbol, once, ever.
    pub instrument_id: u32,
    /// The wire's `Symbol` field, NUL-padded, exactly as it was published.
    ///
    /// The venue's own ticker is *not* what is keyed on. The field is 64 bytes
    /// on the wire, so two venue tickers that differ only past that width are
    /// one symbol to every subscriber, and keying on the venue's string would
    /// mint them two `Instrument ID`s that publish as the same instrument.
    pub symbol: [u8; SYMBOL_LEN],
}

/// Everything that has to survive a restart.
///
/// Three fields, and each is here because losing it breaks a promise a
/// subscriber is already relying on:
///
/// - The **entries** are the promise that an `Instrument ID` names the same
///   instrument tomorrow. A subscriber keys a book on one.
/// - **`next_id`** is the promise that an `Instrument ID` is never re-issued.
///   It is deliberately not derived as `max(entries) + 1`: entries are never
///   removed, so today the two agree, and a later change that started pruning
///   them would silently begin handing a retired ID to a new instrument.
/// - The **`Source ID`** is what makes the other two checkable. A state
///   directory belongs to one publisher identity, and reading another
///   publisher's ID map would publish its IDs under our own `Source ID`.
///
/// `Manifest Seq` is **not** here. It could be, and carrying it would be worse
/// than useless: a restart cannot honour the continuity that would imply, since
/// the published set is rebuilt from whatever the venue offers on the next poll.
/// A subscriber is told about the restart by `Valid` passing through 0 and by
/// the channel's own `Reset Count`, and both of those are truthful. Persisting
/// it would also put a flush on the delisting path, which today needs none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRecord {
    pub source_id: u16,
    pub next_id: u32,
    pub entries: Vec<Entry>,
}

/// Why a persisted record could not be read.
///
/// Every one of these is a startup refusal and none is recoverable by
/// continuing. The alternative — treating an unreadable record as an empty one
/// — is the specific failure the persistence exists to prevent: minting from
/// the start of the ID space again hands `Instrument ID` 1 to whatever the
/// venue happens to offer first, while subscribers still hold books keyed on
/// the ID 1 that was published yesterday.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("not a reference-data state record: it does not begin with {FORMAT_TAG}")]
    NotOurFormat,

    /// A record written by a later build.
    ///
    /// Refused rather than read on a best-effort basis: a layout this build
    /// does not know may hold a field whose absence changes what the fields it
    /// does know mean.
    #[error(
        "state record format {found} is not one this build reads (it writes {FORMAT_VERSION})"
    )]
    UnsupportedVersion { found: u32 },

    #[error("state record line {line}: {what}")]
    Malformed { line: usize, what: &'static str },

    /// Two entries claim one `Instrument ID`.
    #[error("state record: Instrument ID {instrument_id} appears twice")]
    DuplicateId { instrument_id: u32 },

    /// Two entries claim one `Symbol`.
    #[error("state record: a Symbol appears twice, under IDs {first} and {second}")]
    DuplicateSymbol { first: u32, second: u32 },

    /// An entry holds an ID that minting would hand out again.
    ///
    /// `next_id` is the whole guarantee against re-issue, so a record where it
    /// does not exceed every ID already minted is a record that would produce
    /// a collision on the next admission.
    #[error("state record: Instrument ID {instrument_id} is not below next_id {next_id}")]
    IdNotBelowNext { instrument_id: u32, next_id: u32 },

    /// `Instrument ID` 0 was persisted.
    ///
    /// Zero is not minted (see [`FIRST_INSTRUMENT_ID`]), so a record holding it
    /// was not written by this crate.
    #[error("state record: Instrument ID 0 is not one this crate mints")]
    ZeroId,
}

/// The first `Instrument ID` ever minted.
///
/// One, not zero. A zero-filled buffer, a short read, or a message a decoder
/// gave up on part way through all present as an `Instrument ID` of 0, so a
/// real instrument must never own it — the ID that means "nothing was set"
/// cannot also mean "the first thing the venue listed".
pub const FIRST_INSTRUMENT_ID: u32 = 1;

impl StateRecord {
    /// An empty record for a publisher that has never minted anything.
    #[must_use]
    pub const fn empty(source_id: u16) -> Self {
        Self {
            source_id,
            next_id: FIRST_INSTRUMENT_ID,
            entries: Vec::new(),
        }
    }

    /// The bytes to persist.
    ///
    /// Entries are written in `Instrument ID` order, so the same set of
    /// admissions produces the same bytes whatever order the venue offered them
    /// in. That is what makes a diff of two state directories mean something.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut entries = self.entries.clone();
        entries.sort_unstable_by_key(|entry| entry.instrument_id);

        let mut out = format!(
            "{FORMAT_TAG} {FORMAT_VERSION} {} {}\n",
            self.source_id, self.next_id
        );
        for entry in &entries {
            out.push_str(&entry.instrument_id.to_string());
            out.push(' ');
            for byte in entry.symbol {
                // Hexadecimal rather than the symbol's own text. `Symbol` is a
                // fixed-width NUL-padded field, and the specification's own
                // `Fit::Unrepresentable` case says a venue can put bytes in it
                // that are not ASCII — including an interior NUL. A text format
                // cannot round-trip those, and a persisted identity that does
                // not round-trip is an `Instrument ID` that resolves to a
                // different symbol after a restart.
                out.push_str(&format!("{byte:02x}"));
            }
            out.push('\n');
        }
        out.into_bytes()
    }

    /// Read a persisted record, or refuse it.
    ///
    /// # Errors
    ///
    /// Every [`RecordError`]. All of them are startup failures; see that type
    /// for why none of them may be swallowed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let text = std::str::from_utf8(bytes).map_err(|_| RecordError::NotOurFormat)?;
        let mut lines = text.lines().enumerate();

        let (_, header) = lines.next().ok_or(RecordError::NotOurFormat)?;
        let mut fields = header.split(' ');
        if fields.next() != Some(FORMAT_TAG) {
            return Err(RecordError::NotOurFormat);
        }
        let version: u32 = fields
            .next()
            .and_then(|field| field.parse().ok())
            .ok_or(RecordError::NotOurFormat)?;
        if version != FORMAT_VERSION {
            return Err(RecordError::UnsupportedVersion { found: version });
        }
        let source_id: u16 =
            fields
                .next()
                .and_then(|field| field.parse().ok())
                .ok_or(RecordError::Malformed {
                    line: 1,
                    what: "the header has no readable Source ID",
                })?;
        let next_id: u32 =
            fields
                .next()
                .and_then(|field| field.parse().ok())
                .ok_or(RecordError::Malformed {
                    line: 1,
                    what: "the header has no readable next_id",
                })?;
        if fields.next().is_some() {
            return Err(RecordError::Malformed {
                line: 1,
                what: "the header carries more fields than this format has",
            });
        }

        let mut entries: Vec<Entry> = Vec::new();
        // Hashed rather than scanned: the record holds every ID the venue has
        // ever had listed, which only grows, and a linear scan per entry would
        // make startup quadratic in the venue's whole history.
        let mut ids: HashMap<u32, ()> = HashMap::new();
        let mut symbols: HashMap<[u8; SYMBOL_LEN], u32> = HashMap::new();
        for (index, line) in lines {
            let line_number = index + 1;
            let malformed = |what| RecordError::Malformed {
                line: line_number,
                what,
            };
            let (id, symbol) = line
                .split_once(' ')
                .ok_or_else(|| malformed("an entry is not `Instrument ID` then `Symbol`"))?;
            let instrument_id: u32 = id
                .parse()
                .map_err(|_| malformed("an entry's Instrument ID is not a number"))?;
            if instrument_id == 0 {
                return Err(RecordError::ZeroId);
            }
            if instrument_id >= next_id {
                return Err(RecordError::IdNotBelowNext {
                    instrument_id,
                    next_id,
                });
            }
            let symbol = decode_symbol(symbol)
                .ok_or_else(|| malformed("an entry's Symbol is not 64 hexadecimal bytes"))?;
            if ids.insert(instrument_id, ()).is_some() {
                return Err(RecordError::DuplicateId { instrument_id });
            }
            if let Some(first) = symbols.insert(symbol, instrument_id) {
                return Err(RecordError::DuplicateSymbol {
                    first,
                    second: instrument_id,
                });
            }
            entries.push(Entry {
                instrument_id,
                symbol,
            });
        }

        Ok(Self {
            source_id,
            next_id,
            entries,
        })
    }
}

/// The 64 bytes behind `SYMBOL_LEN * 2` hexadecimal digits, or `None`.
fn decode_symbol(text: &str) -> Option<[u8; SYMBOL_LEN]> {
    let digits = text.as_bytes();
    if digits.len() != SYMBOL_LEN * 2 {
        return None;
    }
    let mut symbol = [0u8; SYMBOL_LEN];
    for (byte, pair) in symbol.iter_mut().zip(digits.chunks_exact(2)) {
        let high = nibble(pair[0])?;
        let low = nibble(pair[1])?;
        *byte = (high << 4) | low;
    }
    Some(symbol)
}

/// One lowercase hexadecimal digit's value.
///
/// Lowercase only, because that is what [`StateRecord::encode`] writes and a
/// reader that accepted more than its writer emits would be accepting somebody
/// else's format by accident.
fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}
