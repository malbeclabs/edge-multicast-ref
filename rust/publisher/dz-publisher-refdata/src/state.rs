//! What is persisted, and what refusing to read it prevents.

use std::collections::HashMap;

use dz_edge_refdata::SYMBOL_LEN;

/// The tag every record starts with, so a file that is not one of ours is told
/// apart from one of ours that is damaged.
const FORMAT_TAG: &str = "dz-refdata-state";

/// The record layout this build writes under [`IdAllocation::Sequential`].
///
/// A version rather than a guess: a later layout is refused by name, and a
/// refusal at startup is the only safe answer to a state file this build cannot
/// read — see [`StateRecord::load`].
const FORMAT_VERSION: u32 = 2;

/// The record layout this build writes under [`IdAllocation::Derived`].
///
/// Version 2 with the allocation named in the header, where the second number
/// is the floor rather than a `next_id` that advances. Only a derived
/// publisher writes it, so a sequential one keeps writing version 2 and can
/// still roll back to a build that reads nothing later.
const FORMAT_VERSION_DERIVED: u32 = 3;

/// The header's last field in version 3.
const DERIVED_TAG: &str = "derived";

/// The layout before appended lines, which this build still reads.
///
/// A base with no entry count and no timestamps. Every entry in it is read
/// as recorded published, and the registry rewrites it as the current version
/// when it opens, since a line appended to it could not be told apart from the
/// base.
const FORMAT_VERSION_V1: u32 = 1;

/// How a symbol the record does not hold is given its `Instrument ID`.
///
/// A symbol the record holds keeps its ID under either. The two differ only in
/// what a new symbol gets, and so in whether two publishers that never
/// exchange anything can agree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IdAllocation {
    /// The next number, `next_id`, in the order the venue first offers each
    /// instrument. A fact about this process's history, so two publishers
    /// agree only while they see every listing in the same order.
    #[default]
    Sequential,
    /// [`derive_instrument_id`] of the `Symbol`, at or above the record's
    /// floor. A fact about the instrument, so two publishers that start from
    /// the same record agree on every ID with no coordination.
    ///
    /// **Every path of a channel starts from the same record, or all of them
    /// start from none.** A publisher started cold derives IDs for symbols that
    /// a seeded one holds below its floor, and disagrees on every one of them.
    Derived,
}

impl IdAllocation {
    /// The record layout written under this allocation.
    const fn version(self) -> u32 {
        match self {
            Self::Sequential => FORMAT_VERSION,
            Self::Derived => FORMAT_VERSION_DERIVED,
        }
    }
}

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
    /// `None` when the instrument was published at the time this entry was
    /// written. Otherwise the Unix second it was last published, which is what
    /// `forget_delisted_after` is measured from.
    pub delisted_at: Option<u64>,
}

/// Everything that has to survive a restart.
///
/// Three fields, and each is here because losing it breaks a promise a
/// subscriber is already relying on:
///
/// - The **entries** are the promise that an `Instrument ID` names the same
///   instrument tomorrow. A subscriber keys a book on one.
/// - **`next_id`** is the promise that an `Instrument ID` is never re-issued.
///   It is never derived as `max(entries) + 1`: a delisted entry can be
///   forgotten, and a `next_id` computed from what is left would hand a retired
///   ID to a new instrument. `next_id` is the only thing standing between the
///   two.
/// - The **`Source ID`** is what makes the other two checkable. A state
///   directory belongs to one publisher identity, and reading another
///   publisher's ID map would publish its IDs under our own `Source ID`.
///
/// `Manifest Seq` is **not** here. It could be, and carrying it would be worse
/// than useless: a restart cannot honour the continuity that would imply, since
/// the published set is rebuilt from whatever the venue offers on the next poll.
/// A subscriber is told about the restart by `Valid` passing through 0 and by
/// the channel's own `Reset Count`, and both of those are truthful. Persisting
/// it would also put a flush on the delisting path, which needs none.
///
/// # The layout
///
/// ```text
/// dz-refdata-state 2 <source_id> <next_id> <base entries>
/// <instrument_id> <symbol, 128 hex digits>
/// <instrument_id> <symbol, 128 hex digits> <unix seconds>
/// ...
/// <instrument_id> <symbol, 128 hex digits>
/// ```
///
/// A **base** — the header and as many entries as it counts — followed by
/// **appended lines**. [`encode`](Self::encode) writes a base and
/// [`encode_line`] writes one appended line, which is what makes a mint cost a
/// line rather than the whole history. [`load`](Self::load) reads both and
/// folds the appended lines into the entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRecord {
    pub source_id: u16,
    /// Under [`IdAllocation::Derived`], the **floor**: no derived ID below it
    /// is ever minted, and it never advances. It is the `next_id` of the
    /// sequential record the allocation was switched on over, which is what
    /// stops a derived ID from landing on one that was minted and forgotten.
    pub next_id: u32,
    pub entries: Vec<Entry>,
    /// Which rule the record was written under. Decides the layout and what a
    /// line may carry; see [`IdAllocation`].
    pub allocation: IdAllocation,
}

/// A record as read back, and what reading it found besides the entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loaded {
    /// The base with every appended line folded in.
    pub record: StateRecord,
    /// The layout it was written in. Anything but [`FORMAT_VERSION`] is
    /// rewritten when the registry opens.
    pub version: u32,
    /// How many entries the base held.
    pub base: usize,
    /// How many complete appended lines followed the base.
    pub appended: usize,
    /// Whether a final appended line with no newline was dropped: an append
    /// that never completed, whose admission therefore never happened.
    pub torn: bool,
    /// How many bytes of the record were read, which is every byte but a torn
    /// final line: what the record is cut back to, to take that line off.
    pub complete: usize,
}

impl Loaded {
    /// Whether the record has to be rewritten before a line can be appended to
    /// it: a layout this build does not append to.
    ///
    /// A torn final line is not one. Cutting it off is enough, and needs no
    /// free space, where a rewrite needs room for the whole record.
    #[must_use]
    pub const fn needs_rewrite(&self) -> bool {
        self.version != self.record.allocation.version()
    }
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

    /// An appended line that neither mints the next ID nor restates a known
    /// one.
    ///
    /// Mints are appended in order, one ID at a time, so a line that skips
    /// ahead or names an ID that was never minted was not written by a mint.
    #[error(
        "state record line {line}: Instrument ID {instrument_id} is neither the next ID \
         ({next_id}) nor one already in the record"
    )]
    AppendedOutOfOrder {
        line: usize,
        instrument_id: u32,
        next_id: u32,
    },

    /// An appended line that restates a known `Instrument ID` under a
    /// different `Symbol`.
    #[error(
        "state record line {line}: Instrument ID {instrument_id} is restated under a \
         different Symbol"
    )]
    RestatedUnderAnotherSymbol { line: usize, instrument_id: u32 },

    /// An entry at or above the floor of a derived record whose `Instrument
    /// ID` is not [`derive_instrument_id`] of its `Symbol`.
    ///
    /// Recomputed rather than trusted, which a sequential record cannot do:
    /// there the only check is that a line names the next number.
    #[error(
        "state record line {line}: Instrument ID {instrument_id} is not the derivation of its \
         Symbol"
    )]
    NotDerived { line: usize, instrument_id: u32 },
    /// An appended line in a derived record that mints below the floor.
    ///
    /// The floor is what keeps a derived ID off one a sequential record
    /// minted and later forgot, so nothing is minted under it.
    #[error(
        "state record line {line}: Instrument ID {instrument_id} is minted below the floor \
         ({floor})"
    )]
    MintedBelowFloor {
        line: usize,
        instrument_id: u32,
        floor: u32,
    },
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

/// CRC-32/ISO-HDLC, one entry per byte value: IEEE 802.3, reflected,
/// polynomial `0xEDB88320`. Built at compile time from the polynomial rather
/// than pasted, so the polynomial is the whole statement of it.
const CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut index = 0usize;
    while index < 256 {
        #[allow(clippy::cast_possible_truncation)]
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

/// The `Instrument ID` [`IdAllocation::Derived`] gives a symbol: CRC-32/ISO-HDLC
/// (initial value and final XOR `0xFFFFFFFF`) over the wire `Symbol` up to its
/// first NUL.
///
/// The wire field, not the venue's own ticker, because it is what the record
/// keys on: two tickers that are one symbol on the wire are one input here
/// too. Stated in full so that a consumer can recompute an ID from a symbol
/// without asking the publisher. The result may be `0`, below a record's floor,
/// or already held, and the registry declines all three rather than moving the
/// ID.
#[must_use]
pub fn derive_instrument_id(symbol: &[u8; SYMBOL_LEN]) -> u32 {
    let end = symbol
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(SYMBOL_LEN);
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in &symbol[..end] {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize];
    }
    !crc
}

/// One appended line: this `Instrument ID` is this `Symbol`, and the
/// instrument is published as it is written.
///
/// Used both to mint an ID and to restate one the record holds with a
/// timestamp, which the reader tells apart by whether the ID is already known.
#[must_use]
pub fn encode_line(instrument_id: u32, symbol: &[u8; SYMBOL_LEN]) -> Vec<u8> {
    let mut out = String::with_capacity(12 + SYMBOL_LEN * 2);
    push_entry(&mut out, instrument_id, symbol);
    out.push('\n');
    out.into_bytes()
}

fn push_entry(out: &mut String, instrument_id: u32, symbol: &[u8; SYMBOL_LEN]) {
    out.push_str(&instrument_id.to_string());
    out.push(' ');
    for byte in symbol {
        // Hexadecimal rather than the symbol's own text. `Symbol` is a
        // fixed-width NUL-padded field, and the specification's own
        // `Fit::Unrepresentable` case says a venue can put bytes in it that are
        // not ASCII — including an interior NUL. A text format cannot
        // round-trip those, and a persisted identity that does not round-trip
        // is an `Instrument ID` that resolves to a different symbol after a
        // restart.
        out.push_str(&format!("{byte:02x}"));
    }
}

impl StateRecord {
    /// An empty record for a publisher that has never minted anything.
    #[must_use]
    pub const fn empty(source_id: u16) -> Self {
        Self {
            source_id,
            next_id: FIRST_INSTRUMENT_ID,
            entries: Vec::new(),
            allocation: IdAllocation::Sequential,
        }
    }

    /// The bytes of a base.
    ///
    /// Entries are written in `Instrument ID` order, so the same set of
    /// admissions produces the same bytes whatever order the venue offered them
    /// in. That is what makes a diff of two state directories mean something.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut entries = self.entries.clone();
        entries.sort_unstable_by_key(|entry| entry.instrument_id);

        let mut out = format!(
            "{FORMAT_TAG} {} {} {} {}",
            self.allocation.version(),
            self.source_id,
            self.next_id,
            entries.len()
        );
        if self.allocation == IdAllocation::Derived {
            out.push(' ');
            out.push_str(DERIVED_TAG);
        }
        out.push('\n');
        for entry in &entries {
            push_entry(&mut out, entry.instrument_id, &entry.symbol);
            if let Some(at) = entry.delisted_at {
                out.push(' ');
                out.push_str(&at.to_string());
            }
            out.push('\n');
        }
        out.into_bytes()
    }

    /// Read a persisted record with its appended lines folded in, or refuse it.
    ///
    /// # Errors
    ///
    /// Every [`RecordError`]. All of them are startup failures; see that type
    /// for why none of them may be swallowed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        Self::load(bytes).map(|loaded| loaded.record)
    }

    /// Read a persisted record, and what reading it found, or refuse it.
    ///
    /// **A final appended line with no newline is dropped**, and only that.
    /// It is an append that never completed: an instrument is admitted only
    /// after its line is flushed, so nothing published depends on it. A
    /// base line with no newline is not the same thing — a base is
    /// written whole and renamed into place, so a torn one is damage — and a
    /// line that is complete and wrong was written by something this build did
    /// not write. Both are refusals.
    ///
    /// # Errors
    ///
    /// Every [`RecordError`]. All of them are startup failures; see that type
    /// for why none of them may be swallowed.
    pub fn load(bytes: &[u8]) -> Result<Loaded, RecordError> {
        let text = std::str::from_utf8(bytes).map_err(|_| RecordError::NotOurFormat)?;
        let mut lines = text.split_inclusive('\n').enumerate();

        let (_, header) = lines.next().ok_or(RecordError::NotOurFormat)?;
        let mut fields = header.trim_end_matches('\n').split(' ');
        if fields.next() != Some(FORMAT_TAG) {
            return Err(RecordError::NotOurFormat);
        }
        let version: u32 = fields
            .next()
            .and_then(|field| field.parse().ok())
            .ok_or(RecordError::NotOurFormat)?;
        if version != FORMAT_VERSION
            && version != FORMAT_VERSION_V1
            && version != FORMAT_VERSION_DERIVED
        {
            return Err(RecordError::UnsupportedVersion { found: version });
        }
        if !header.ends_with('\n') {
            return Err(RecordError::Malformed {
                line: 1,
                what: "the header is not a complete line",
            });
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
        // Version 1 has no count: every line is the base.
        let base: Option<usize> = if version == FORMAT_VERSION_V1 {
            None
        } else {
            Some(fields.next().and_then(|field| field.parse().ok()).ok_or(
                RecordError::Malformed {
                    line: 1,
                    what: "the header has no readable entry count",
                },
            )?)
        };
        let allocation = if version == FORMAT_VERSION_DERIVED {
            if fields.next() != Some(DERIVED_TAG) {
                return Err(RecordError::Malformed {
                    line: 1,
                    what: "a version 3 header does not name its allocation",
                });
            }
            IdAllocation::Derived
        } else {
            IdAllocation::Sequential
        };
        let derived = allocation == IdAllocation::Derived;
        if fields.next().is_some() {
            return Err(RecordError::Malformed {
                line: 1,
                what: "the header carries more fields than this format has",
            });
        }

        let mut entries: Vec<Entry> = Vec::new();
        // Hashed rather than scanned: the record can hold every ID the venue
        // has ever had listed, and a linear scan per entry would make startup
        // quadratic in the venue's whole history.
        let mut ids: HashMap<u32, usize> = HashMap::new();
        let mut symbols: HashMap<[u8; SYMBOL_LEN], u32> = HashMap::new();
        let mut running_next = next_id;
        let mut read = 0usize;
        let mut appended = 0usize;
        let mut torn = false;
        let mut complete = header.len();
        for (index, raw) in lines {
            let line_number = index + 1;
            let malformed = |what| RecordError::Malformed {
                line: line_number,
                what,
            };
            let in_base = base.is_none_or(|count| read < count);
            let Some(line) = raw.strip_suffix('\n') else {
                if in_base {
                    return Err(malformed("a base entry is not a complete line"));
                }
                if !starts_an_appended_line(raw) {
                    return Err(malformed(
                        "a final line with no newline is not the start of an appended line",
                    ));
                }
                torn = true;
                break;
            };
            read += 1;
            complete += raw.len();
            let mut fields = line.split(' ');
            let id = fields.next().unwrap_or_default();
            let symbol = fields
                .next()
                .ok_or_else(|| malformed("an entry is not `Instrument ID` then `Symbol`"))?;
            let delisted_at: Option<u64> = match fields.next() {
                None => None,
                Some(_) if version == FORMAT_VERSION_V1 => {
                    return Err(malformed("a version 1 entry carries a timestamp"));
                }
                Some(_) if !in_base => {
                    return Err(malformed("an appended line carries a timestamp"));
                }
                Some(at) => Some(
                    at.parse()
                        .map_err(|_| malformed("an entry's timestamp is not a number"))?,
                ),
            };
            if fields.next().is_some() {
                return Err(malformed(
                    "an entry carries more fields than this format has",
                ));
            }
            let instrument_id: u32 = id
                .parse()
                .map_err(|_| malformed("an entry's Instrument ID is not a number"))?;
            if instrument_id == 0 {
                return Err(RecordError::ZeroId);
            }
            let symbol = decode_symbol(symbol)
                .ok_or_else(|| malformed("an entry's Symbol is not 64 hexadecimal bytes"))?;

            if in_base {
                // Under derived allocation the header's number is the floor:
                // an entry below it was minted before the switch, and one at or
                // above it was derived, which is checked rather than trusted.
                if derived {
                    if instrument_id >= next_id && instrument_id != derive_instrument_id(&symbol) {
                        return Err(RecordError::NotDerived {
                            line: line_number,
                            instrument_id,
                        });
                    }
                } else if instrument_id >= next_id {
                    return Err(RecordError::IdNotBelowNext {
                        instrument_id,
                        next_id,
                    });
                }
                if ids.insert(instrument_id, entries.len()).is_some() {
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
                    delisted_at,
                });
                continue;
            }

            appended += 1;
            if let Some(&at) = ids.get(&instrument_id) {
                // A restatement: the instrument was relisted after a base
                // recorded it as delisted.
                if entries[at].symbol != symbol {
                    return Err(RecordError::RestatedUnderAnotherSymbol {
                        line: line_number,
                        instrument_id,
                    });
                }
                entries[at].delisted_at = None;
            } else if derived {
                // A derived mint: its own symbol's derivation, at or above the
                // floor. A held ID is the restatement branch above, which
                // refuses it under another symbol; a held symbol is refused
                // here. `next_id` is the floor and does not move.
                if instrument_id != derive_instrument_id(&symbol) {
                    return Err(RecordError::NotDerived {
                        line: line_number,
                        instrument_id,
                    });
                }
                if instrument_id < next_id {
                    return Err(RecordError::MintedBelowFloor {
                        line: line_number,
                        instrument_id,
                        floor: next_id,
                    });
                }
                if let Some(first) = symbols.insert(symbol, instrument_id) {
                    return Err(RecordError::DuplicateSymbol {
                        first,
                        second: instrument_id,
                    });
                }
                ids.insert(instrument_id, entries.len());
                entries.push(Entry {
                    instrument_id,
                    symbol,
                    delisted_at: None,
                });
            } else if instrument_id == running_next {
                if let Some(first) = symbols.insert(symbol, instrument_id) {
                    return Err(RecordError::DuplicateSymbol {
                        first,
                        second: instrument_id,
                    });
                }
                ids.insert(instrument_id, entries.len());
                entries.push(Entry {
                    instrument_id,
                    symbol,
                    delisted_at: None,
                });
                running_next = running_next
                    .checked_add(1)
                    .ok_or_else(|| malformed("an appended line mints past the ID space"))?;
            } else {
                return Err(RecordError::AppendedOutOfOrder {
                    line: line_number,
                    instrument_id,
                    next_id: running_next,
                });
            }
        }
        if let Some(count) = base {
            if read < count {
                return Err(RecordError::Malformed {
                    line: read + 2,
                    what: "the base is shorter than its header counts",
                });
            }
        }

        Ok(Loaded {
            record: Self {
                source_id,
                next_id: running_next,
                entries,
                allocation,
            },
            version,
            base: read - appended,
            appended,
            torn,
            complete,
        })
    }
}

/// Whether `tail` is what [`encode_line`] writes, cut short before its newline:
/// digits, then a space and at most `SYMBOL_LEN * 2` lowercase hexadecimal
/// digits.
///
/// Only a prefix of a line this build appends can be an append that never
/// completed. Anything else with no newline was written by something else, and
/// cutting it off would hide the damage rather than refuse it.
fn starts_an_appended_line(tail: &str) -> bool {
    let (id, symbol) = tail.split_once(' ').unwrap_or((tail, ""));
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    let id_fits = !id.is_empty() && id.len() <= u32::MAX.to_string().len() && digits(id);
    let symbol_fits = symbol.len() <= SYMBOL_LEN * 2 && symbol.bytes().all(|b| nibble(b).is_some());
    id_fits && symbol_fits
}

/// The 64 bytes behind `SYMBOL_LEN * 2` hexadecimal digits, or `None`.
fn decode_symbol(text: &str) -> Option<[u8; SYMBOL_LEN]> {
    let digits = text.as_bytes();
    if digits.len() != SYMBOL_LEN * 2 {
        return None;
    }
    let mut symbol = [0u8; SYMBOL_LEN];
    // `as_chunks` rather than `chunks_exact`: the pair arrives as `[u8; 2]`, so
    // the two indexes below cannot panic and the length check above is the only
    // thing standing between the input and the output.
    let (pairs, rest) = digits.as_chunks::<2>();
    debug_assert!(rest.is_empty(), "the length was checked above");
    for (byte, pair) in symbol.iter_mut().zip(pairs) {
        let high = nibble(pair[0])?;
        let low = nibble(pair[1])?;
        *byte = (high << 4) | low;
    }
    Some(symbol)
}

/// One lowercase hexadecimal digit's value.
///
/// Lowercase only, because that is what [`encode_line`] writes and a
/// reader that accepted more than its writer emits would be accepting somebody
/// else's format by accident.
fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}
