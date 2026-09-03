//! The record encoding: one normalized event, length-delimited and versioned.
//!
//! # Why an encoding exists at all
//!
//! Two things need one, and they are the same thing pointed in opposite
//! directions.
//!
//! A venue whose integration is not Rust cannot implement the adapter trait, so
//! the boundary would exclude it. `[adapter] kind = "uds"` lets that
//! integration be another process in any language: it writes these records, and
//! the built-in adapter reads them. It costs a serialization format and a copy
//! per event, which is exactly why a Rust venue should not use it.
//!
//! And a reference stream for the offline comparison needs the publisher's own
//! normalized events written down somewhere. That is the same records, written
//! rather than read.
//!
//! # The shape
//!
//! ```text
//! u32  length      bytes that follow, so a reader can skip a record it cannot read
//! u8   version     this module's own, not the feed's
//! u8   kind        which event
//! ...  body
//! ```
//!
//! Length first and version second is deliberate: a reader that does not know
//! the version can still find the next record. A version inside a
//! self-describing body would make an unknown version unskippable, which turns
//! one bad record into the end of the stream.
//!
//! # Instruments are named by symbol, not by handle
//!
//! An `InstrumentRef` is a dense index into one runtime's admitted table. A
//! record carrying one would only mean anything inside the process that minted
//! it — and the whole point of this encoding is that the writer is somewhere
//! else. So a record names the venue's own symbol, and the reader resolves it
//! against the handles it holds. That is the same conclusion the offline
//! re-lowering reached for the same reason: the symbol is the only identity two
//! sides can both state.

use dz_adapter_core::{
    Aggressor, ClearScope, Event, InstrumentRef, Presence, Scalar, Side, SideUpdate, TradeFlags,
};

/// This encoding's version. Bumped when a body changes shape, never when a new
/// event kind is added — a reader that does not know a kind skips the record
/// and stays in the stream.
pub const VERSION: u8 = 1;

/// The header a reader needs before it can do anything: the length and the
/// version.
pub const HEADER: usize = 4 + 1 + 1;

const KIND_QUOTE: u8 = 1;
const KIND_TRADE: u8 = 2;
const KIND_LEVEL: u8 = 3;
const KIND_CLEAR: u8 = 4;

const SCALAR_TEXT: u8 = 0;
const SCALAR_FIXED: u8 = 1;

const SIDE_GONE: u8 = 0;
const SIDE_PRESENT: u8 = 1;

const SCOPE_ENTIRE_SIDE: u8 = 0;
const SCOPE_BOTH_SIDES: u8 = 1;
const SCOPE_FROM_PRICE: u8 = 2;

const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

/// Why a record could not be read.
///
/// Deliberately not [`ParseError`](dz_adapter_core::ParseError): that one is
/// the taxonomy a *venue's* upstream parse reports, and this is our own
/// encoding. The built-in adapter maps these onto it at the boundary, which is
/// where the two vocabularies meet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    /// Fewer bytes than the header needs, or than the length declared.
    #[error("record is shorter than it declared")]
    Truncated,
    /// A version this reader does not implement. The record is skippable —
    /// its length was readable, which is why the length comes first.
    #[error("record states version {found}, and this reader implements {VERSION}")]
    Version { found: u8 },
    /// An event kind this reader does not implement. Also skippable, and
    /// ordinary rather than an error for a stream written by a newer writer.
    #[error("record states event kind {found}")]
    Kind { found: u8 },
    /// Structurally present and unusable: a tag outside its range, a symbol
    /// that is not UTF-8, a body that ended early.
    #[error("record is malformed: {detail}")]
    Malformed { detail: &'static str },
}

/// Why one event could not be written down.
///
/// Separate from [`RecordError`], which is the read side, for the reason the
/// codec crates keep their two apart: a writer's caller should not have to name
/// a decode type, and neither enum should carry a variant the other direction
/// cannot reach.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RecordWriteError {
    /// An [`Event`] variant this encoding does not cover.
    ///
    /// `Event` is `#[non_exhaustive]` and this crate is not the one that
    /// defines it, so a variant added there compiles here and arrives at the
    /// wildcard arm — a runtime case, not the build failure this method's
    /// documentation used to claim.
    ///
    /// Recoverable, and it used to be a `todo!`. Three things could happen to
    /// an event this cannot write, and the panic was the worst of them: a
    /// recorder that panics stops recording every feed it holds, over one event
    /// kind. Silently skipping is the second worst, and is what the old comment
    /// was right to reject — a reference stream missing an event kind reads
    /// exactly like a publisher that never emitted one. So it is refused,
    /// named, and left for the caller to count, which is what the codec crates
    /// do with a message a feed does not carry.
    #[error("this encoding does not cover the {kind} event")]
    UnsupportedEvent { kind: String },
}

/// A reader over one record's bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], RecordError> {
        let end = self.at.checked_add(n).ok_or(RecordError::Truncated)?;
        let slice = self.bytes.get(self.at..end).ok_or(RecordError::Truncated)?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, RecordError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, RecordError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u64(&mut self) -> Result<u64, RecordError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().expect("eight bytes")))
    }

    fn i64(&mut self) -> Result<i64, RecordError> {
        Ok(self.u64()? as i64)
    }

    fn str(&mut self) -> Result<&'a str, RecordError> {
        let len = usize::from(self.u16()?);
        let bytes = self.take(len)?;
        core::str::from_utf8(bytes).map_err(|_| RecordError::Malformed {
            detail: "a text field is not utf-8",
        })
    }

    fn scalar(&mut self) -> Result<Scalar<'a>, RecordError> {
        match self.u8()? {
            SCALAR_TEXT => Ok(Scalar::Text(self.str()?)),
            SCALAR_FIXED => {
                let mantissa = self.i64()?;
                let exponent = self.u8()? as i8;
                Ok(Scalar::Fixed { mantissa, exponent })
            }
            _ => Err(RecordError::Malformed {
                detail: "a scalar tag is outside its range",
            }),
        }
    }

    fn optional_u16(&mut self) -> Result<Option<u16>, RecordError> {
        match self.u8()? {
            ABSENT => Ok(None),
            PRESENT => Ok(Some(self.u16()?)),
            _ => Err(RecordError::Malformed {
                detail: "a presence tag is outside its range",
            }),
        }
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, RecordError> {
        match self.u8()? {
            ABSENT => Ok(None),
            PRESENT => Ok(Some(self.u64()?)),
            _ => Err(RecordError::Malformed {
                detail: "a presence tag is outside its range",
            }),
        }
    }

    fn optional_scalar(&mut self) -> Result<Option<Scalar<'a>>, RecordError> {
        match self.u8()? {
            ABSENT => Ok(None),
            PRESENT => Ok(Some(self.scalar()?)),
            _ => Err(RecordError::Malformed {
                detail: "a presence tag is outside its range",
            }),
        }
    }

    fn side_update(&mut self) -> Result<SideUpdate<'a>, RecordError> {
        match self.u8()? {
            SIDE_GONE => Ok(SideUpdate::Gone),
            SIDE_PRESENT => Ok(SideUpdate::Present {
                px: self.scalar()?,
                qty: self.scalar()?,
                source_count: self.optional_u16()?,
            }),
            _ => Err(RecordError::Malformed {
                detail: "a side tag is outside its range",
            }),
        }
    }

    fn side(&mut self) -> Result<Side, RecordError> {
        match self.u8()? {
            0 => Ok(Side::Bid),
            1 => Ok(Side::Ask),
            _ => Err(RecordError::Malformed {
                detail: "a side is outside its range",
            }),
        }
    }

    fn aggressor(&mut self) -> Result<Aggressor, RecordError> {
        match self.u8()? {
            0 => Ok(Aggressor::Unknown),
            1 => Ok(Aggressor::Buy),
            2 => Ok(Aggressor::Sell),
            _ => Err(RecordError::Malformed {
                detail: "an aggressor is outside its range",
            }),
        }
    }

    fn presence(&mut self) -> Result<Presence, RecordError> {
        match self.u8()? {
            0 => Ok(Presence::Unknown),
            1 => Ok(Presence::New),
            2 => Ok(Presence::Change),
            _ => Err(RecordError::Malformed {
                detail: "a presence is outside its range",
            }),
        }
    }

    fn trade_flags(&mut self) -> Result<TradeFlags, RecordError> {
        let byte = self.u8()?;
        if byte & 0xF8 != 0 {
            return Err(RecordError::Malformed {
                detail: "a trade flags byte sets a bit nobody defined",
            });
        }
        Ok(TradeFlags {
            block: byte & 0x01 != 0,
            sweep: byte & 0x02 != 0,
            cross: byte & 0x04 != 0,
        })
    }

    fn clear_scope(&mut self) -> Result<ClearScope<'a>, RecordError> {
        match self.u8()? {
            SCOPE_ENTIRE_SIDE => Ok(ClearScope::EntireSide(self.side()?)),
            SCOPE_BOTH_SIDES => Ok(ClearScope::BothSides),
            SCOPE_FROM_PRICE => Ok(ClearScope::FromPrice {
                side: self.side()?,
                px: self.scalar()?,
            }),
            _ => Err(RecordError::Malformed {
                detail: "a clear scope tag is outside its range",
            }),
        }
    }
}

/// How many bytes the record at the front of `bytes` occupies, if all of it is
/// there.
///
/// `None` means the buffer holds part of a record and the caller should read
/// more — which is the whole reason the length comes first.
#[must_use]
pub fn record_len(bytes: &[u8]) -> Option<usize> {
    let declared = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    let total = declared.checked_add(4)?;
    (bytes.len() >= total).then_some(total)
}

/// Read one record, resolving its symbol to a handle.
///
/// `resolve` is how the reader's own admitted set enters: the record names a
/// symbol and only the reader knows what it admitted it as. `Ok(None)` is a
/// record for an instrument this reader holds no handle for, which is ordinary
/// — a source may offer more than a runtime's selection policy admits.
///
/// # Errors
///
/// [`RecordError`]. `Version` and `Kind` are skippable: the record's length was
/// readable, so the caller can step past it and stay in the stream.
pub fn decode<'a>(
    bytes: &'a [u8],
    mut resolve: impl FnMut(&str) -> Option<InstrumentRef>,
) -> Result<Option<Event<'a>>, RecordError> {
    if bytes.len() < HEADER {
        return Err(RecordError::Truncated);
    }
    let declared = u32::from_le_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
    let body = bytes.get(4..4 + declared).ok_or(RecordError::Truncated)?;

    let mut r = Reader { bytes: body, at: 0 };
    let version = r.u8()?;
    if version != VERSION {
        return Err(RecordError::Version { found: version });
    }
    let kind = r.u8()?;
    let symbol = r.str()?;
    let Some(instrument) = resolve(symbol) else {
        return Ok(None);
    };
    let source_ts_ns = r.u64()?;

    let event = match kind {
        KIND_QUOTE => Event::Quote {
            instrument,
            source_ts_ns,
            bid: r.side_update()?,
            ask: r.side_update()?,
        },
        KIND_TRADE => Event::Trade {
            instrument,
            source_ts_ns,
            px: r.scalar()?,
            qty: r.scalar()?,
            aggressor: r.aggressor()?,
            trade_id: r.optional_u64()?,
            cumulative_volume: r.optional_scalar()?,
            flags: r.trade_flags()?,
        },
        KIND_LEVEL => Event::Level {
            instrument,
            source_ts_ns,
            side: r.side()?,
            px: r.scalar()?,
            qty: r.scalar()?,
            order_count: r.optional_u16()?,
            presence: r.presence()?,
        },
        KIND_CLEAR => Event::Clear {
            instrument,
            source_ts_ns,
            scope: r.clear_scope()?,
        },
        found => return Err(RecordError::Kind { found }),
    };

    // A body with bytes left over is a writer and a reader that disagree about
    // this event's shape, which is exactly what the version exists to catch —
    // so it is malformed rather than tolerated. Tolerating it would let a
    // writer add a field silently and have half the fleet ignore it.
    if r.at != body.len() {
        return Err(RecordError::Malformed {
            detail: "the body carries more bytes than this event's shape",
        });
    }
    Ok(Some(event))
}

/// Writes records, for the reference stream and for a source written in Rust.
///
/// The symbol is a parameter rather than read off the event, because an event
/// carries a handle and a handle means nothing outside the process that minted
/// it. Whoever writes has the table; this does not.
#[derive(Debug, Default)]
pub struct RecordWriter {
    body: Vec<u8>,
}

impl RecordWriter {
    /// A writer with nothing buffered.
    #[must_use]
    pub const fn new() -> Self {
        Self { body: Vec::new() }
    }

    /// Encode one event, appending the whole record to `out`.
    ///
    /// # Errors
    ///
    /// [`RecordWriteError::UnsupportedEvent`] for an `Event` variant this
    /// encoding does not cover. `Event` is `#[non_exhaustive]` and lives in
    /// another crate, so a variant added to it does *not* fail this build — it
    /// reaches the wildcard arm at runtime. Nothing is appended to `out` in that
    /// case, so a refused event leaves no partial record behind.
    pub fn write(
        &mut self,
        symbol: &str,
        event: &Event<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), RecordWriteError> {
        self.body.clear();
        self.body.push(VERSION);

        match event {
            Event::Quote {
                source_ts_ns,
                bid,
                ask,
                ..
            } => {
                self.body.push(KIND_QUOTE);
                self.str(symbol);
                self.body.extend_from_slice(&source_ts_ns.to_le_bytes());
                self.side_update(bid);
                self.side_update(ask);
            }
            Event::Trade {
                source_ts_ns,
                px,
                qty,
                aggressor,
                trade_id,
                cumulative_volume,
                flags,
                ..
            } => {
                self.body.push(KIND_TRADE);
                self.str(symbol);
                self.body.extend_from_slice(&source_ts_ns.to_le_bytes());
                self.scalar(px);
                self.scalar(qty);
                self.body.push(match aggressor {
                    Aggressor::Unknown => 0,
                    Aggressor::Buy => 1,
                    Aggressor::Sell => 2,
                });
                match trade_id {
                    None => self.body.push(ABSENT),
                    Some(id) => {
                        self.body.push(PRESENT);
                        self.body.extend_from_slice(&id.to_le_bytes());
                    }
                }
                match cumulative_volume {
                    None => self.body.push(ABSENT),
                    Some(volume) => {
                        self.body.push(PRESENT);
                        self.scalar(volume);
                    }
                }
                let mut byte = 0u8;
                if flags.block {
                    byte |= 0x01;
                }
                if flags.sweep {
                    byte |= 0x02;
                }
                if flags.cross {
                    byte |= 0x04;
                }
                self.body.push(byte);
            }
            Event::Level {
                source_ts_ns,
                side,
                px,
                qty,
                order_count,
                presence,
                ..
            } => {
                self.body.push(KIND_LEVEL);
                self.str(symbol);
                self.body.extend_from_slice(&source_ts_ns.to_le_bytes());
                self.side(*side);
                self.scalar(px);
                self.scalar(qty);
                match order_count {
                    None => self.body.push(ABSENT),
                    Some(count) => {
                        self.body.push(PRESENT);
                        self.body.extend_from_slice(&count.to_le_bytes());
                    }
                }
                self.body.push(match presence {
                    Presence::Unknown => 0,
                    Presence::New => 1,
                    Presence::Change => 2,
                });
            }
            Event::Clear {
                source_ts_ns,
                scope,
                ..
            } => {
                self.body.push(KIND_CLEAR);
                self.str(symbol);
                self.body.extend_from_slice(&source_ts_ns.to_le_bytes());
                match scope {
                    ClearScope::EntireSide(side) => {
                        self.body.push(SCOPE_ENTIRE_SIDE);
                        self.side(*side);
                    }
                    ClearScope::BothSides => self.body.push(SCOPE_BOTH_SIDES),
                    ClearScope::FromPrice { side, px } => {
                        self.body.push(SCOPE_FROM_PRICE);
                        self.side(*side);
                        self.scalar(px);
                    }
                }
            }
            // `Event` is `#[non_exhaustive]`, so a variant added upstream lands
            // here rather than being written as something else. Refused rather
            // than skipped, because a reference stream missing an event kind
            // reports the publisher dropping every one of them — and rather
            // than panicked, because a recorder that goes down over one event
            // kind stops recording every feed it holds. See
            // [`RecordWriteError::UnsupportedEvent`].
            other => {
                // Nothing has reached `out`, so there is no partial record to
                // take back.
                return Err(RecordWriteError::UnsupportedEvent {
                    kind: format!("{other:?}"),
                });
            }
        }

        let len = u32::try_from(self.body.len()).expect("one event is far below 4 GiB");
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&self.body);
        Ok(())
    }

    fn str(&mut self, text: &str) {
        let len = u16::try_from(text.len()).unwrap_or(u16::MAX);
        self.body.extend_from_slice(&len.to_le_bytes());
        self.body
            .extend_from_slice(&text.as_bytes()[..usize::from(len)]);
    }

    fn scalar(&mut self, scalar: &Scalar<'_>) {
        match scalar {
            Scalar::Text(text) => {
                self.body.push(SCALAR_TEXT);
                self.str(text);
            }
            Scalar::Fixed { mantissa, exponent } => {
                self.body.push(SCALAR_FIXED);
                self.body.extend_from_slice(&mantissa.to_le_bytes());
                self.body.push(*exponent as u8);
            }
        }
    }

    fn side(&mut self, side: Side) {
        self.body.push(match side {
            Side::Bid => 0,
            Side::Ask => 1,
        });
    }

    fn side_update(&mut self, side: &SideUpdate<'_>) {
        match side {
            SideUpdate::Gone => self.body.push(SIDE_GONE),
            SideUpdate::Present {
                px,
                qty,
                source_count,
            } => {
                self.body.push(SIDE_PRESENT);
                self.scalar(px);
                self.scalar(qty);
                match source_count {
                    None => self.body.push(ABSENT),
                    Some(count) => {
                        self.body.push(PRESENT);
                        self.body.extend_from_slice(&count.to_le_bytes());
                    }
                }
            }
        }
    }
}
