use dz_edge_core::{AppMessage, DecodeError, PortRole};

/// Not a method. A publisher must not send it.
pub const METHOD_UNKNOWN: u8 = 0;
/// Book: `(best_bid + best_ask) / 2`.
pub const METHOD_BBO_MID: u8 = 1;
/// Book: `(best_ask × bid_qty + best_bid × ask_qty) / (bid_qty + ask_qty)`.
pub const METHOD_MICRO_PRICE: u8 = 2;
/// Book: depth-weighted across levels, publisher-defined.
pub const METHOD_WEIGHTED_MID: u8 = 3;
/// Trade: the most recent trade price.
pub const METHOD_LAST_TRADE_FALLBACK: u8 = 4;
/// Trade: `Σ(trade_price × trade_qty) / Σ trade_qty` over the window.
pub const METHOD_VWAP: u8 = 5;
/// Publisher-specific, documented out of band.
pub const METHOD_CUSTOM: u8 = 255;

/// The inputs have not updated within the publisher's freshness window.
pub const QUALITY_STALE: u8 = 0x01;
/// Book methods only. The book had liquidity on one side.
pub const QUALITY_ONE_SIDED: u8 = 0x02;
/// Book methods only. The book had `bid >= ask`.
pub const QUALITY_CROSSED_OR_LOCKED: u8 = 0x04;
/// The price was not derived from the current inputs.
pub const QUALITY_SYNTHETIC: u8 = 0x08;

/// Whether the specification classifies `method` as a book method.
#[must_use]
pub const fn is_book_method(method: u8) -> bool {
    matches!(
        method,
        METHOD_BBO_MID | METHOD_MICRO_PRICE | METHOD_WEIGHTED_MID
    )
}

/// Whether the specification classifies `method` as a trade method.
#[must_use]
pub const fn is_trade_method(method: u8) -> bool {
    matches!(method, METHOD_LAST_TRADE_FALLBACK | METHOD_VWAP)
}

/// A `Midpoint` that breaks a rule the specification states with MUST.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MidpointRuleError {
    #[error("Method is 0 (Unknown), which a publisher must not send")]
    MethodUnknown,

    #[error("Method is VWAP and Window is 0")]
    VwapWindowZero,

    #[error("Method is VWAP, Volume is 0 and the synthetic bit is clear")]
    VwapEmptyWindowNotSynthetic,

    #[error("Method {method} is not VWAP and Window is {window_ms}, not 0")]
    WindowNotZero { method: u8, window_ms: u32 },

    #[error("Method {method} is not VWAP and Volume is {volume}, not 0")]
    VolumeNotZero { method: u8, volume: u64 },

    #[error("Method {method} is a trade method and Quality Flags {quality_flags:#04x} set a book-only bit")]
    BookFlagOnTradeMethod { method: u8, quality_flags: u8 },
}

/// `0x03 Midpoint` (48 bytes). One derived price with its provenance.
///
/// `price` carries the instrument's Price Exponent and `volume` its Qty
/// Exponent, both from `InstrumentDefinition`. This type does no scaling.
///
/// `encode_into` and `decode` do not apply the specification's field rules.
/// A publisher calls [`Midpoint::check`] before it pushes. A decoder keeps a
/// message that breaks a rule, so the evidence of a publisher defect survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Midpoint {
    pub instrument_id: u32,
    /// Must equal the `Source ID` in the instrument's `InstrumentDefinition`.
    pub source_id: u16,
    /// A `METHOD_*` value. Never [`METHOD_UNKNOWN`].
    pub method: u8,
    /// `QUALITY_*` bits.
    pub quality_flags: u8,
    /// Venue timestamp. Book method: the book state. Method 4: the trade.
    /// VWAP: the end of the window.
    pub input_timestamp_ns: u64,
    pub compute_timestamp_ns: u64,
    pub price: i64,
    /// VWAP: the window length in milliseconds, never 0. Otherwise 0.
    pub window_ms: u32,
    /// VWAP: the sum of trade quantity in the window. Otherwise 0.
    pub volume: u64,
}

impl Midpoint {
    /// The trade window for a VWAP, as `(start_exclusive_ns, end_inclusive_ns]`.
    ///
    /// `None` for any other method. The start saturates at 0.
    #[must_use]
    pub fn window_ns(&self) -> Option<(u64, u64)> {
        if self.method != METHOD_VWAP {
            return None;
        }
        let end = self.input_timestamp_ns;
        let len = u64::from(self.window_ms) * 1_000_000;
        Some((end.saturating_sub(len), end))
    }

    /// Checks the rules the specification states with MUST for this message
    /// alone. The `Source ID` rule needs the `InstrumentDefinition` and is not
    /// checked here.
    pub fn check(&self) -> Result<(), MidpointRuleError> {
        if self.method == METHOD_UNKNOWN {
            return Err(MidpointRuleError::MethodUnknown);
        }
        if self.method == METHOD_VWAP {
            if self.window_ms == 0 {
                return Err(MidpointRuleError::VwapWindowZero);
            }
            if self.volume == 0 && self.quality_flags & QUALITY_SYNTHETIC == 0 {
                return Err(MidpointRuleError::VwapEmptyWindowNotSynthetic);
            }
        } else {
            if self.window_ms != 0 {
                return Err(MidpointRuleError::WindowNotZero {
                    method: self.method,
                    window_ms: self.window_ms,
                });
            }
            if self.volume != 0 {
                return Err(MidpointRuleError::VolumeNotZero {
                    method: self.method,
                    volume: self.volume,
                });
            }
        }
        if is_trade_method(self.method)
            && self.quality_flags & (QUALITY_ONE_SIDED | QUALITY_CROSSED_OR_LOCKED) != 0
        {
            return Err(MidpointRuleError::BookFlagOnTradeMethod {
                method: self.method,
                quality_flags: self.quality_flags,
            });
        }
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer {
                need: Self::SIZE,
                got: buf.len(),
            });
        }
        if buf[0] != Self::TYPE_ID {
            return Err(DecodeError::BadTypeId(buf[0]));
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            instrument_id: u32::from_le_bytes(
                buf[4..8]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            method: buf[10],
            quality_flags: buf[11],
            input_timestamp_ns: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            compute_timestamp_ns: u64::from_le_bytes(
                buf[20..28]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            price: i64::from_le_bytes(
                buf[28..36]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            window_ms: u32::from_le_bytes(
                buf[36..40]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            volume: u64::from_le_bytes(
                buf[40..48]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
        })
    }
}

impl AppMessage for Midpoint {
    const TYPE_ID: u8 = 0x03;
    const SIZE: usize = 48;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.method;
        dst[11] = self.quality_flags;
        dst[12..20].copy_from_slice(&self.input_timestamp_ns.to_le_bytes());
        dst[20..28].copy_from_slice(&self.compute_timestamp_ns.to_le_bytes());
        dst[28..36].copy_from_slice(&self.price.to_le_bytes());
        dst[36..40].copy_from_slice(&self.window_ms.to_le_bytes());
        dst[40..48].copy_from_slice(&self.volume.to_le_bytes());
    }

    // Midpoint carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}
