use dz_edge_core::{AppMessage, DecodeError, EncodeError, PortRole};

/// `Bound Flags` bit 0: `Lower Bound` is present.
pub const BOUND_LOWER_PRESENT: u8 = 0b0001;
/// `Bound Flags` bit 1: `Lower Bound` is inclusive.
pub const BOUND_LOWER_INCLUSIVE: u8 = 0b0010;
/// `Bound Flags` bit 2: `Upper Bound` is present.
pub const BOUND_UPPER_PRESENT: u8 = 0b0100;
/// `Bound Flags` bit 3: `Upper Bound` is inclusive.
pub const BOUND_UPPER_INCLUSIVE: u8 = 0b1000;
/// `Bound Flags` bits 4–7: reserved. A publisher clears them and a subscriber
/// ignores them.
pub const BOUND_RESERVED: u8 = 0b1111_0000;

/// `0x09 StrikeInterval` (40 bytes), on the `refdata` port role.
///
/// The strike of one instrument as an interval: a lower bound, an upper bound,
/// or both. It follows the instrument's `InstrumentDefinition` in the same
/// datagram and nowhere else, which is a rule about a datagram and so is held by
/// whatever fills one, not by this type.
///
/// A strike with neither bound present is **pending**: the venue has listed the
/// instrument and has not fixed the strike yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrikeInterval {
    pub instrument_id: u32,
    pub source_id: u16,
    /// The decimal exponent of both bounds. Independent of the definition's
    /// `Price Exponent`, which scales the contract's price and not the value the
    /// venue compares with the strike.
    pub strike_exponent: i8,
    pub bound_flags: u8,
    /// `0` when [`BOUND_LOWER_PRESENT`] is clear.
    pub lower_bound: i64,
    /// `0` when [`BOUND_UPPER_PRESENT`] is clear.
    pub upper_bound: i64,
    /// `0` while pending, when the venue fixed the strike at listing, and when
    /// the time is not available.
    pub fixing_time_ns: u64,
}

impl StrikeInterval {
    /// The strike of an instrument whose venue has not fixed it yet.
    #[must_use]
    pub const fn pending(instrument_id: u32, source_id: u16, strike_exponent: i8) -> Self {
        Self {
            instrument_id,
            source_id,
            strike_exponent,
            bound_flags: 0,
            lower_bound: 0,
            upper_bound: 0,
            fixing_time_ns: 0,
        }
    }

    /// Whether neither bound is present. Reads the two presence bits only, as a
    /// subscriber does, so a reserved bit somebody set does not make a pending
    /// strike look fixed.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        self.bound_flags & (BOUND_LOWER_PRESENT | BOUND_UPPER_PRESENT) == 0
    }

    /// What is wrong with these fields, if anything: the publisher rules under
    /// the specification's `Bound Flags` table.
    ///
    /// One function for both directions of [`AppMessage::validate`]'s argument.
    /// The encoder refuses what it returns. The decoder does not, and a reader
    /// that wants to judge a publisher asks it here.
    #[must_use]
    pub const fn defect(&self) -> Option<&'static str> {
        let flags = self.bound_flags;
        let has_lower = flags & BOUND_LOWER_PRESENT != 0;
        let has_upper = flags & BOUND_UPPER_PRESENT != 0;
        if flags & BOUND_RESERVED != 0 {
            return Some("Bound Flags bits 4-7 are reserved and must be clear");
        }
        if !has_lower && flags & BOUND_LOWER_INCLUSIVE != 0 {
            return Some("a lower bound that is not present cannot be inclusive");
        }
        if !has_upper && flags & BOUND_UPPER_INCLUSIVE != 0 {
            return Some("an upper bound that is not present cannot be inclusive");
        }
        if !has_lower && self.lower_bound != 0 {
            return Some("Lower Bound must be 0 when it is not present");
        }
        if !has_upper && self.upper_bound != 0 {
            return Some("Upper Bound must be 0 when it is not present");
        }
        if !has_lower && !has_upper && self.fixing_time_ns != 0 {
            return Some("Fixing Time must be 0 while the strike is pending");
        }
        if has_lower && has_upper {
            if self.lower_bound > self.upper_bound {
                return Some("Lower Bound is greater than Upper Bound, so the interval is empty");
            }
            let both_inclusive =
                flags & BOUND_LOWER_INCLUSIVE != 0 && flags & BOUND_UPPER_INCLUSIVE != 0;
            if self.lower_bound == self.upper_bound && !both_inclusive {
                return Some("equal bounds with an exclusive end make the interval empty");
            }
        }
        None
    }

    /// # Errors
    ///
    /// The framing errors only: a short buffer, another Type ID, a declared
    /// length that is not 40.
    ///
    /// **A body that breaks a publisher rule decodes.** The specification gives
    /// a subscriber one rule for every value of `Bound Flags` and tells it to
    /// ignore the reserved bits, so two readers cannot disagree about these
    /// bytes the way they can about a clear bounded on both sides, and refusing
    /// here would hide from a recorder what a publisher sent. [`Self::defect`]
    /// says what is wrong with it. The four reserved bytes at offset 36 are not
    /// kept.
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
        let u64_at = |at: usize| {
            u64::from_le_bytes(
                buf[at..at + 8]
                    .try_into()
                    .expect("range width matches the target array"),
            )
        };
        Ok(Self {
            instrument_id: u32::from_le_bytes(
                buf[4..8]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            strike_exponent: buf[10] as i8,
            bound_flags: buf[11],
            lower_bound: u64_at(12) as i64,
            upper_bound: u64_at(20) as i64,
            fixing_time_ns: u64_at(28),
        })
    }
}

impl AppMessage for StrikeInterval {
    const TYPE_ID: u8 = 0x09;
    const SIZE: usize = 40;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Refdata];

    /// Refuses what [`StrikeInterval::defect`] names, where it is written. A
    /// publisher that sent it would state a strike every conformance run reports
    /// and no subscriber can price against.
    fn validate(&self) -> Result<(), EncodeError> {
        match self.defect() {
            Some(what) => Err(EncodeError::MalformedMessage {
                message: core::any::type_name::<Self>(),
                what,
            }),
            None => Ok(()),
        }
    }

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.strike_exponent as u8;
        dst[11] = self.bound_flags;
        dst[12..20].copy_from_slice(&self.lower_bound.to_le_bytes());
        dst[20..28].copy_from_slice(&self.upper_bound.to_le_bytes());
        dst[28..36].copy_from_slice(&self.fixing_time_ns.to_le_bytes());
        dst[36..40].fill(0);
    }

    // StrikeInterval carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}
