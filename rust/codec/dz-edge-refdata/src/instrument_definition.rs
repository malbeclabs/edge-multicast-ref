use dz_edge_core::{AppMessage, DecodeError, SCHEMA_VERSION, SCHEMA_VERSION_V1};

pub const SYMBOL_LEN: usize = 64;
/// The schema-1 `Symbol` width, before the 2.0.0 widening.
pub const SYMBOL_LEN_V1: usize = 16;
pub const LEG_LEN: usize = 8;

/// The schema-1 message size: 50 bytes shorter, and no `Source ID`.
pub const SIZE_V1: usize = 80;

pub const ASSET_CLASS_UNKNOWN: u8 = 0;
pub const ASSET_CLASS_CRYPTO_SPOT: u8 = 1;
pub const ASSET_CLASS_PREDICTION_BINARY: u8 = 2;
pub const ASSET_CLASS_PREDICTION_SCALAR: u8 = 3;
pub const ASSET_CLASS_PREDICTION_CATEGORICAL: u8 = 4;
pub const ASSET_CLASS_PERPETUAL_FUTURE: u8 = 5;

pub const MARKET_MODEL_UNKNOWN: u8 = 0;
pub const MARKET_MODEL_CLOB: u8 = 1;
pub const MARKET_MODEL_AMM: u8 = 2;

pub const SETTLE_TYPE_NA: u8 = 0;
pub const SETTLE_TYPE_CASH: u8 = 1;
pub const SETTLE_TYPE_PHYSICAL: u8 = 2;

pub const PRICE_BOUND_UNBOUNDED: u8 = 0;
pub const PRICE_BOUND_UNIT_INTERVAL: u8 = 1;
pub const PRICE_BOUND_NON_NEGATIVE: u8 = 2;

/// `0x02 InstrumentDefinition` (130 bytes at schema 3).
///
/// The only message whose layout changed between generations, which is why it
/// carries the dual-version burden alone. Encoded at schema 3; decodable at 1
/// and 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentDefinition {
    pub instrument_id: u32,
    /// Absent at schema 1, where it decodes as 0.
    pub source_id: u16,
    pub symbol: [u8; SYMBOL_LEN],
    pub leg1: [u8; LEG_LEN],
    pub leg2: [u8; LEG_LEN],
    pub asset_class: u8,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub market_model: u8,
    pub tick_size: i64,
    pub lot_size: u64,
    pub contract_value: u64,
    pub expiry_ns: u64,
    pub settle_type: u8,
    pub price_bound: u8,
    pub manifest_seq: u16,
}

impl AppMessage for InstrumentDefinition {
    const TYPE_ID: u8 = 0x02;
    const SIZE: usize = 130;

    fn encode_into(&self, dst: &mut [u8]) {
        assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10..74].copy_from_slice(&self.symbol);
        dst[74..82].copy_from_slice(&self.leg1);
        dst[82..90].copy_from_slice(&self.leg2);
        dst[90] = self.asset_class;
        dst[91] = self.price_exponent as u8;
        dst[92] = self.qty_exponent as u8;
        dst[93] = self.market_model;
        dst[94..102].copy_from_slice(&self.tick_size.to_le_bytes());
        dst[102..110].copy_from_slice(&self.lot_size.to_le_bytes());
        dst[110..118].copy_from_slice(&self.contract_value.to_le_bytes());
        dst[118..126].copy_from_slice(&self.expiry_ns.to_le_bytes());
        dst[126] = self.settle_type;
        dst[127] = self.price_bound;
        dst[128..130].copy_from_slice(&self.manifest_seq.to_le_bytes());
    }
}

impl InstrumentDefinition {
    /// Decode at the generation the datagram header declared.
    ///
    /// Schema 2 is refused: the 128-byte layout was superseded before any
    /// publisher emitted it, so accepting it would invent a generation.
    pub fn decode(buf: &[u8], schema_version: u8) -> Result<Self, DecodeError> {
        match schema_version {
            SCHEMA_VERSION => Self::decode_v3(buf),
            SCHEMA_VERSION_V1 => Self::decode_v1(buf),
            other => Err(DecodeError::UnsupportedSchema(other)),
        }
    }

    fn decode_v3(buf: &[u8]) -> Result<Self, DecodeError> {
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
        let mut symbol = [0u8; SYMBOL_LEN];
        symbol.copy_from_slice(&buf[10..74]);
        let mut leg1 = [0u8; LEG_LEN];
        leg1.copy_from_slice(&buf[74..82]);
        let mut leg2 = [0u8; LEG_LEN];
        leg2.copy_from_slice(&buf[82..90]);
        Ok(Self {
            instrument_id: u32::from_le_bytes(
                buf[4..8]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            symbol,
            leg1,
            leg2,
            asset_class: buf[90],
            price_exponent: buf[91] as i8,
            qty_exponent: buf[92] as i8,
            market_model: buf[93],
            tick_size: i64::from_le_bytes(
                buf[94..102]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            lot_size: u64::from_le_bytes(
                buf[102..110]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            contract_value: u64::from_le_bytes(
                buf[110..118]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            expiry_ns: u64::from_le_bytes(
                buf[118..126]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            settle_type: buf[126],
            price_bound: buf[127],
            manifest_seq: u16::from_le_bytes([buf[128], buf[129]]),
        })
    }

    fn decode_v1(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < SIZE_V1 {
            return Err(DecodeError::ShortBuffer {
                need: SIZE_V1,
                got: buf.len(),
            });
        }
        if buf[0] != Self::TYPE_ID {
            return Err(DecodeError::BadTypeId(buf[0]));
        }
        if buf[1] as usize != SIZE_V1 {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: SIZE_V1 as u8,
            });
        }
        // Schema 1 has no Source ID and a char[16] Symbol, so every field after
        // Instrument ID sits 50 bytes earlier than at schema 3.
        let mut symbol = [0u8; SYMBOL_LEN];
        symbol[..SYMBOL_LEN_V1].copy_from_slice(&buf[8..24]);
        let mut leg1 = [0u8; LEG_LEN];
        leg1.copy_from_slice(&buf[24..32]);
        let mut leg2 = [0u8; LEG_LEN];
        leg2.copy_from_slice(&buf[32..40]);
        Ok(Self {
            instrument_id: u32::from_le_bytes(
                buf[4..8]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            source_id: 0,
            symbol,
            leg1,
            leg2,
            asset_class: buf[40],
            price_exponent: buf[41] as i8,
            qty_exponent: buf[42] as i8,
            market_model: buf[43],
            tick_size: i64::from_le_bytes(
                buf[44..52]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            lot_size: u64::from_le_bytes(
                buf[52..60]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            contract_value: u64::from_le_bytes(
                buf[60..68]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            expiry_ns: u64::from_le_bytes(
                buf[68..76]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            settle_type: buf[76],
            price_bound: buf[77],
            manifest_seq: u16::from_le_bytes([buf[78], buf[79]]),
        })
    }
}
