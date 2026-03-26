use solana_ledger::shred::{Shred, ShredType};

/// Extracted fields from a parsed shred.
#[derive(Debug, Clone)]
pub struct ParsedShred {
    pub slot: u64,
    pub index: u32,
    pub is_data: bool,
    pub fec_set_index: u32,
    pub version: u16,
    pub signature: [u8; 64],
}

/// Parse a raw UDP payload into a ParsedShred.
/// Returns None if the payload cannot be parsed as a valid shred.
pub fn parse_shred(payload: &[u8]) -> Option<ParsedShred> {
    let shred = Shred::new_from_serialized_shred(payload.to_vec()).ok()?;
    let sig_ref: &[u8] = shred.signature().as_ref();
    let sig_bytes: [u8; 64] = sig_ref.try_into().ok()?;

    Some(ParsedShred {
        slot: shred.slot(),
        index: shred.index(),
        is_data: shred.shred_type() == ShredType::Data,
        fec_set_index: shred.fec_set_index(),
        version: shred.version(),
        signature: sig_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_garbage_returns_none() {
        let garbage = vec![0u8; 100];
        assert!(parse_shred(&garbage).is_none());
    }

    #[test]
    fn test_parse_empty_returns_none() {
        assert!(parse_shred(&[]).is_none());
    }

    #[test]
    fn test_parse_various_sizes_no_panic() {
        for size in [64, 128, 256, 512, 1024, 1228, 1272] {
            let data = vec![0xFFu8; size];
            let _ = parse_shred(&data);
        }
    }
}
