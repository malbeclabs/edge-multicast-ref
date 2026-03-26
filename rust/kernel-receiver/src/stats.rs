use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

/// First 8 bytes of the shred signature, used as a proxy leader identifier.
pub type SignaturePrefix = [u8; 8];

#[derive(Debug, Clone)]
pub struct SlotStats {
    pub slot: u64,
    pub data_shred_count: u64,
    pub coding_shred_count: u64,
    pub highest_data_index: u32,
    pub fec_set_count: usize,
    pub signature_prefix: SignaturePrefix,
    pub first_seen: Instant,
    pub last_seen: Instant,
    fec_set_indices: HashSet<u32>,
}

impl SlotStats {
    fn new(slot: u64, signature: [u8; 64]) -> Self {
        let now = Instant::now();
        let mut sig_prefix = [0u8; 8];
        sig_prefix.copy_from_slice(&signature[..8]);
        Self {
            slot,
            data_shred_count: 0,
            coding_shred_count: 0,
            highest_data_index: 0,
            fec_set_count: 0,
            signature_prefix: sig_prefix,
            first_seen: now,
            last_seen: now,
            fec_set_indices: HashSet::new(),
        }
    }

    fn record(&mut self, is_data: bool, index: u32, fec_set_index: u32) {
        self.last_seen = Instant::now();
        if is_data {
            self.data_shred_count += 1;
            if index > self.highest_data_index {
                self.highest_data_index = index;
            }
        } else {
            self.coding_shred_count += 1;
        }
        self.fec_set_indices.insert(fec_set_index);
        self.fec_set_count = self.fec_set_indices.len();
    }
}

#[derive(Debug)]
pub struct Stats {
    pub total_data_shreds: u64,
    pub total_coding_shreds: u64,
    pub total_heartbeats: u64,
    pub parse_errors: u64,
    pub last_heartbeat: Option<Instant>,
    pub start_time: Instant,

    /// Recent slots ordered by slot number. Bounded by `max_slots`.
    pub slots: BTreeMap<u64, SlotStats>,
    max_slots: usize,

    /// Timestamps of recent shreds for rate calculation.
    rate_window: Vec<Instant>,
}

impl Stats {
    pub fn new(max_slots: usize) -> Self {
        Self {
            total_data_shreds: 0,
            total_coding_shreds: 0,
            total_heartbeats: 0,
            parse_errors: 0,
            last_heartbeat: None,
            start_time: Instant::now(),
            slots: BTreeMap::new(),
            max_slots,
            rate_window: Vec::new(),
        }
    }

    pub fn record_shred(
        &mut self,
        slot: u64,
        is_data: bool,
        index: u32,
        fec_set_index: u32,
        signature: [u8; 64],
    ) {
        if is_data {
            self.total_data_shreds += 1;
        } else {
            self.total_coding_shreds += 1;
        }

        let slot_stats = self
            .slots
            .entry(slot)
            .or_insert_with(|| SlotStats::new(slot, signature));
        slot_stats.record(is_data, index, fec_set_index);

        // Evict oldest slots if over capacity
        while self.slots.len() > self.max_slots {
            if let Some((&oldest, _)) = self.slots.iter().next() {
                self.slots.remove(&oldest);
            }
        }

        // Track for rate calculation
        self.rate_window.push(Instant::now());
    }

    pub fn record_heartbeat(&mut self) {
        self.total_heartbeats += 1;
        self.last_heartbeat = Some(Instant::now());
    }

    pub fn record_parse_error(&mut self) {
        self.parse_errors += 1;
    }

    pub fn shreds_per_second(&mut self) -> f64 {
        let now = Instant::now();
        let one_sec_ago = now - std::time::Duration::from_secs(1);

        // Remove entries older than 1 second
        self.rate_window.retain(|t| *t >= one_sec_ago);

        self.rate_window.len() as f64
    }

    pub fn get_slot(&self, slot: u64) -> Option<&SlotStats> {
        self.slots.get(&slot)
    }

    /// Returns recent slots in descending order (newest first).
    pub fn recent_slots(&self) -> Vec<&SlotStats> {
        self.slots.values().rev().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats() {
        let stats = Stats::new(4);
        assert_eq!(stats.total_data_shreds, 0);
        assert_eq!(stats.total_coding_shreds, 0);
        assert_eq!(stats.total_heartbeats, 0);
        assert_eq!(stats.parse_errors, 0);
        assert_eq!(stats.slots.len(), 0);
    }

    #[test]
    fn test_record_shred_data() {
        let mut stats = Stats::new(4);
        stats.record_shred(100, true, 0, 0, [0xAB; 64]);
        assert_eq!(stats.total_data_shreds, 1);
        assert_eq!(stats.total_coding_shreds, 0);
        assert_eq!(stats.slots.len(), 1);

        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.slot, 100);
        assert_eq!(slot.data_shred_count, 1);
        assert_eq!(slot.coding_shred_count, 0);
        assert_eq!(slot.highest_data_index, 0);
    }

    #[test]
    fn test_record_shred_coding() {
        let mut stats = Stats::new(4);
        stats.record_shred(100, false, 5, 0, [0xAB; 64]);
        assert_eq!(stats.total_data_shreds, 0);
        assert_eq!(stats.total_coding_shreds, 1);

        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.coding_shred_count, 1);
    }

    #[test]
    fn test_multiple_shreds_same_slot() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        stats.record_shred(100, true, 0, 0, sig);
        stats.record_shred(100, true, 1, 0, sig);
        stats.record_shred(100, true, 5, 1, sig);
        stats.record_shred(100, false, 0, 0, sig);

        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.data_shred_count, 3);
        assert_eq!(slot.coding_shred_count, 1);
        assert_eq!(slot.highest_data_index, 5);
        assert_eq!(slot.fec_set_count, 2);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        for slot in 0..6 {
            stats.record_shred(slot, true, 0, 0, sig);
        }
        assert_eq!(stats.slots.len(), 4);
        assert!(stats.get_slot(0).is_none());
        assert!(stats.get_slot(1).is_none());
        assert!(stats.get_slot(2).is_some());
        assert!(stats.get_slot(5).is_some());
    }

    #[test]
    fn test_heartbeat_counting() {
        let mut stats = Stats::new(4);
        stats.record_heartbeat();
        stats.record_heartbeat();
        assert_eq!(stats.total_heartbeats, 2);
        assert!(stats.last_heartbeat.is_some());
    }

    #[test]
    fn test_shreds_per_second() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        for i in 0..10 {
            stats.record_shred(100, true, i, 0, sig);
        }
        let _rate = stats.shreds_per_second();
    }
}
