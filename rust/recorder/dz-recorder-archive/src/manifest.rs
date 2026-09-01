//! What a reader can answer about an object without opening it.
//!
//! Every field is computed from state the writer already holds while the
//! segment is open. Nothing here re-reads the segment: a manifest produced by
//! reading back the object would be a second decode of the same bytes, and the
//! only thing it could add is a second opportunity to disagree.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use dz_recorder_core::{ChannelInstance, RecordedDatagram, SinkError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The offsets the coverage read uses, from the spec's datagram header table.
const CHANNEL_ID_OFFSET: usize = 3;
const SEQUENCE_NUMBER_OFFSET: usize = 4;
const RESET_COUNT_OFFSET: usize = 21;
const DATAGRAM_HEADER_SIZE: usize = 24;

/// The most channel instances one segment will describe.
///
/// An any-source join accepts datagrams from any sender, so the key space is
/// not ours to trust. Past this point the datagrams are still archived and the
/// overflow is counted; only the coverage row is given up.
const MAX_INSTANCES: usize = 4096;

/// One port role the recorder was asked to join, as the index table holds it.
///
/// The group, the port and the interface are here and not only the role, because
/// the row exists to tell a reader what the archive is *supposed* to contain: a
/// port joined on the wrong port is silent in exactly the way a port nobody
/// joined is, and without the intent the analysis tier reports a pass over rules
/// it never ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinedRole {
    /// `mktdata`, `refdata` or `snapshot`, and no alias.
    pub role: String,
    pub group: Ipv4Addr,
    pub port: u16,
    /// Absent when the join was left to route discovery. Never a placeholder:
    /// an interface nothing named is not an interface the archive may claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    /// The source address at join time, when the join reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Ipv4Addr>,
}

/// What one channel instance contributed to one segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCoverage {
    /// In arrival order, not in value order: the segment is a time window, and
    /// what the analysis tier joins across objects is the window's edges.
    pub first_seq: u64,
    pub last_seq: u64,
    pub count: u64,
    /// A reset restarts the sequence space, so a segment that spans one holds
    /// two spaces and says so rather than reading it as backward motion.
    pub reset_counts_seen: Vec<u8>,
}

/// Per-instance coverage, accumulated while the segment is open.
///
/// **The one deliberate exception to "the record path never parses."** The
/// three fields below are read as bare little-endian integers at their fixed
/// offsets, and deliberately not through `DatagramHeader::decode`, which
/// rejects an unknown schema version and would therefore drop the coverage row
/// for exactly the datagram most worth knowing about. The archive holds the
/// bytes either way; this decides only whether the manifest can describe them.
#[derive(Debug, Default)]
pub struct CoverageTracker {
    instances: BTreeMap<ChannelInstance, Coverage>,
    short_datagrams: u64,
    instances_dropped: u64,
}

#[derive(Debug)]
struct Coverage {
    first_seq: u64,
    last_seq: u64,
    count: u64,
    reset_counts_seen: BTreeSet<u8>,
}

impl CoverageTracker {
    pub fn observe(&mut self, dg: &RecordedDatagram<'_>) {
        if dg.payload.len() < DATAGRAM_HEADER_SIZE {
            // Counted rather than skipped: a silent skip makes the manifest
            // disagree with the object for no visible reason.
            self.short_datagrams += 1;
            return;
        }
        let channel_id = dg.payload[CHANNEL_ID_OFFSET];
        let sequence_number = u64::from_le_bytes(
            dg.payload[SEQUENCE_NUMBER_OFFSET..SEQUENCE_NUMBER_OFFSET + 8]
                .try_into()
                .expect("range width matches the target array"),
        );
        let reset_count = dg.payload[RESET_COUNT_OFFSET];
        let key = ChannelInstance::new(*dg.src.ip(), channel_id, dg.dst.port());

        if let Some(cov) = self.instances.get_mut(&key) {
            cov.last_seq = sequence_number;
            cov.count += 1;
            cov.reset_counts_seen.insert(reset_count);
        } else if self.instances.len() >= MAX_INSTANCES {
            self.instances_dropped += 1;
        } else {
            self.instances.insert(
                key,
                Coverage {
                    first_seq: sequence_number,
                    last_seq: sequence_number,
                    count: 1,
                    reset_counts_seen: BTreeSet::from([reset_count]),
                },
            );
        }
    }

    #[must_use]
    pub fn short_datagrams(&self) -> u64 {
        self.short_datagrams
    }

    #[must_use]
    pub fn instances_dropped(&self) -> u64 {
        self.instances_dropped
    }

    #[must_use]
    pub fn coverage(&self) -> BTreeMap<ChannelInstance, InstanceCoverage> {
        self.instances
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    InstanceCoverage {
                        first_seq: v.first_seq,
                        last_seq: v.last_seq,
                        count: v.count,
                        reset_counts_seen: v.reset_counts_seen.iter().copied().collect(),
                    },
                )
            })
            .collect()
    }
}

/// One object's row in the index table, and the object metadata beside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub site: String,
    pub recorder: String,
    pub env: String,
    /// The feed specification's name, never a venue.
    pub feed: String,
    pub build_version: String,
    pub build_commit: String,
    pub config_hash: String,

    pub segment_seq: u64,
    /// Receive timestamps, not send timestamps: this is the window the recorder
    /// can vouch for.
    pub start_ns: u64,
    pub end_ns: u64,

    pub datagram_count: u64,
    pub payload_byte_count: u64,
    /// Filled in by the compressor, because the hash has to cover the bytes a
    /// consumer will actually fetch.
    /// The Hive-partitioned key this object is to land under, relative to a
    /// bucket — not the local file name.
    ///
    /// The recorder does not upload, but it does decide the layout, and this is
    /// where it says so: a shipper that reads the manifest beside the object
    /// needs to know nothing about partitioning. It is also the key the analysis
    /// tier reprocesses on, together with the digest, and a bare file name
    /// cannot serve as one — two recorders at two sites rotate segment 5 in the
    /// same nanosecond and produce the same name for different bytes.
    pub object_key: String,
    pub byte_count: u64,
    /// Hex, so an index-table row is a string and a comparison is textual.
    pub sha256: String,

    #[serde(with = "instances_as_rows")]
    pub instances: BTreeMap<ChannelInstance, InstanceCoverage>,
    /// Datagrams too short to carry a header, archived but not described.
    pub short_datagrams: u64,
    /// Channel instances past the per-segment cap, archived but not described.
    pub instances_dropped: u64,

    /// Our own losses. A gap covered by this is not a finding.
    pub capture_drop_total: u64,
    /// `port-role` or `capture-handle`: the scope `capture_drop_total` and the
    /// segment's `isb_osdrop` totals may be subtracted at. A ring counts frames
    /// dropped before it can tell the roles apart, so at `capture-handle` scope
    /// subtracting these from one role's sequence gaps would be subtracting a
    /// guess — which is how a false publisher-loss finding is made.
    pub capture_drop_scope: String,
    /// Loss upstream of the capture point, which is its own category and not
    /// publisher loss.
    pub interface_drop_total: u64,

    /// What the recorder was asked to join, and where. A port that was never
    /// joined produces no data, and no data looks exactly like a clean feed.
    pub roles_joined: Vec<JoinedRole>,
    /// `captured` or `synthesised`, so no reader mistakes a synthesised field
    /// for an observed one.
    pub link_headers: String,
    /// Datagrams whose own headers contradicted `link_headers`, each one marked
    /// in the object as well. Non-zero means the mode did not deliver what it
    /// claimed, which is a fact about the archive a reader has to have before
    /// trusting a header field in it.
    pub link_header_exceptions: u64,
}

impl SegmentManifest {
    pub fn to_json(&self) -> Result<String, SinkError> {
        serde_json::to_string_pretty(self).map_err(|e| SinkError::Encode(e.to_string()))
    }
}

/// A `ChannelInstance` is not a string, so the map is carried as a row per
/// instance — which is the shape the index table wants anyway.
mod instances_as_rows {
    use super::{
        BTreeMap, ChannelInstance, Deserialize, Deserializer, InstanceCoverage, Ipv4Addr,
        Serialize, Serializer,
    };

    #[derive(Serialize, Deserialize)]
    struct Row {
        source: Ipv4Addr,
        channel_id: u8,
        dst_port: u16,
        #[serde(flatten)]
        coverage: InstanceCoverage,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<ChannelInstance, InstanceCoverage>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let rows: Vec<Row> = map
            .iter()
            .map(|(k, v)| Row {
                source: k.source,
                channel_id: k.channel_id,
                dst_port: k.dst_port,
                coverage: v.clone(),
            })
            .collect();
        rows.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<ChannelInstance, InstanceCoverage>, D::Error> {
        let rows = Vec::<Row>::deserialize(d)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    ChannelInstance::new(r.source, r.channel_id, r.dst_port),
                    r.coverage,
                )
            })
            .collect())
    }
}
