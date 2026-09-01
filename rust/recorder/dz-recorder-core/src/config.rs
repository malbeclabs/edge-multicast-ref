//! Configuration, parsed per crate so that keys, types and defaults cannot
//! drift between recorder hosts.
//!
//! Every struct here carries `deny_unknown_fields`: a misspelled section that
//! parses cleanly and falls back to a default is how a host runs the wrong
//! transport while the operator believes otherwise.
//!
//! Three things configuration deliberately cannot reach: the datagram size cap
//! (see [`CaptureConfig::snaplen`]), a second multicast group for reference
//! data, and drop accounting. Each is an invariant a feed spec or the recorder
//! design already decided. There are no bucket, credential or endpoint keys
//! either, because the recorder does not upload — [`ArchiveConfig`]'s
//! `completed_dir` is the whole interface to whatever ships from it.

use dz_edge_core::MAX_DATAGRAM_SIZE;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Ethernet, IPv4 and UDP: 14 + 20 + 8. These bytes precede every archived
/// payload — captured in AF_PACKET mode, synthesised in socket mode — so they
/// are also what the capture length has to leave room for.
pub const ETHERNET_IPV4_UDP_HEADER_SIZE: usize = 14 + 20 + 8;

/// The longest those same headers can be: 14 + 60 + 8, an IPv4 header carrying
/// the full 40 bytes of options.
///
/// The synthesised case above is not a bound. A capture length sized to it
/// slices the tail off a compliant datagram at the cap whose IPv4 header
/// carries options — and the recorder then counts that datagram as a publisher
/// over the cap, which is a finding it manufactured itself. The archive already
/// declares its snaplen from this constant.
pub const MAX_LINK_HEADER_SIZE: usize = 14 + 60 + 8;

/// Loading a configuration failed.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The message names the offending key, which is the only part an operator
    /// needs, so it is carried through verbatim.
    #[error("configuration is not valid: {0}")]
    Toml(#[from] toml::de::Error),
    /// Two port roles on one feed cannot share a port. Whatever maps a port
    /// back to a role has to pick one, and then every datagram on that port is
    /// attributed to a role it may not belong to — silently, and for the life
    /// of the archive.
    #[error("feed `{spec}` gives port {port} to more than one port role")]
    DuplicatePort { spec: String, port: u16 },
    /// Two feeds claiming one port on one group. A datagram there belongs to
    /// whichever feed a reader happens to check first, which is not a property
    /// worth having.
    #[error("feeds `{first}` and `{second}` both claim {group}:{port}")]
    GroupPortClaimedTwice {
        group: Ipv4Addr,
        port: u16,
        first: String,
        second: String,
    },
}

/// One recorder host's whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecorderConfig {
    /// Label on every `dz_recorder_*` series and on every object key.
    pub site: String,
    /// Unique within the site.
    pub recorder: String,
    pub env: String,
    /// One entry per feed recorded.
    #[serde(default)]
    pub feed: Vec<FeedConfig>,
    #[serde(default)]
    pub capture: CaptureConfig,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl RecorderConfig {
    /// Load from TOML text.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        for feed in &config.feed {
            feed.check_ports_are_distinct()?;
        }
        config.check_groups_do_not_overlap()?;
        Ok(config)
    }

    /// Lowercase hex sha256 of the *parsed* configuration, canonically
    /// serialised.
    ///
    /// Of the parsed form rather than the file bytes because this goes into the
    /// archive as provenance: a finding has to stay attributable to a
    /// configuration across a reformatting, an added comment or a reordered
    /// key, none of which change what the recorder does.
    #[must_use]
    pub fn config_hash(&self) -> String {
        hex(&Sha256::digest(self.canonical_toml().as_bytes()))
    }

    /// Two feeds on one group cannot claim the same port either.
    ///
    /// The check is per `(group, port)` and not per port: the same port number
    /// on two different groups is two different channel instances, which is
    /// ordinary. The same port on the same group is the collision one feed
    /// already forbids, arriving by a longer route — and there it is worse,
    /// because the two feeds have different specs, so a reader would attribute
    /// a datagram to whichever feed was listed first.
    fn check_groups_do_not_overlap(&self) -> Result<(), ConfigError> {
        let mut claimed: Vec<(Ipv4Addr, u16, &str)> = Vec::new();
        for feed in &self.feed {
            for port in feed.ports() {
                if let Some((_, _, first)) = claimed
                    .iter()
                    .find(|(group, taken, _)| *group == feed.multicast_group && *taken == port)
                {
                    return Err(ConfigError::GroupPortClaimedTwice {
                        group: feed.multicast_group,
                        port,
                        first: (*first).to_owned(),
                        second: feed.spec.clone(),
                    });
                }
                claimed.push((feed.multicast_group, port, &feed.spec));
            }
        }
        Ok(())
    }

    /// The canonical form the hash is taken over. It re-parses to an equal
    /// configuration, so the hash is a function of behaviour and nothing else.
    #[must_use]
    pub fn canonical_toml(&self) -> String {
        // A bogus provenance hash in every archive is worse than a loud failure
        // at load: every field here is a scalar, an array of scalars, or a
        // sub-table declared after the scalars, so this cannot fail unless a
        // later field breaks that, in which case it must not go unnoticed.
        toml::to_string(self).expect("the configuration is serialisable as TOML")
    }
}

/// One feed on one multicast group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedConfig {
    /// The feed specification's name.
    pub spec: String,
    /// One group for every port role, per the reference-data supplement.
    pub multicast_group: Ipv4Addr,
    /// The interface the feed arrives on. Unset leaves it to route discovery,
    /// which is wrong exactly when the feed arrives on a tunnel the default
    /// route does not name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    pub mktdata_port: u16,
    pub refdata_port: u16,
    /// Depth feeds only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_port: Option<u16>,
    /// Empty means no expectation stated. See [`Self::admits_every_source`].
    #[serde(default)]
    pub expected_sources: Vec<Ipv4Addr>,
}

impl FeedConfig {
    /// Always true, and it is a method rather than a comment so that a future
    /// change here has to be a deliberate one.
    ///
    /// `expected_sources` gates counting and alerting, never the archive. A
    /// wrongly recorded datagram is filterable afterwards on the source
    /// address; a wrongly dropped one is gone.
    #[must_use]
    pub fn admits_every_source(&self) -> bool {
        true
    }

    /// A port belongs to one port role.
    ///
    /// Only a role collision is rejected here. A port of zero parses, because
    /// the design's own example uses it as the value an operator has yet to
    /// fill in, and refusing it is the job of whatever starts a recorder rather
    /// than of whatever reads a file — a half-written configuration should fail
    /// where somebody is watching, not where somebody is editing.
    fn ports(&self) -> Vec<u16> {
        let mut ports = vec![self.mktdata_port, self.refdata_port];
        if let Some(snapshot) = self.snapshot_port {
            ports.push(snapshot);
        }
        ports
    }

    fn check_ports_are_distinct(&self) -> Result<(), ConfigError> {
        let ports = self.ports();
        for (i, port) in ports.iter().enumerate() {
            if ports[i + 1..].contains(port) {
                return Err(ConfigError::DuplicatePort {
                    spec: self.spec.clone(),
                    port: *port,
                });
            }
        }
        Ok(())
    }
}

/// Where the bytes are taken from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CaptureMode {
    /// The default: it records what the network delivered to the interface
    /// rather than what one socket survived.
    Afpacket,
    /// Where `CAP_NET_RAW` is unavailable.
    Socket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CaptureConfig {
    pub mode: CaptureMode,
    /// The AF_PACKET ring, or the socket receive buffer in socket mode.
    #[serde(deserialize_with = "de_byte_size", serialize_with = "ser_byte_size")]
    pub buffer: u64,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            mode: CaptureMode::Afpacket,
            buffer: 64 * MIB,
        }
    }
}

impl CaptureConfig {
    /// The capture length: the mandated datagram cap plus the 42 bytes of
    /// The mandated cap plus the longest link headers that can precede it.
    ///
    /// Computed, never configured. Every feed spec mandates the cap, so no key
    /// may raise it — the same discipline the publisher's builder applies, for
    /// the same reason: a key that can express a larger value is how the cap
    /// drifted the first time. What the headers may be is not the same
    /// question: sizing to the synthesised 42 truncates a compliant datagram
    /// whose IPv4 header carries options, and truncation is what the recorder
    /// reports as a publisher violation.
    #[must_use]
    pub const fn snaplen(&self) -> usize {
        MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_SIZE
    }
}

/// How a rotated segment is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Recent Wireshark reads it directly, and the payloads are dense
    /// fixed-size structures that compress several-fold.
    Zstd,
    /// For environments that need the older guarantee, at a worse ratio.
    ///
    /// No writer implements this yet. Whatever maps a configuration onto a
    /// writer must refuse it loudly rather than fall back to zstd: an operator
    /// who asked for the older guarantee and silently got the newer one has
    /// objects a reader in that environment cannot open.
    Gzip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ArchiveConfig {
    /// The segment currently being written. Empty by default, which is not a
    /// usable path: there is no defensible host path to invent here, so a
    /// recorder that archives has to state its own and the binary that wires
    /// one up rejects an empty value rather than writing somewhere surprising.
    pub staging_dir: PathBuf,
    /// Rotated, hashed and manifested. The whole interface to the shipper.
    pub completed_dir: PathBuf,
    /// Rotation fires on size or age, whichever comes first: a size bound keeps
    /// objects uniform for the analysis tier, and an age bound keeps a
    /// low-volume feed's data off a local disk for hours.
    #[serde(deserialize_with = "de_byte_size", serialize_with = "ser_byte_size")]
    pub rotate_bytes: u64,
    #[serde(deserialize_with = "de_duration", serialize_with = "ser_duration")]
    pub rotate_interval: Duration,
    pub compression: Compression,
    /// The buffer for a storage outage, sized as retention × measured bytes per
    /// second. When it fills, the oldest completed segment is evicted and
    /// counted — the capture path is never blocked, because a writer that
    /// blocks on a full disk converts a storage outage into feed loss and into
    /// false publisher-loss findings in every archive written during it.
    #[serde(deserialize_with = "de_byte_size", serialize_with = "ser_byte_size")]
    pub staging_max: u64,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            staging_dir: PathBuf::new(),
            completed_dir: PathBuf::new(),
            rotate_bytes: 256 * MIB,
            rotate_interval: Duration::from_secs(60),
            compression: Compression::Zstd,
            staging_max: 64 * GIB,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HealthConfig {
    /// Off by default: the header-only tier is the one that stays correct under
    /// loss, and walking messages spends drain-thread time to learn what the
    /// analysis tier already learns offline from the archive.
    pub walk_messages: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    pub listen_addr: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 9100)),
        }
    }
}

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;

/// Split a value written as digits immediately followed by a unit.
fn split_unit(raw: &str) -> Result<(u64, &str), String> {
    let text = raw.trim();
    let boundary = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{text}` has no unit"))?;
    let (digits, unit) = text.split_at(boundary);
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a whole number followed by a unit"))?;
    Ok((value, unit))
}

/// Sizes are written with a unit — `"64MiB"`, `"64GiB"` — and a bare number is
/// rejected rather than guessed at, because both plausible guesses for a disk
/// budget are wrong by orders of magnitude.
fn parse_byte_size(raw: &str) -> Result<u64, String> {
    let (value, unit) = split_unit(raw)?;
    let scale = match unit {
        "B" => 1,
        "KiB" => KIB,
        "MiB" => MIB,
        "GiB" => GIB,
        "TiB" => TIB,
        _ => {
            return Err(format!(
                "`{unit}` is not a size unit (B, KiB, MiB, GiB, TiB)"
            ))
        }
    };
    value
        .checked_mul(scale)
        .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit byte count"))
}

/// Durations are written with a unit — `"60s"` — for the same reason sizes are.
fn parse_duration(raw: &str) -> Result<Duration, String> {
    let (value, unit) = split_unit(raw)?;
    let nanos = match unit {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        _ => {
            return Err(format!(
                "`{unit}` is not a duration unit (ns, us, ms, s, m, h)"
            ))
        }
    };
    value
        .checked_mul(nanos)
        .map(Duration::from_nanos)
        .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit nanosecond count"))
}

fn de_byte_size<'de, D: Deserializer<'de>>(de: D) -> Result<u64, D::Error> {
    let raw = String::deserialize(de)?;
    parse_byte_size(&raw).map_err(D::Error::custom)
}

fn de_duration<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
    let raw = String::deserialize(de)?;
    parse_duration(&raw).map_err(D::Error::custom)
}

/// Emitted in the base unit so that two spellings of one size — `"64MiB"` and
/// `"65536KiB"` — reach the hash as the same bytes, and so that the canonical
/// form is still a configuration this parser accepts.
fn ser_byte_size<S: Serializer>(value: &u64, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(&format!("{value}B"))
}

fn ser_duration<S: Serializer>(value: &Duration, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(&format!("{}ns", value.as_nanos()))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}
