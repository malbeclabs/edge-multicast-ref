//! Everything decided before a datagram is recorded, and everything refused
//! there.
//!
//! A recorder that starts on a guess records the wrong thing, and an archive of
//! the wrong thing is worse than no archive: it looks exactly like an archive of
//! the right thing until somebody draws a finding from it. So a configuration
//! that is incomplete, that contradicts itself, or that asks for something this
//! build cannot do fails here, naming the key, before the first join.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::{
    ArchiveWriterConfig, CaptureDropScope, Compression as ArchiveCompression, LinkHeaders, RoleJoin,
};
use dz_recorder_capture::{device_address, DeviceAddressError, PortBinding};
use dz_recorder_core::{
    CaptureMode, Compression, ConfigError, FeedConfig, RecorderConfig, RecorderIdentity,
};
use thiserror::Error;

use crate::identity::identity_of;

/// The zstd level every object is written at.
///
/// A constant and not a key: the configuration deliberately has no compression
/// level, because the ratio the host's staging budget was sized against is a
/// property of the archive rather than of one host's taste, and two recorders
/// whose objects compress differently make a storage estimate meaningless. 3 is
/// zstd's own default and the point its ratio curve is measured at.
const ZSTD_LEVEL: i32 = 3;

/// What a recorder refuses to start on.
#[derive(Debug, Error)]
pub enum StartupError {
    #[error("reading {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Config { path: PathBuf, source: ConfigError },

    #[error("`{key}` is empty. {why}")]
    EmptyIdentity {
        key: &'static str,
        why: &'static str,
    },

    #[error("`feed[{spec}].interface` = `{interface}`: {source}")]
    InterfaceAddress {
        spec: String,
        interface: String,
        source: DeviceAddressError,
    },

    #[error(
        "there is no `[[feed]]` to record. A recorder that joins nothing archives nothing, and \
         nothing looks exactly like a clean feed."
    )]
    NoFeeds,

    #[error(
        "`feed[{index}].spec` is empty. It is the `feed=` partition of every object key this feed \
         writes and the `feed` label on every series it produces, so it is the recorder's to state."
    )]
    FeedSpecEmpty { index: usize },

    #[error(
        "`feed.spec` = `{spec}` is not a name that can be an object-key partition and a directory \
         component. Use letters, digits, `.`, `-` and `_`."
    )]
    FeedSpecNotAName { spec: String },

    #[error(
        "two feeds share `spec` = `{spec}`. The spec partitions the object key and labels every \
         series, so both feeds would write into one partition with two segment sequences each \
         starting at zero, and neither the objects nor the series could be told apart."
    )]
    DuplicateFeedSpec { spec: String },

    #[error(
        "`feed.{key}` is 0 for feed `{spec}`. Zero is the value a half-written configuration \
         carries rather than a port anything publishes on, and a port role that was never joined \
         produces no data — which looks exactly like a clean feed."
    )]
    PortNotStated { spec: String, key: &'static str },

    #[error(
        "`feed.multicast_group` = `{group}` for feed `{spec}` is not a multicast address. Nothing \
         would ever be delivered to the join, and a recorder that is delivered nothing looks \
         exactly like a clean feed."
    )]
    GroupNotMulticast { spec: String, group: Ipv4Addr },

    #[error(
        "`feed.interface` = `{interface}` for feed `{spec}` is an interface name, and socket mode \
         joins the group on the interface's IPv4 address: the IGMP report has to leave by the \
         interface the feed arrives on, and this build cannot resolve a name to an address. State \
         the address here, or leave `feed.interface` unset to accept route discovery."
    )]
    InterfaceIsNotAnAddress { spec: String, interface: String },

    #[error(
        "`feed.interface` = `{interface}` for feed `{spec}` is an address, and AF_PACKET mode \
         captures on a device the kernel names rather than on an address. State the interface's \
         name here."
    )]
    InterfaceIsNotADevice { spec: String, interface: String },

    #[error(
        "`feed.interface` is unset for feed `{spec}`, and AF_PACKET mode has to be told which \
         interface to capture on: a capture with no device would archive every datagram on the \
         host."
    )]
    InterfaceUnset { spec: String },

    /// Only a build that has to make this refusal carries it: with the feature
    /// compiled in there is no configuration that can reach it.
    #[cfg(not(feature = "afpacket"))]
    #[error(
        "`capture.mode` = \"afpacket\", but this build has no AF_PACKET support compiled in. \
         Rebuild with `--features afpacket`, which needs libpcap-dev at build time, or set \
         `capture.mode` = \"socket\". Recording through the fallback would archive what one \
         socket survived while the configuration asked for what the network delivered."
    )]
    AfPacketNotCompiledIn,

    #[error(
        "`archive.compression` = \"gzip\", and no writer implements it. Falling back to zstd would \
         hand an operator who asked for the older guarantee a set of objects their environment \
         cannot open, so this is refused rather than substituted. Set `archive.compression` = \
         \"zstd\"."
    )]
    GzipNotImplemented,

    #[error(
        "`archive.{key}` is empty, and there is no defensible host path to invent for it. {why}"
    )]
    DirectoryNotStated {
        key: &'static str,
        why: &'static str,
    },

    #[error(
        "`archive.staging_dir` and `archive.completed_dir` are the same directory. The staging \
         budget scans both, so every object in it would be counted twice and the outage buffer \
         would evict at half the size configured."
    )]
    DirectoriesAreTheSame,

    #[error(
        "`archive.staging_max` is {staging_max} bytes, which is {per_feed} bytes for each of the \
         {feeds} and below `archive.rotate_bytes` ({rotate_bytes}). Every segment would be evicted \
         as soon as it was written, and the archive would stay empty while the recorder looked \
         healthy."
    )]
    StagingBudgetTooSmall {
        staging_max: u64,
        /// Rendered rather than counted, so the sentence reads as one: an
        /// operator reading `1 feeds` stops to work out whether the recorder
        /// has miscounted.
        feeds: String,
        per_feed: u64,
        rotate_bytes: u64,
    },

    /// The configuration states an archive and the command line states inline
    /// mode.
    ///
    /// Both statements are on the screen, deliberately. An operator who reaches
    /// this has written one of them and inherited the other, and which is which
    /// is not something this process can know — so it names both rather than
    /// choosing one to call the mistake.
    ///
    /// Refused and never resolved. Reading it as inline mode leaves a host
    /// somebody believes is keeping bytes for a year that it never kept for a
    /// second; reading it as archive mode leaves a host that was asked for rows
    /// keeping bytes instead. There is no third reading, and a recorder that
    /// picked one would be picking which of an operator's two statements to
    /// ignore.
    #[error(
        "`archive.{key}` = `{path}` states an archive, and `--inline-config {inline_config}` \
         states inline mode. One process runs one arrangement: archive mode writes every \
         datagram to an object and derives no row, inline mode derives rows and keeps no \
         datagram. Remove the `[archive]` directories to derive rows, or drop `--inline-config` \
         to record the archive this configuration describes."
    )]
    BothArrangementsStated {
        key: &'static str,
        path: String,
        inline_config: String,
    },

    /// Nothing states an arrangement.
    ///
    /// This is what silence means, and it is the row that makes the other three
    /// a reading rather than a guess: with no default, a configuration that says
    /// nothing about the arrangement says nothing at all, and is refused before
    /// a socket is bound.
    ///
    /// Named for the shape of the failure and not for either mode, because an
    /// operator who reaches it has not chosen one. Five keys could have settled
    /// it and none of them is defaulted, which is the whole reason there is
    /// nothing here to invent.
    #[error(
        "nothing states an arrangement. Archive mode is selected by `archive.staging_dir` and \
         `archive.completed_dir`, inline mode by `--inline-config`, and neither is given. Not one \
         of the five keys that would settle it has a defensible value to invent: a recorder that \
         guessed would keep bytes on a disk nobody sized or load rows into a database nobody \
         chose. State the two directories to record an archive, or the file to derive rows."
    )]
    NoArrangementStated,
}

/// What `feed.interface` states, before a mode has said what it can use.
///
/// The key is one string and the two capture modes need different things from
/// it — socket mode an address to join on, AF_PACKET mode a device to capture
/// on — so the form is parsed here and the mode does the refusing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Interface {
    /// The key was left unset: the design's stated meaning is route discovery.
    Discovered,
    /// An IPv4 address, which is what a membership join takes.
    Address(Ipv4Addr),
    /// A name, which is what libpcap opens a capture on.
    Device(String),
}

impl Interface {
    #[must_use]
    pub fn parse(stated: Option<&str>) -> Self {
        match stated {
            None => Self::Discovered,
            Some(text) => match text.parse::<Ipv4Addr>() {
                Ok(address) => Self::Address(address),
                Err(_) => Self::Device(text.to_owned()),
            },
        }
    }
}

/// The capture handle's own loss accounting scope, which is a fact about the
/// handle and never a preference.
///
/// Socket mode holds one socket per port role and therefore one `SO_RXQ_OVFL`
/// accumulator per role. An `AF_PACKET` ring counts the frames it could not fit
/// *before* anything has looked at a port, so its loss belongs to the handle and
/// to no role in particular. Recording either as the other makes every later
/// subtraction wrong in the direction that manufactures publisher loss.
#[must_use]
pub const fn drop_scope(mode: CaptureMode) -> CaptureDropScope {
    match mode {
        CaptureMode::Socket => CaptureDropScope::PortRole,
        CaptureMode::Afpacket => CaptureDropScope::CaptureHandle,
    }
}

/// Whether the link headers in the archive were on the wire or assembled.
///
/// Also a fact about the mode: socket mode never sees an Ethernet or IPv4
/// header, so its archive states that its own are synthesised and no reader
/// mistakes one for a captured field.
#[must_use]
pub const fn link_headers(mode: CaptureMode) -> LinkHeaders {
    match mode {
        CaptureMode::Socket => LinkHeaders::Synthesised,
        CaptureMode::Afpacket => LinkHeaders::Captured,
    }
}

/// The configured compression, or the refusal.
pub fn compression(compression: Compression) -> Result<ArchiveCompression, StartupError> {
    match compression {
        Compression::Zstd => Ok(ArchiveCompression::Zstd { level: ZSTD_LEVEL }),
        Compression::Gzip => Err(StartupError::GzipNotImplemented),
    }
}

/// Which of the two arrangements this host is running.
///
/// Not a capture mode and not a preference: it decides whether an archive is
/// written at all, and therefore whether a feed has an [`ArchiveWriterConfig`]
/// to be wired with. The two are mutually exclusive by construction — archive
/// mode requires the directories and inline mode refuses them — which is why
/// this is a plan-level fact rather than a flag a caller could forget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrangement {
    /// Objects are written and a loader derives rows from them.
    ///
    /// Selected by `archive.staging_dir` and `archive.completed_dir` carrying a
    /// value, which is what this arrangement has required since it existed — so
    /// a host that recorded an archive yesterday selects it by saying what it
    /// already said.
    Archive,
    /// Rows are derived from the live capture and no datagram is kept.
    ///
    /// Selected by `--inline-config`, which names the file carrying the spool
    /// directory, the ledger and the destination this arrangement cannot run
    /// without.
    ///
    /// **Constructible in every build, feature or not.** It used to be behind
    /// the feature, on the argument that a variant a build can never construct
    /// is one its dead-code analysis is right to flag. That is no longer true:
    /// a build without inline mode constructs this variant in order to *refuse*
    /// it, which is the one thing such a build must do rather than fall back to
    /// the arrangement nobody chose.
    Inline,
}

impl Arrangement {
    /// Which arrangement a configuration and a command line state, or the
    /// refusal that neither or both were stated.
    ///
    /// **The four cases live here and nowhere else**, and the two predicates
    /// they are drawn from are the two the refusals name: an archive directory
    /// carrying a value, and a second file given. Each arrangement requires what
    /// the other has no use for and no member of either key set is defaulted, so
    /// the four are total — which is asserted in its own test, because four
    /// cases enumerated by hand are four cases somebody can add a fifth to.
    ///
    /// Either directory is enough to state an archive, not both. A
    /// half-stated archive is a *statement* about the arrangement and an
    /// incomplete one about the archive, so it selects archive mode and is then
    /// refused by [`StartupError::DirectoryNotStated`], naming the key that is
    /// missing. Reading it as inline mode instead would answer an operator who
    /// wrote `staging_dir` with a complaint about a spool directory they have
    /// never heard of.
    ///
    /// Compiled in every build. A build without inline mode is precisely the
    /// build that has to refuse a configuration selecting it, and a refusal
    /// that exists only in the builds which do not need it is no refusal at
    /// all.
    ///
    /// # Errors
    ///
    /// [`StartupError::BothArrangementsStated`] or
    /// [`StartupError::NoArrangementStated`], each naming both statements rather
    /// than choosing one of them to call the mistake.
    pub fn selected_by(
        config: &RecorderConfig,
        inline_config: Option<&Path>,
    ) -> Result<Self, StartupError> {
        let stated = archive_directory_stated(config);
        match (stated, inline_config) {
            (Some(_), None) => Ok(Self::Archive),
            (None, Some(_)) => Ok(Self::Inline),
            (Some((key, path)), Some(file)) => Err(StartupError::BothArrangementsStated {
                key,
                path: path.display().to_string(),
                inline_config: file.display().to_string(),
            }),
            (None, None) => Err(StartupError::NoArrangementStated),
        }
    }
}

/// The first archive directory carrying a value, and the key it was written
/// under.
///
/// The key travels with the path because the refusals name it: an operator who
/// stated `completed_dir` and not `staging_dir` is told about the one they
/// wrote, and a message naming the other would read as a complaint about a key
/// they never touched.
fn archive_directory_stated(config: &RecorderConfig) -> Option<(&'static str, &Path)> {
    [
        ("staging_dir", config.archive.staging_dir.as_path()),
        ("completed_dir", config.archive.completed_dir.as_path()),
    ]
    .into_iter()
    .find(|(_, dir)| !dir.as_os_str().is_empty())
}

/// One feed, wired.
#[derive(Debug, Clone)]
pub struct FeedPlan {
    pub spec: String,
    /// One per port role this feed carries, in the order the roles are declared.
    pub bindings: Vec<PortBinding>,
    pub port_roles: Vec<PortRole>,
    pub expected_sources: Vec<Ipv4Addr>,
    /// The `Channel ID`s an operator declared. Empty states nothing, which is
    /// the common case and not a declaration that the feed has none.
    pub expected_channel_ids: Vec<u8>,
    /// The address the membership is joined on. Unspecified means the key was
    /// left unset and route discovery was asked for.
    pub membership_interface: Ipv4Addr,
    /// The capture device, in AF_PACKET mode. `None` in socket mode, which
    /// captures on the sockets it joined and has no device to open.
    pub device: Option<String>,
    /// `None` in inline mode, where nothing writes an object. Absent rather than
    /// a value nobody uses: a writer configuration built for a mode that never
    /// opens a writer is a set of directories an operator would reasonably
    /// expect to find objects in.
    pub archive: Option<ArchiveWriterConfig>,
}

impl FeedPlan {
    /// The datagram rate limit and every other per-datagram bound live in the
    /// crates; what is decided here is only what the configuration states.
    fn new(
        feed: &FeedConfig,
        index: usize,
        config: &RecorderConfig,
        identity: &RecorderIdentity,
        staging_max: u64,
        arrangement: Arrangement,
    ) -> Result<Self, StartupError> {
        let spec = check_spec(feed, index)?;
        if !feed.multicast_group.is_multicast() {
            return Err(StartupError::GroupNotMulticast {
                spec,
                group: feed.multicast_group,
            });
        }

        let mut bindings = vec![
            binding(
                feed,
                PortRole::Mktdata,
                feed.mktdata_port,
                "mktdata_port",
                &spec,
            )?,
            binding(
                feed,
                PortRole::Refdata,
                feed.refdata_port,
                "refdata_port",
                &spec,
            )?,
        ];
        if let Some(port) = feed.snapshot_port {
            bindings.push(binding(
                feed,
                PortRole::Snapshot,
                port,
                "snapshot_port",
                &spec,
            )?);
        }

        let stated = Interface::parse(feed.interface.as_deref());
        let (membership_interface, device) =
            resolve_interface(&stated, config.capture.mode, &spec)?;

        // Every join the recorder was asked to make, whether or not a datagram
        // ever arrives on it: an archive that does not state its intent cannot
        // tell "the snapshot port was silent" from "nobody asked for it", and a
        // reader who cannot tell those apart reports a pass over a rule that
        // never ran.
        let roles_joined = bindings
            .iter()
            .map(|b| RoleJoin {
                role: b.role,
                group: b.group,
                port: b.port,
                // The name as configured, so the archive states what was asked
                // for, and the address the membership was actually joined on —
                // which is now known in both modes: socket mode is given it,
                // and device mode resolves it. `source` stays absent only for a
                // route-discovered join, where the address the kernel picked is
                // genuinely not something this build observes, and an
                // unobserved value must not become a written one.
                interface: feed.interface.clone(),
                source: match &stated {
                    Interface::Address(address) => Some(*address),
                    Interface::Device(_) => Some(membership_interface),
                    Interface::Discovered => None,
                },
            })
            .collect();

        // A directory per feed, always, even for the only feed a host records.
        // Two writers sharing one staging directory would each see the other's
        // open segment as an orphan under a name its own sequence has not
        // reached — and evict it. The subdivision also makes adding a second
        // feed a configuration change rather than a migration.
        let staging_dir = config.archive.staging_dir.join(&spec);
        let completed_dir = config.archive.completed_dir.join(&spec);

        Ok(Self {
            port_roles: bindings.iter().map(|b| b.role).collect(),
            expected_sources: feed.expected_sources.clone(),
            expected_channel_ids: feed.expected_channel_ids.clone(),
            membership_interface,
            device,
            archive: match arrangement {
                // Reachable only through `Plan::for_inline`, which the feature
                // gates — but the variant exists in every build now, because a
                // build without the feature constructs it to refuse it, so this
                // branch is written in every build too.
                Arrangement::Inline => None,
                Arrangement::Archive => Some(ArchiveWriterConfig {
                    staging_dir,
                    completed_dir,
                    rotate_bytes: config.archive.rotate_bytes,
                    rotate_interval: config.archive.rotate_interval,
                    staging_max,
                    compression: compression(config.archive.compression)?,
                    identity: identity.clone(),
                    feed: spec.clone(),
                    roles_joined,
                    link_headers: link_headers(config.capture.mode),
                    capture_drop_scope: drop_scope(config.capture.mode),
                }),
            },
            bindings,
            spec,
        })
    }
}

/// A validated configuration, with everything the wiring needs already decided.
#[derive(Debug, Clone)]
pub struct Plan {
    pub config: RecorderConfig,
    pub identity: RecorderIdentity,
    pub feeds: Vec<FeedPlan>,
    pub mode: CaptureMode,
    pub listen_addr: SocketAddr,
    /// `SO_RCVBUF` in socket mode, the ring in AF_PACKET mode.
    pub buffer_bytes: u64,
    /// The capture length: the mandated cap plus its link headers, computed and
    /// never configured.
    pub snaplen: usize,
    pub rotate_interval: Duration,
    /// The host's staging budget divided between the feeds that share the disk.
    /// Zero in inline mode, which has no staging disk to divide.
    pub staging_max_per_feed: u64,
    pub arrangement: Arrangement,
}

impl Plan {
    /// Every refusal, in the order an operator would read the file.
    ///
    /// Archive mode: the directories are required, the staging budget is
    /// divided, and every feed is wired with a writer configuration.
    ///
    /// Reached when [`Arrangement::selected_by`] answered
    /// [`Arrangement::Archive`], which is to say when this configuration states
    /// one of the two directories. Stating one and not the other reaches here
    /// too, and is refused by key: the arrangement was stated and the archive
    /// was not finished.
    pub fn from_config(config: &RecorderConfig) -> Result<Self, StartupError> {
        check_identity(config)?;
        let feeds = check_feeds_are_named(config)?;

        if config.archive.staging_dir.as_os_str().is_empty() {
            return Err(StartupError::DirectoryNotStated {
                key: "staging_dir",
                why: "State where the open segment is written: it is the buffer an object-storage \
                      outage is absorbed by, and a recorder host is sized for it.",
            });
        }
        if config.archive.completed_dir.as_os_str().is_empty() {
            return Err(StartupError::DirectoryNotStated {
                key: "completed_dir",
                why:
                    "State the directory the shipper reads: it is this recorder's whole interface \
                      to whatever moves objects off the host.",
            });
        }
        if same_directory(&config.archive.staging_dir, &config.archive.completed_dir) {
            return Err(StartupError::DirectoriesAreTheSame);
        }

        // Refused before the per-feed loop, so that an operator who wrote
        // "gzip" is told about "gzip" rather than about the first feed.
        compression(config.archive.compression)?;
        check_mode_is_compiled_in(config.capture.mode)?;

        let staging_max_per_feed = config.archive.staging_max / feeds as u64;
        if staging_max_per_feed < config.archive.rotate_bytes {
            return Err(StartupError::StagingBudgetTooSmall {
                staging_max: config.archive.staging_max,
                feeds: plural(feeds, "feed"),
                per_feed: staging_max_per_feed,
                rotate_bytes: config.archive.rotate_bytes,
            });
        }

        let identity = identity_of(config);
        let feeds = config
            .feed
            .iter()
            .enumerate()
            .map(|(index, feed)| {
                FeedPlan::new(
                    feed,
                    index,
                    config,
                    &identity,
                    staging_max_per_feed,
                    Arrangement::Archive,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            mode: config.capture.mode,
            listen_addr: config.metrics.listen_addr,
            buffer_bytes: config.capture.buffer,
            snaplen: config.capture.snaplen(),
            rotate_interval: config.archive.rotate_interval,
            staging_max_per_feed,
            config: config.clone(),
            identity,
            feeds,
            arrangement: Arrangement::Archive,
        })
    }

    /// The same plan for a host that keeps no datagram.
    ///
    /// **Every feed check is the same one**, and that is the point of it being
    /// here rather than a second function somewhere else: the group must still
    /// be multicast, the port roles must still be distinct, the interface must
    /// still resolve, and the capture mode must still be compiled in. Inline
    /// mode joins exactly what archive mode joins and reads exactly what it
    /// reads; what it does with the datagrams afterwards is the only difference,
    /// and a plan that skipped these would let a misconfigured feed through on
    /// the mode that gives an operator the least evidence to diagnose it with.
    ///
    /// What is *not* checked is the archive: no directories, no compression, no
    /// staging budget. Those keys are refused outright by the inline
    /// configuration — a host believed to be keeping bytes it never kept is the
    /// failure that refusal exists for — so there is nothing here to validate.
    #[cfg(feature = "inline")]
    pub fn for_inline(config: &RecorderConfig) -> Result<Self, StartupError> {
        check_identity(config)?;
        check_feeds_are_named(config)?;
        check_mode_is_compiled_in(config.capture.mode)?;

        let identity = identity_of(config);
        let feeds = config
            .feed
            .iter()
            .enumerate()
            .map(|(index, feed)| {
                // No staging budget to divide: nothing is staged.
                FeedPlan::new(feed, index, config, &identity, 0, Arrangement::Inline)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            mode: config.capture.mode,
            listen_addr: config.metrics.listen_addr,
            buffer_bytes: config.capture.buffer,
            snaplen: config.capture.snaplen(),
            // The window bound is the inline configuration's, and this field is
            // the archive's rotation. Carried so the summary can stay one
            // function, and read by nothing in inline mode.
            rotate_interval: config.archive.rotate_interval,
            staging_max_per_feed: 0,
            config: config.clone(),
            identity,
            feeds,
            arrangement: Arrangement::Inline,
        })
    }

    /// Whether this arrangement writes objects.
    ///
    /// A method rather than a comparison at each site, so a third arrangement —
    /// if one is ever added — has one place to answer it.
    #[must_use]
    pub fn writes_an_archive(&self) -> bool {
        self.arrangement == Arrangement::Archive
    }

    /// What `--check` prints: enough for a deployment pipeline to see what this
    /// configuration would actually do, including the three things it derives
    /// rather than reads.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "site={} recorder={} env={}",
            self.identity.site, self.identity.recorder, self.identity.env
        );
        let _ = writeln!(
            out,
            "build version={} commit={}",
            self.identity.build_version, self.identity.build_commit
        );
        let _ = writeln!(out, "config hash={}", self.identity.config_hash);
        let _ = writeln!(
            out,
            "capture mode={} buffer={}B snaplen={}B link headers={} drop scope={}",
            mode_token(self.mode),
            self.buffer_bytes,
            self.snaplen,
            link_headers(self.mode).as_str(),
            drop_scope(self.mode).as_str(),
        );
        // Only where an archive is written. In inline mode these keys are
        // refused outright, and printing a rotation bound and a staging budget
        // of zero would have an operator reading `--check` for a problem that
        // is the absence of a thing this arrangement does not do.
        if self.writes_an_archive() {
            let _ = writeln!(
                out,
                "archive rotate={}B or {:?} compression=zstd staging budget={}B per feed",
                self.config.archive.rotate_bytes, self.rotate_interval, self.staging_max_per_feed,
            );
        }
        let _ = writeln!(out, "metrics listen_addr={}", self.listen_addr);
        let _ = writeln!(
            out,
            "health walk_messages={}",
            self.config.health.walk_messages
        );
        for feed in &self.feeds {
            let roles: Vec<String> = feed
                .bindings
                .iter()
                .map(|b| format!("{}={}", b.role.as_str(), b.port))
                .collect();
            let _ = write!(
                out,
                "feed {} group={} {} interface={}",
                feed.spec,
                feed.bindings
                    .first()
                    .map_or(Ipv4Addr::UNSPECIFIED, |b| b.group),
                roles.join(" "),
                feed.device
                    .as_deref()
                    .map_or_else(|| feed.membership_interface.to_string(), ToOwned::to_owned),
            );
            if let Some(archive) = &feed.archive {
                let _ = write!(
                    out,
                    " staging={} completed={}",
                    archive.staging_dir.display(),
                    archive.completed_dir.display(),
                );
            }
            let _ = writeln!(out);
        }
        out
    }
}

#[must_use]
pub const fn mode_token(mode: CaptureMode) -> &'static str {
    match mode {
        CaptureMode::Socket => "socket",
        CaptureMode::Afpacket => "afpacket",
    }
}

/// A build that cannot capture through the ring must not record through the
/// socket instead: the two archives answer different questions, and one
/// labelled as the other is the misattribution the whole design is arranged
/// against.
fn check_mode_is_compiled_in(mode: CaptureMode) -> Result<(), StartupError> {
    match mode {
        CaptureMode::Socket => Ok(()),
        #[cfg(feature = "afpacket")]
        CaptureMode::Afpacket => Ok(()),
        #[cfg(not(feature = "afpacket"))]
        CaptureMode::Afpacket => Err(StartupError::AfPacketNotCompiledIn),
    }
}

fn check_identity(config: &RecorderConfig) -> Result<(), StartupError> {
    for (key, value, why) in [
        (
            "site",
            &config.site,
            "It labels every dz_recorder_* series and partitions every object key, so two \
             recorders whose objects cannot be told apart are two recorders whose findings cannot \
             be either.",
        ),
        (
            "recorder",
            &config.recorder,
            "It is what makes a series and an object key unique within the site.",
        ),
        (
            "env",
            &config.env,
            "It partitions every object key, so without it a test recorder's objects land among \
             everything else's.",
        ),
    ] {
        if value.trim().is_empty() {
            return Err(StartupError::EmptyIdentity { key, why });
        }
    }
    Ok(())
}

/// The feed names, checked for the two properties an object key needs from
/// them: that each exists, and that no two are the same.
fn check_feeds_are_named(config: &RecorderConfig) -> Result<usize, StartupError> {
    if config.feed.is_empty() {
        return Err(StartupError::NoFeeds);
    }
    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, feed) in config.feed.iter().enumerate() {
        let spec = check_spec(feed, index)?;
        if seen.insert(feed.spec.as_str(), index).is_some() {
            return Err(StartupError::DuplicateFeedSpec { spec });
        }
    }
    Ok(config.feed.len())
}

fn check_spec(feed: &FeedConfig, index: usize) -> Result<String, StartupError> {
    if feed.spec.trim().is_empty() {
        return Err(StartupError::FeedSpecEmpty { index });
    }
    // A partition of an object key and a directory under the staging budget,
    // so a separator or a traversal in it is not a naming preference: it is a
    // recorder writing outside the directory whose size it is enforcing.
    let usable = feed.spec != "."
        && feed.spec != ".."
        && feed
            .spec
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    if !usable {
        return Err(StartupError::FeedSpecNotAName {
            spec: feed.spec.clone(),
        });
    }
    Ok(feed.spec.clone())
}

fn binding(
    feed: &FeedConfig,
    role: PortRole,
    port: u16,
    key: &'static str,
    spec: &str,
) -> Result<PortBinding, StartupError> {
    if port == 0 {
        return Err(StartupError::PortNotStated {
            spec: spec.to_owned(),
            key,
        });
    }
    Ok(PortBinding::new(role, feed.multicast_group, port))
}

/// What each mode can use from `feed.interface`, and what it refuses.
fn resolve_interface(
    stated: &Interface,
    mode: CaptureMode,
    spec: &str,
) -> Result<(Ipv4Addr, Option<String>), StartupError> {
    match (mode, stated) {
        (CaptureMode::Socket, Interface::Address(address)) => Ok((*address, None)),
        // The design's own meaning for the unset key, taken as stated rather
        // than as a value to invent: an operator who leaves it out has asked
        // for route discovery.
        (CaptureMode::Socket, Interface::Discovered) => Ok((Ipv4Addr::UNSPECIFIED, None)),
        (CaptureMode::Socket, Interface::Device(name)) => {
            Err(StartupError::InterfaceIsNotAnAddress {
                spec: spec.to_owned(),
                interface: name.clone(),
            })
        }
        // The ring is opened on the named device, which is where the archive's
        // bytes come from — and the membership that makes the network deliver
        // them is a socket join, which takes an address. Resolved here rather
        // than left unset: an unset address asks for route discovery, which
        // sends the IGMP report out of the default route, and the capture
        // crate's own contract says the report has to leave by the interface
        // the feed arrives on. On the topology this recorder exists for — a
        // feed arriving over a tunnel that is not the default route — a
        // discovered join never propagates, and a group that never joined is
        // silence that reads as a clean feed.
        //
        // A device carrying no address is refused by name here, where an
        // operator can act on it.
        (CaptureMode::Afpacket, Interface::Device(name)) => {
            let address =
                device_address(name).map_err(|source| StartupError::InterfaceAddress {
                    spec: spec.to_owned(),
                    interface: name.clone(),
                    source,
                })?;
            Ok((address, Some(name.clone())))
        }
        (CaptureMode::Afpacket, Interface::Address(address)) => {
            Err(StartupError::InterfaceIsNotADevice {
                spec: spec.to_owned(),
                interface: address.to_string(),
            })
        }
        (CaptureMode::Afpacket, Interface::Discovered) => Err(StartupError::InterfaceUnset {
            spec: spec.to_owned(),
        }),
    }
}

/// `1 feed`, `2 feeds`.
fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// Textual, not canonicalised: neither directory need exist yet, and the case
/// this is guarding against is one configuration naming one directory twice.
fn same_directory(first: &Path, second: &Path) -> bool {
    first == second
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// The writer configuration an archive-mode plan wires a feed with.
    ///
    /// `Option` since inline mode wires none, and these are archive-mode tests:
    /// a plan of theirs that produced `None` is a bug in the plan rather than a
    /// case to handle.
    fn archive_of(plan: &Plan, feed: usize) -> &ArchiveWriterConfig {
        plan.feeds[feed]
            .archive
            .as_ref()
            .expect("an archive-mode plan wires every feed with a writer configuration")
    }

    /// A configuration that every refusal below is a single edit away from.
    /// Documentation-range addresses only: this repository is public.
    pub const VALID: &str = r#"
site     = "site-a"
recorder = "recorder-1"
env      = "test"

[[feed]]
spec            = "top-of-book"
multicast_group = "233.252.0.1"
interface       = "192.0.2.7"
mktdata_port    = 41000
refdata_port    = 41001

[capture]
mode   = "socket"
buffer = "8MiB"

[archive]
staging_dir     = "/var/lib/dz-recorder/staging"
completed_dir   = "/var/lib/dz-recorder/completed"
rotate_bytes    = "16MiB"
rotate_interval = "60s"
compression     = "zstd"
staging_max     = "1GiB"

[metrics]
listen_addr = "127.0.0.1:0"
"#;

    pub fn valid_config() -> RecorderConfig {
        RecorderConfig::parse(VALID).expect("the fixture parses")
    }

    /// `VALID` with the two archive directories emptied: the shape inline mode
    /// runs on, and the shape that states no archive.
    fn config_stating_no_archive() -> RecorderConfig {
        let text = VALID
            .replace(
                r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
                r#"staging_dir     = """#,
            )
            .replace(
                r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
                r#"completed_dir   = """#,
            );
        RecorderConfig::parse(&text).expect("the fixture parses")
    }

    /// Every combination of the two predicates the selection reads, and the
    /// outcome each one is required to reach.
    ///
    /// **This is the test the other four cannot be.** Four cases asserted one
    /// at a time are four cases somebody can add a fifth to — a third
    /// arrangement, a defaulted `staging_dir` — and all four would still pass
    /// over a rule that had stopped being total. So the enumeration itself is
    /// the assertion: both predicates, all four combinations, each required to
    /// reach a named outcome, and nothing left over.
    ///
    /// The two that refuse are what make the two that run a reading rather than
    /// a guess, so they are asserted at the same altitude and in the same test
    /// rather than filed as error-path coverage.
    #[test]
    fn the_four_configuration_shapes_are_total_and_two_of_them_run() {
        let archive_stated = valid_config();
        let no_archive = config_stating_no_archive();
        let file = std::path::Path::new("inline.toml");

        // An archive stated and no file: archive mode, which is what every
        // host recording an archive today already states.
        assert_eq!(
            Arrangement::selected_by(&archive_stated, None).expect("an archive is stated"),
            Arrangement::Archive
        );

        // A file and no archive: inline mode.
        assert_eq!(
            Arrangement::selected_by(&no_archive, Some(file)).expect("the file is stated"),
            Arrangement::Inline
        );

        // Both: refused, with both statements in the message. Never resolved —
        // resolving it is starting a recorder in an arrangement its own
        // configuration contradicts.
        let message = Arrangement::selected_by(&archive_stated, Some(file))
            .expect_err("both arrangements were stated")
            .to_string();
        assert!(message.contains("archive.staging_dir"), "{message}");
        assert!(message.contains("inline.toml"), "{message}");

        // Neither: refused, naming both ways of stating one. This is the row
        // that leaves no silence for a default to be placed on.
        let message = Arrangement::selected_by(&no_archive, None)
            .expect_err("nothing states an arrangement")
            .to_string();
        assert!(message.contains("archive.staging_dir"), "{message}");
        assert!(message.contains("archive.completed_dir"), "{message}");
        assert!(message.contains("--inline-config"), "{message}");
    }

    /// Either directory states an archive, and a half-stated one is then
    /// refused by key rather than read as the other arrangement.
    ///
    /// The order matters and is asserted: an operator who wrote `completed_dir`
    /// alone is answered about `completed_dir`. Told about `staging_dir`
    /// instead, they would be reading a complaint about a key they never
    /// touched; told about a spool directory, they would be reading about a
    /// mode they never asked for.
    #[test]
    fn a_half_stated_archive_states_the_arrangement_and_is_refused_by_key() {
        for (key, removed) in [
            (
                "staging_dir",
                r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
            ),
            (
                "completed_dir",
                r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
            ),
        ] {
            let text = VALID.replace(removed, "");
            let config = RecorderConfig::parse(&text).expect("the fixture parses");
            assert_eq!(
                Arrangement::selected_by(&config, None)
                    .expect("one directory is enough to state an archive"),
                Arrangement::Archive,
                "a half-stated archive is a statement about the arrangement"
            );
            let message = Plan::from_config(&config)
                .expect_err("and an incomplete statement about the archive")
                .to_string();
            assert!(message.contains(key), "{message}");
        }

        // And the message an operator sees names the key they wrote, not the
        // other one.
        let text = VALID.replace(
            r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
            r#"staging_dir     = """#,
        );
        let config = RecorderConfig::parse(&text).expect("the fixture parses");
        let (key, _) = archive_directory_stated(&config).expect("completed_dir carries a value");
        assert_eq!(key, "completed_dir");
    }

    #[cfg(feature = "inline")]
    fn inline_plan_of(text: &str) -> Result<Plan, StartupError> {
        Plan::for_inline(&RecorderConfig::parse(text).expect("the fixture parses"))
    }

    /// An inline plan wires no writer configuration, and says so by absence.
    ///
    /// A set of directories built for a mode that never opens a writer is a set
    /// of directories an operator would reasonably expect to find objects in.
    #[cfg(feature = "inline")]
    #[test]
    fn an_inline_plan_wires_no_writer_configuration() {
        let plan = inline_plan_of(VALID).expect("the fixture is one inline mode can start on");
        assert_eq!(plan.arrangement, Arrangement::Inline);
        assert_eq!(plan.staging_max_per_feed, 0, "nothing is staged");
        for feed in &plan.feeds {
            assert!(
                feed.archive.is_none(),
                "feed {} was wired with a writer nothing will open",
                feed.spec
            );
        }
    }

    /// Inline mode needs no archive directories, and archive mode still does.
    ///
    /// The two are mutually exclusive by construction, which is the whole reason
    /// the plan has two constructors rather than a flag.
    #[cfg(feature = "inline")]
    #[test]
    fn an_inline_plan_needs_no_archive_directories() {
        let text = VALID
            .replace(
                r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
                r#"staging_dir     = """#,
            )
            .replace(
                r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
                r#"completed_dir   = """#,
            );
        assert!(
            inline_plan_of(&text).is_ok(),
            "inline mode writes no object, so it has no directory to require"
        );
        assert!(
            matches!(plan_of(&text), Err(StartupError::DirectoryNotStated { .. })),
            "archive mode still requires them"
        );
    }

    /// **Every feed check still runs.**
    ///
    /// Inline mode joins exactly what archive mode joins and reads exactly what
    /// it reads. Letting a misconfigured feed through here would let it through
    /// on the mode that gives an operator the least evidence to diagnose it
    /// with — there is no archive to go back to.
    #[cfg(feature = "inline")]
    #[test]
    fn an_inline_plan_refuses_every_feed_fault_an_archive_plan_refuses() {
        let not_multicast = VALID.replace(
            r#"multicast_group = "233.252.0.1""#,
            r#"multicast_group = "192.0.2.1""#,
        );
        assert!(
            matches!(
                inline_plan_of(&not_multicast),
                Err(StartupError::GroupNotMulticast { .. })
            ),
            "a group that is not multicast is refused in both arrangements"
        );

        let no_site = VALID.replace(r#"site     = "site-a""#, r#"site     = """#);
        assert!(
            inline_plan_of(&no_site).is_err(),
            "an unnamed host is refused in both arrangements"
        );
    }

    fn plan_of(text: &str) -> Result<Plan, StartupError> {
        Plan::from_config(&RecorderConfig::parse(text).expect("the fixture parses"))
    }

    fn refusal(text: &str) -> String {
        plan_of(text)
            .expect_err("this configuration must be refused")
            .to_string()
    }

    #[test]
    fn a_complete_configuration_is_accepted() {
        let plan = plan_of(VALID).expect("the fixture is a configuration a recorder can start on");
        assert_eq!(plan.feeds.len(), 1);
        assert_eq!(plan.feeds[0].spec, "top-of-book");
    }

    #[test]
    fn gzip_is_refused_by_name_rather_than_silently_written_as_zstd() {
        let text = VALID.replace(r#"compression     = "zstd""#, r#"compression     = "gzip""#);
        let message = refusal(&text);
        assert!(message.contains("archive.compression"), "{message}");
        assert!(message.contains("gzip"), "{message}");
        assert!(
            compression(Compression::Gzip).is_err(),
            "the mapping itself must refuse it, not only the plan"
        );
    }

    #[test]
    fn zstd_is_the_only_compression_that_maps_to_a_writer() {
        assert!(matches!(
            compression(Compression::Zstd),
            Ok(ArchiveCompression::Zstd { .. })
        ));
    }

    #[test]
    fn an_empty_staging_dir_is_refused() {
        let text = VALID.replace(
            r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
            r#"staging_dir     = """#,
        );
        let message = refusal(&text);
        assert!(message.contains("archive.staging_dir"), "{message}");
    }

    #[test]
    fn an_empty_completed_dir_is_refused() {
        let text = VALID.replace(
            r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
            r#"completed_dir   = """#,
        );
        let message = refusal(&text);
        assert!(message.contains("archive.completed_dir"), "{message}");
    }

    #[test]
    fn one_directory_named_twice_is_refused() {
        let text = VALID.replace(
            r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
            r#"completed_dir   = "/var/lib/dz-recorder/staging""#,
        );
        let message = refusal(&text);
        assert!(message.contains("same directory"), "{message}");
    }

    #[test]
    fn socket_mode_keeps_one_accumulator_per_port_role() {
        assert_eq!(
            drop_scope(CaptureMode::Socket),
            CaptureDropScope::PortRole,
            "socket mode holds one socket, and therefore one SO_RXQ_OVFL counter, per role"
        );
    }

    #[test]
    fn afpacket_mode_counts_its_drops_before_it_knows_the_role() {
        assert_eq!(
            drop_scope(CaptureMode::Afpacket),
            CaptureDropScope::CaptureHandle,
            "a ring drops frames before demultiplexing, so charging them to a role is a guess"
        );
    }

    #[test]
    fn the_drop_scope_reaches_the_archive_configuration() {
        let plan = plan_of(VALID).expect("a valid configuration");
        assert_eq!(
            archive_of(&plan, 0).capture_drop_scope,
            CaptureDropScope::PortRole
        );
        assert_eq!(archive_of(&plan, 0).link_headers, LinkHeaders::Synthesised);
    }

    #[test]
    fn socket_mode_synthesises_its_link_headers_and_afpacket_captures_them() {
        assert_eq!(link_headers(CaptureMode::Socket), LinkHeaders::Synthesised);
        assert_eq!(link_headers(CaptureMode::Afpacket), LinkHeaders::Captured);
    }

    #[test]
    fn a_configuration_with_no_feed_is_refused() {
        let text: String = VALID
            .lines()
            .filter(|line| {
                !(line.starts_with("[[feed]]")
                    || line.starts_with("spec")
                    || line.starts_with("multicast_group")
                    || line.starts_with("interface")
                    || line.starts_with("mktdata_port")
                    || line.starts_with("refdata_port"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let message = refusal(&text);
        assert!(message.contains("[[feed]]"), "{message}");
    }

    #[test]
    fn a_port_left_at_zero_is_refused_by_name() {
        let text = VALID.replace("mktdata_port    = 41000", "mktdata_port    = 0");
        let message = refusal(&text);
        assert!(message.contains("feed.mktdata_port"), "{message}");
        let text = VALID.replace("refdata_port    = 41001", "refdata_port    = 0");
        let message = refusal(&text);
        assert!(message.contains("feed.refdata_port"), "{message}");
    }

    #[test]
    fn a_snapshot_port_left_at_zero_is_refused_too() {
        let text = VALID.replace(
            "refdata_port    = 41001",
            "refdata_port    = 41001\nsnapshot_port   = 0",
        );
        let message = refusal(&text);
        assert!(message.contains("feed.snapshot_port"), "{message}");
    }

    #[test]
    fn a_group_that_is_not_multicast_is_refused() {
        let text = VALID.replace(
            r#"multicast_group = "233.252.0.1""#,
            r#"multicast_group = "192.0.2.1""#,
        );
        let message = refusal(&text);
        assert!(message.contains("feed.multicast_group"), "{message}");
    }

    #[test]
    fn an_empty_site_recorder_or_env_is_refused_by_name() {
        for (key, line, blanked) in [
            ("site", r#"site     = "site-a""#, r#"site     = """#),
            ("recorder", r#"recorder = "recorder-1""#, r#"recorder = """#),
            ("env", r#"env      = "test""#, r#"env      = """#),
        ] {
            let message = refusal(&VALID.replace(line, blanked));
            assert!(message.contains(key), "{key}: {message}");
        }
    }

    #[test]
    fn two_feeds_cannot_share_one_spec() {
        let text = format!(
            "{VALID}\n[[feed]]\nspec = \"top-of-book\"\nmulticast_group = \"233.252.0.2\"\n\
             mktdata_port = 41002\nrefdata_port = 41003\ninterface = \"192.0.2.7\"\n"
        );
        let message = refusal(&text);
        assert!(message.contains("share `spec`"), "{message}");
    }

    #[test]
    fn a_spec_that_is_not_a_name_is_refused() {
        let text = VALID.replace(
            r#"spec            = "top-of-book""#,
            r#"spec            = "../x""#,
        );
        let message = refusal(&text);
        assert!(message.contains("feed.spec"), "{message}");
    }

    #[test]
    fn each_feed_writes_into_its_own_directories() {
        let text = format!(
            "{VALID}\n[[feed]]\nspec = \"depth\"\nmulticast_group = \"233.252.0.2\"\n\
             mktdata_port = 41002\nrefdata_port = 41003\ninterface = \"192.0.2.7\"\n"
        );
        let plan = plan_of(&text).expect("two feeds on two groups is a valid configuration");
        assert_ne!(
            archive_of(&plan, 0).staging_dir,
            archive_of(&plan, 1).staging_dir
        );
        assert!(archive_of(&plan, 0).staging_dir.ends_with("top-of-book"));
        assert!(archive_of(&plan, 1).completed_dir.ends_with("depth"));
    }

    #[test]
    fn the_host_staging_budget_is_divided_between_the_feeds_sharing_the_disk() {
        let text = format!(
            "{VALID}\n[[feed]]\nspec = \"depth\"\nmulticast_group = \"233.252.0.2\"\n\
             mktdata_port = 41002\nrefdata_port = 41003\ninterface = \"192.0.2.7\"\n"
        );
        let plan = plan_of(&text).expect("two feeds is a valid configuration");
        assert_eq!(plan.staging_max_per_feed, 1024 * 1024 * 1024 / 2);
        assert_eq!(archive_of(&plan, 0).staging_max, plan.staging_max_per_feed);
        assert_eq!(archive_of(&plan, 1).staging_max, plan.staging_max_per_feed);
    }

    #[test]
    fn a_staging_budget_below_one_segment_is_refused() {
        let text = VALID.replace(r#"staging_max     = "1GiB""#, r#"staging_max     = "8MiB""#);
        let message = refusal(&text);
        assert!(message.contains("archive.staging_max"), "{message}");
        assert!(
            message.contains("evicted as soon as it was written"),
            "{message}"
        );
    }

    #[test]
    fn socket_mode_refuses_an_interface_name_rather_than_discovering_a_route() {
        let text = VALID.replace(
            r#"interface       = "192.0.2.7""#,
            r#"interface       = "eth0""#,
        );
        let message = refusal(&text);
        assert!(message.contains("feed.interface"), "{message}");
        assert!(message.contains("eth0"), "{message}");
    }

    #[test]
    fn socket_mode_takes_an_unset_interface_as_route_discovery() {
        let text = VALID.replace("interface       = \"192.0.2.7\"\n", "");
        let plan =
            plan_of(&text).expect("an unset interface is what route discovery is spelled as");
        assert_eq!(plan.feeds[0].membership_interface, Ipv4Addr::UNSPECIFIED);
        assert_eq!(plan.feeds[0].device, None);
    }

    #[test]
    fn socket_mode_joins_on_the_address_the_configuration_states() {
        let plan = plan_of(VALID).expect("a valid configuration");
        assert_eq!(
            plan.feeds[0].membership_interface,
            Ipv4Addr::new(192, 0, 2, 7)
        );
    }

    #[test]
    fn afpacket_mode_needs_a_device_and_refuses_an_address() {
        let spec = "top-of-book";
        // The device is resolved to the address the membership joins on, and
        // `lo` is the one name every host has. UNSPECIFIED here would be the
        // route-discovery join the capture crate's contract forbids: it leaves
        // by the default route, which on this recorder's topology is not the
        // interface the feed arrives on.
        assert!(matches!(
            resolve_interface(
                &Interface::Device("lo".to_owned()),
                CaptureMode::Afpacket,
                spec
            ),
            Ok((Ipv4Addr::LOCALHOST, Some(_)))
        ));
        // A name the host does not carry is refused at startup rather than
        // becoming a membership that never propagates.
        assert!(matches!(
            resolve_interface(
                &Interface::Device("dz-no-such-device0".to_owned()),
                CaptureMode::Afpacket,
                spec
            ),
            Err(StartupError::InterfaceAddress { .. })
        ));
        assert!(matches!(
            resolve_interface(
                &Interface::Address(Ipv4Addr::new(192, 0, 2, 7)),
                CaptureMode::Afpacket,
                spec
            ),
            Err(StartupError::InterfaceIsNotADevice { .. })
        ));
        assert!(matches!(
            resolve_interface(&Interface::Discovered, CaptureMode::Afpacket, spec),
            Err(StartupError::InterfaceUnset { .. })
        ));
    }

    #[test]
    fn an_interface_is_read_as_an_address_when_it_is_one_and_a_name_otherwise() {
        assert_eq!(Interface::parse(None), Interface::Discovered);
        assert_eq!(
            Interface::parse(Some("192.0.2.7")),
            Interface::Address(Ipv4Addr::new(192, 0, 2, 7))
        );
        assert_eq!(
            Interface::parse(Some("eth0")),
            Interface::Device("eth0".to_owned())
        );
    }

    #[cfg(not(feature = "afpacket"))]
    #[test]
    fn a_build_without_the_ring_refuses_to_record_through_the_socket_instead() {
        let text = VALID.replace(r#"mode   = "socket""#, r#"mode   = "afpacket""#);
        let message = refusal(&text);
        assert!(message.contains("capture.mode"), "{message}");
        assert!(message.contains("afpacket"), "{message}");
    }

    #[cfg(feature = "afpacket")]
    #[test]
    fn a_build_with_the_ring_wires_the_capture_handle_scope() {
        // `lo` rather than a plausible-looking `eth0`: the device is now
        // resolved to the address its membership joins on, so the name has to
        // be one every host actually carries or the test asserts the
        // environment it happens to run in.
        let text = VALID
            .replace(r#"mode   = "socket""#, r#"mode   = "afpacket""#)
            .replace(
                r#"interface       = "192.0.2.7""#,
                r#"interface       = "lo""#,
            );
        let plan = plan_of(&text).expect("afpacket on a named device is a valid configuration");
        assert_eq!(
            archive_of(&plan, 0).capture_drop_scope,
            CaptureDropScope::CaptureHandle
        );
        assert_eq!(archive_of(&plan, 0).link_headers, LinkHeaders::Captured);
        assert_eq!(plan.feeds[0].device.as_deref(), Some("lo"));
        assert_eq!(
            plan.feeds[0].membership_interface,
            Ipv4Addr::LOCALHOST,
            "the membership joins on the device's own address, never by route discovery"
        );
        // And the archive says so. An address the recorder observed and did not
        // write is provenance thrown away: a reader asking which interface a
        // segment arrived on would have to infer it from the device name.
        assert_eq!(
            archive_of(&plan, 0).roles_joined[0].source,
            Some(Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn the_summary_states_the_mode_the_scope_and_the_provenance() {
        let plan = plan_of(VALID).expect("a valid configuration");
        let summary = plan.summary();
        assert!(summary.contains("drop scope=port-role"), "{summary}");
        assert!(summary.contains("link headers=synthesised"), "{summary}");
        assert!(summary.contains(&plan.identity.config_hash), "{summary}");
        assert!(summary.contains("top-of-book"), "{summary}");
    }
}
