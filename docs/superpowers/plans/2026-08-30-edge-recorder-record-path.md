# Edge Recorder: the Record Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land `dz-recorder-core`, `dz-recorder-archive`, `dz-recorder-replay` and `dz-recorder-capture`, so any host receiving any feed in the family can archive the bytes it received — with its own losses recorded inside the archive — and replay them back byte-identically.

**Architecture:** One `Source` trait with two implementations (live capture, archive replay) and one `Sink` (the pcapng writer). The record path decodes nothing: it links `dz-edge-core` for `PortRole` and `MAX_DATAGRAM_SIZE` and for nothing else. Capture and archive never share a thread with each other's slow parts — rotation, compression and hashing run off the drain thread, and the drain thread never blocks on the writer.

**Tech Stack:** Rust 2021. `pcap-file` 2.0 (pcapng blocks), `zstd` 0.13, `sha2`, `nix` (socket mode), `pcap` (AF_PACKET mode, libpcap FFI), `serde`/`toml` (config), `thiserror`. No async runtime in the record path.

**Spec:** `docs/superpowers/specs/2026-08-28-edge-recorder-crates-design.md`

**Scope:** This is plan 1 of three, covering the spec's *Order of work* steps 1–3.

| Plan | Steps | What lands | Delivers |
|---|---|---|---|
| **1 — the record path** (this one) | 1–3 | `dz-recorder-core`, `-archive`, `-replay`, `-capture`; round-trip, fault, loss-attribution and backpressure tests | an archive of what actually arrived, with the recorder's own losses recorded inside it, replayable byte-identically |
| **2 — health and rollout** | 4–6 | `DatagramHeader::peek` in `dz-edge-core`; `dz-recorder-health` and `dz_recorder_*`; the `dz-recorder` binary; object layout, manifest and index table; the rollout | a recorder that is operable: minutes-scale alerting on the feed and on itself, and an archive whose coverage is answerable without opening an object |
| **3 — the analysis tier** | 7–9 | `dz-edge-mbp`; the subscriber state machine, book and fingerprint; replay into conformance; the per-datagram and per-message row loaders; the cross-site join | conformance findings and rows over replayed bytes, and publisher loss isolated by the cross-site join |

**Plans 1 and 2 are sized to reach plan 3, not to stand alone.** The record path exists to feed a replayable archive; the archive exists so that every per-message row and every conformance finding can be produced offline, from bytes, by something that never decodes in the receive path. Read plan 1's interfaces as chosen for that end state rather than for their own tasks.

Nothing in plan 1 runs on a host, and every test in it runs in CI with no privileges and no network. That is the reason the order is this way and not the other.

---

## Global Constraints

- **Vocabulary:** `GLOSSARY.md` in `edge-feed-spec` governs every identifier, comment, test name and commit message. `datagram` never `frame`; `era` never `epoch`; `port role` with the tokens `mktdata`/`refdata`/`snapshot`; `channel` only for the `Channel ID` shard.
- **No venue names.** This repository is public. No commit message, comment, test name, fixture or config example in this plan names a venue, a venue repository, a venue crate, an issue tracker, or gives a count of publishers or of recorder sites.
- **`PortRole::as_str()` is `"mktdata"`.** The spec's token, with no alias and no synonym anywhere: not in a metric label, a filename, a pcapng interface name, or a column. `"marketdata"` is not a spelling this code knows. A port role with two spellings is a join that silently returns nothing.
- **The record path never parses.** `dz-edge-core` is linked for `PortRole` and `MAX_DATAGRAM_SIZE`. No `Datagram::decode`, no `DatagramHeader::decode`, no message walk anywhere in these four crates. A datagram whose bytes we cannot explain is still archived.
- **The record path never blocks.** Neither a full staging directory, nor a slow disk, nor a stalled compressor may stall the drain thread. Every path that could is a counter and a drop instead.
- **Lints:** every crate carries `#![forbid(unsafe_code)]` and the workspace clippy set already used by `dz-edge-core`.
- **Endianness:** the archive is written little-endian (pcapng's byte-order magic states it); the payload bytes are copied verbatim and never byte-swapped.

---

## The pieces the record path has to get right

Every row below is a place where the obvious implementation is the wrong one, so each is stated before the tasks rather than discovered inside them.

| Piece | Why it is not obvious |
|---|---|
| `SO_TIMESTAMPNS` + `recvmsg`, carrying a kernel-vs-fallback stamp kind | a latency computed from an application stamp measures our own scheduler; an archive that cannot say which kind it holds cannot be trusted for latency at all |
| One dedicated blocking drain thread per socket, short read timeout so it observes a stop flag | the drain thread must do nothing but drain, and must still be able to stop promptly |
| Multicast bind/join with an explicit interface address, and the `ENODEV`-during-reprovision case | a join against an interface that is not there yet has to retry; propagating the error ends the source before any drain thread exists, so nothing retries |
| Rejoin on silence: a stranded membership that reports nothing, replaced on a `stale_after` cadence | a membership goes away with the interface it was joined on and nothing reports it — the socket stays open, readable, and permanently silent |
| Bounded per-source state with least-recently-seen eviction, keyed on the channel instance | an any-source join accepts datagrams from any sender, so the key space is not ours to trust |
| `SO_RXQ_OVFL` and its per-datagram delta | the whole loss-attribution story: without it every gap we caused is charged to the publisher |
| `IP_RECVTTL` / `IP_PKTINFO` for socket-mode header synthesis | a synthesised field has to be recorded as synthesised, and *not observed* must never be written as zero |
| AF_PACKET capture | the default mode, because it records what the network delivered rather than what one socket survived |
| Archive, replay, manifest, rotation, eviction | the format and its metadata are the substance of this plan; keeping bytes is the easy half |

---

## Findings that constrain the implementation

Four things were checked against the code rather than assumed. Each changes a task below.

**1. `DatagramHeader::decode` rejects what the health tier must count.** It returns `Err(UnsupportedSchema)` for an unknown schema version and `Err(DeclaredLengthOutOfRange)` for a declared length outside `24..=1232` — but the spec's health tier is required to count magic and schema version *by value rather than judged*, and to check the declared length *against* the cap. With today's API both of those datagrams are simply undecodable and the tier learns nothing. `dz-edge-core` needs a lenient `DatagramHeader::peek` that validates only the buffer length. **This is a plan-2 prerequisite, recorded here so it is not rediscovered.** The record path is unaffected: it does not decode.

**2. `pcap-file` 2.0.0 covers every pcapng element the spec asks for.** Verified in the vendored source: `SectionHeaderOption::{Hardware, OS, UserApplication, Comment}`, `InterfaceDescriptionOption::{IfName, IfDescription, IfTsResol}`, `EnhancedPacketOption::DropCount` (option code 4), and `InterfaceStatisticsOption::{IsbIfRecv, IsbIfDrop}`. Pin `2.0.0`; `3.0.0-rc1` is the only newer release and it is a release candidate.

**3. `nix` covers socket mode completely and AF_PACKET not at all.** `sockopt::{ReceiveTimestampns, RxqOvfl, Ipv4PacketInfo, Ipv4RecvTtl}` and `ControlMessageOwned::{ScmTimestampns, RxqOvfl, Ipv4PacketInfo, Ipv4Ttl}` all exist — note that the *option* for `IP_RECVTTL` is `Ipv4RecvTtl` and that `sockopt::Ipv4Ttl` is `IP_TTL`, the outgoing TTL, which is not what a receiver wants — socket mode needs no `unsafe` and no C dependency. But `nix` exposes neither `PACKET_STATISTICS` nor `SO_ATTACH_FILTER`, which AF_PACKET mode needs for its drop counter and its BPF filter. Doing it by hand means `libc::getsockopt` and `unsafe` in `dz-recorder-capture`.

**4. Therefore AF_PACKET goes through the `pcap` crate, and socket mode is built first.** libpcap gives the mmap ring, BPF filter compilation, and `stats()` (received / dropped / if_dropped) — the last of which is exactly `epb_dropcount` and the ISB pair — while keeping the `unsafe` inside the FFI crate so `#![forbid(unsafe_code)]` survives. The cost is a build-time `libpcap-dev`, which is **not installed on this workstation** (only the `libpcap0.8t64` runtime is), so the AF_PACKET task carries an install step and a CI package line. Socket mode is built first because it needs no `CAP_NET_RAW` and therefore runs in CI, which is what makes the round-trip test a gate rather than a manual ritual.

---

### Task 1: `dz-recorder-core` — the types every other crate speaks

**Files:**
- Modify: `rust/Cargo.toml` (add the four members and the shared dependency versions)
- Create: `rust/recorder/dz-recorder-core/Cargo.toml`
- Create: `rust/recorder/dz-recorder-core/src/lib.rs`
- Create: `rust/recorder/dz-recorder-core/src/datagram.rs`
- Create: `rust/recorder/dz-recorder-core/src/traits.rs`
- Create: `rust/recorder/dz-recorder-core/src/error.rs`
- Test: `rust/recorder/dz-recorder-core/tests/recorded_datagram.rs`

**Interfaces:**
- Consumes: `dz_edge_core::PortRole`.
- Produces: `RecordedDatagram<'a>`, `RecvTsKind`, `ChannelInstance`, `Source`, `Sink`, `Observer`, `CompletedSegment`, `SourceError`, `SinkError`.

- [ ] **Step 1: Write the failing test**

```rust
// rust/recorder/dz-recorder-core/tests/recorded_datagram.rs
use dz_recorder_core::{ChannelInstance, RecordedDatagram, RecvTsKind};
use dz_edge_core::PortRole;
use std::net::SocketAddrV4;

#[test]
fn a_kernel_stamp_is_distinguishable_from_a_fallback() {
    // A latency computed from an application-level stamp measures our own
    // scheduler. An archive that cannot say which kind it holds cannot be
    // trusted for latency at all, so the kind is carried, never inferred.
    assert_ne!(RecvTsKind::KernelSoftware, RecvTsKind::ApplicationFallback);
}

#[test]
fn the_channel_instance_is_source_channel_and_port() {
    // Two publishers may serve the same Channel ID to the same group and port,
    // each advancing its own sequence space. A key any coarser reads the
    // alternation as backward motion.
    let a: SocketAddrV4 = "10.0.0.1:0".parse().unwrap();
    let b: SocketAddrV4 = "10.0.0.2:0".parse().unwrap();
    assert_ne!(
        ChannelInstance::new(*a.ip(), 1, 40000),
        ChannelInstance::new(*b.ip(), 1, 40000),
    );
    assert_ne!(
        ChannelInstance::new(*a.ip(), 1, 40000),
        ChannelInstance::new(*a.ip(), 1, 40001),
    );
}

#[test]
fn a_recorded_datagram_borrows_its_payload() {
    let buf = [0u8; 64];
    let dg = RecordedDatagram {
        payload: &buf,
        src: "10.0.0.1:40000".parse().unwrap(),
        dst: "239.0.0.10:40000".parse().unwrap(),
        role: PortRole::Mktdata,
        recv_ts_ns: 1,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta: 0,
        ttl: Some(1),
    };
    assert_eq!(dg.payload.len(), 64);
    assert_eq!(dg.role.as_str(), "mktdata");
}
```

- [ ] **Step 2: Implement**

`RecordedDatagram` borrows its payload from the receive buffer — the record path must not allocate per datagram. `ttl` is `Option<u8>` because socket mode gets it only if `IP_RECVTTL` was honoured and AF_PACKET always has it; `None` means *not observed*, never *zero*.

```rust
// rust/recorder/dz-recorder-core/src/datagram.rs
/// How `recv_ts_ns` was obtained.
///
/// Carried rather than assumed: a stamp the kernel did not produce must not be
/// mistaken for one it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTsKind {
    /// `SO_TIMESTAMPNS`, or libpcap's nanosecond precision.
    KernelSoftware,
    /// The control message was absent and we stamped it ourselves.
    ApplicationFallback,
}

/// One received datagram and everything known about its arrival.
pub struct RecordedDatagram<'a> {
    pub payload: &'a [u8],
    pub src: SocketAddrV4,
    pub dst: SocketAddrV4,
    pub role: PortRole,
    pub recv_ts_ns: u64,
    pub recv_ts_kind: RecvTsKind,
    /// Datagrams the capture handle lost between the previous one and this one.
    pub drop_delta: u32,
    /// `None` when the capture mode did not observe it.
    pub ttl: Option<u8>,
}
```

`ChannelInstance` is `(Ipv4Addr, u8, u16)` — source address, `Channel ID`, destination port — with `Hash`, `Eq` and `Copy`. It lives here rather than in the health crate because the manifest keys on it too.

The three traits are exactly the spec's. `Sink::rotate` returns `Result<Option<CompletedSegment>, SinkError>`; `None` means the segment held nothing and no object was produced, which is not an error and must not be logged as one.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-core`, `cargo clippy -p dz-recorder-core --all-targets`.

---

### Task 2: `dz-recorder-core` — configuration and recorder identity

**Files:**
- Create: `rust/recorder/dz-recorder-core/src/config.rs`
- Create: `rust/recorder/dz-recorder-core/src/identity.rs`
- Test: `rust/recorder/dz-recorder-core/tests/config.rs`
- Test fixture: `rust/recorder/dz-recorder-core/tests/fixtures/recorder_example.toml`

**Interfaces:**
- Produces: `RecorderConfig`, `FeedConfig`, `CaptureConfig`, `ArchiveConfig`, `RecorderIdentity`, `config_hash()`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn an_unknown_key_is_a_load_failure() {
    // A misspelled section that parses cleanly and falls back to a default is
    // how a host runs the wrong transport while the operator believes otherwise.
    let err = RecorderConfig::parse("site='a'\nrecorder='b'\nenv='c'\nmodee='socket'\n")
        .unwrap_err();
    assert!(err.to_string().contains("modee"));
}

#[test]
fn no_key_can_raise_the_datagram_size_cap() {
    // The cap is mandated by every feed spec. Configuration cannot reach it.
    let toml = std::fs::read_to_string("tests/fixtures/recorder_example.toml").unwrap();
    assert!(!toml.contains("max_datagram"), "the cap is a constant, not a key");
    let cfg = RecorderConfig::parse(&toml).unwrap();
    assert_eq!(cfg.capture.snaplen(), dz_edge_core::MAX_DATAGRAM_SIZE + 42);
}

#[test]
fn an_unexpected_source_is_recorded_not_dropped() {
    // expected_sources gates counting and alerting, never the archive. A wrongly
    // recorded datagram is filterable afterwards; a wrongly dropped one is gone.
    let cfg = RecorderConfig::parse(&example()).unwrap();
    let feed = &cfg.feed[0];
    assert!(feed.expected_sources.is_empty() || feed.admits_every_source());
}

#[test]
fn the_config_hash_ignores_comments_and_key_order() {
    // The hash goes in the archive as provenance, so a finding is attributable
    // to a configuration. Reformatting the file must not invalidate that.
    let a = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n").unwrap();
    let b = RecorderConfig::parse("# note\nrecorder='r'\nenv='e'\nsite='s'\n").unwrap();
    assert_eq!(a.config_hash(), b.config_hash());
}
```

- [ ] **Step 2: Implement**

The TOML shape is the spec's, verbatim, with `#[serde(deny_unknown_fields)]` on every struct. `snaplen` is `MAX_DATAGRAM_SIZE + 42` (14 Ethernet + 20 IPv4 + 8 UDP), computed, never configured — the same discipline `DatagramBuilder` applies to the cap. `config_hash` is the sha256 of the *parsed* config serialised canonically, not of the file bytes, so a comment or a reordering does not invalidate provenance.

`RecorderIdentity` is `{ site, recorder, env, build_version, build_commit, config_hash }` and is what fills the Section Header block options.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-core`.

---

### Task 3: `dz-recorder-archive` — the pcapng segment writer

**Files:**
- Create: `rust/recorder/dz-recorder-archive/Cargo.toml`
- Create: `rust/recorder/dz-recorder-archive/src/lib.rs`
- Create: `rust/recorder/dz-recorder-archive/src/writer.rs`
- Test: `rust/recorder/dz-recorder-archive/tests/pcapng_blocks.rs`

**Interfaces:**
- Consumes: `RecordedDatagram`, `RecorderIdentity`, `pcap_file::pcapng`.
- Produces: `SegmentWriter`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn the_section_header_carries_the_recorder_identity() {
    // A self-describing archive cannot be separated from its provenance. An
    // archive copied, renamed or pulled out of a bucket by hand still knows
    // which recorder, which build and which configuration wrote it.
    let bytes = write_one_segment();
    let shb = first_section_header(&bytes);
    assert!(shb.options.iter().any(|o| matches!(o, SectionHeaderOption::Hardware(h) if h == "site/recorder")));
    assert!(shb.options.iter().any(|o| matches!(o, SectionHeaderOption::UserApplication(u) if u.starts_with("dz-recorder/"))));
}

#[test]
fn one_interface_description_block_per_port_role() {
    // The manifest states the intent. A port that was never joined produces no
    // data, and no data looks exactly like a clean feed.
    let idbs = interface_blocks(&write_three_roles());
    let names: Vec<_> = idbs.iter().map(if_name).collect();
    assert_eq!(names, ["mktdata", "refdata", "snapshot"]);
}

#[test]
fn timestamps_are_nanoseconds_not_microseconds() {
    // pcapng's default resolution is 10^-6. A recorder taking kernel nanosecond
    // stamps and writing them at microsecond resolution silently discards the
    // three digits the whole latency argument rests on.
    let idb = &interface_blocks(&write_one_segment())[0];
    assert!(idb.options.iter().any(|o| matches!(o, InterfaceDescriptionOption::IfTsResol(9))));
    let epb = &packet_blocks(&write_one_segment())[0];
    assert_eq!(epb.timestamp.as_nanos() as u64, KNOWN_RECV_TS_NS);
}

#[test]
fn the_drop_delta_travels_inside_the_archive() {
    let bytes = write_with_drop_deltas(&[0, 0, 7]);
    let epbs = packet_blocks(&bytes);
    assert!(epbs[2].options.iter().any(|o| matches!(o, EnhancedPacketOption::DropCount(7))));
    assert!(!epbs[0].options.iter().any(|o| matches!(o, EnhancedPacketOption::DropCount(_))),
        "a zero delta writes no option; every datagram carrying one is noise");
}

#[test]
fn a_datagram_is_written_whole_and_verbatim() {
    let payload: Vec<u8> = (0u8..=255).collect();
    let epb = &packet_blocks(&write_payload(&payload))[0];
    assert_eq!(&epb.data[42..], &payload[..], "no truncation, no byte swap");
    assert_eq!(epb.original_len as usize, 42 + payload.len());
}
```

- [ ] **Step 2: Implement**

One Section Header block per segment, one Interface Description block per port role in a fixed order (so `interface_id` is stable across segments and a reader can map it without options), one Enhanced Packet Block per datagram.

Ethernet + IPv4 + UDP headers are prepended to the payload. In AF_PACKET mode they are the captured bytes. In socket mode they are **synthesised**, and that fact is recorded in the Section Header comment so no reader mistakes a synthesised field for a captured one: the MAC addresses are zero, the IP identification and checksum are zero, and `ttl` is written only when `IP_RECVTTL` reported one.

`IfTsResol(9)` on every IDB. `DropCount` is written only when the delta is non-zero.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-archive`; open a written segment with `capinfos` and `tshark -r` and confirm both read it.

---

### Task 4: `dz-recorder-archive` — rotation, compression, hashing

**Files:**
- Create: `rust/recorder/dz-recorder-archive/src/rotate.rs`
- Create: `rust/recorder/dz-recorder-archive/src/compress.rs`
- Test: `rust/recorder/dz-recorder-archive/tests/rotation.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn rotation_fires_on_size_or_age_whichever_comes_first() {
    // A size bound keeps objects uniform for the analysis tier; an age bound
    // keeps a low-volume feed's data off a local disk for hours.
    let mut w = writer_with(rotate_bytes(1024), rotate_interval(secs(60)));
    write_bytes(&mut w, 2048);
    assert!(w.rotate_due(at_secs(1)));

    let mut w = writer_with(rotate_bytes(1 << 30), rotate_interval(secs(60)));
    write_bytes(&mut w, 8);
    assert!(w.rotate_due(at_secs(61)));
}

#[test]
fn an_empty_segment_rotates_to_nothing() {
    let mut w = writer_with(defaults());
    assert!(w.rotate(at_secs(61)).unwrap().is_none(), "no object, and no error either");
    assert_eq!(completed_dir_entries(), 0);
}

#[test]
fn the_segment_sequence_is_monotonic_and_gapless_within_a_run() {
    // A gap in the sequence of objects is a gap in the archive. Without it a
    // recorder that was down for an hour is indistinguishable from a feed that
    // was quiet for an hour.
    let seqs = rotate_n(5).iter().map(|s| s.segment_seq).collect::<Vec<_>>();
    assert_eq!(seqs, [0, 1, 2, 3, 4]);
}

#[test]
fn compression_never_runs_on_the_write_path() {
    // Rotation hands the file to a compressor thread and returns. A writer that
    // compresses inline stalls the drain thread for the length of a 256 MiB zstd.
    let elapsed = time_rotate_of_a_large_segment();
    assert!(elapsed < Duration::from_millis(50));
}

#[test]
fn the_hash_is_of_the_object_that_lands() {
    // Integrity and idempotent reprocessing key on (object key, sha256), so the
    // hash must cover the compressed bytes a consumer will actually fetch.
    let seg = rotate_one();
    let on_disk = std::fs::read(&seg.path).unwrap();
    assert_eq!(seg.sha256, sha256(&on_disk));
    assert!(seg.path.extension().unwrap() == "zst");
}
```

- [ ] **Step 2: Implement**

On rotation: fsync, close, move to a temp name in the staging directory, hand to the compressor thread. The compressor writes `<start_ns>-<end_ns>-<segment_seq>.pcapng.zst`, hashes what it wrote, writes the manifest beside it, then moves both into `completed_dir` — the move is last and it is the publication, so the shipper never sees a partial object.

The Interface Statistics blocks are appended before the close, from counters the writer already holds. `segment_seq` is monotonic per recorder run and starts at 0.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-archive`; `zstd -t` and `capinfos` on a completed object.

---

### Task 5: `dz-recorder-archive` — the manifest

**Files:**
- Create: `rust/recorder/dz-recorder-archive/src/manifest.rs`
- Test: `rust/recorder/dz-recorder-archive/tests/manifest.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn the_manifest_states_the_ports_the_recorder_was_asked_to_join() {
    // A port that was never joined produces no data, and no data looks exactly
    // like a clean feed. Without the intent, the analysis tier reports a pass
    // over rules it never ran.
    let m = manifest_of(a_run_that_joined_only_mktdata());
    assert_eq!(m.roles_joined, vec!["mktdata"]);
    assert!(!m.roles_joined.contains(&"snapshot".to_owned()),
        "so a silent snapshot port reports na, not pass");
}

#[test]
fn per_channel_instance_coverage_answers_without_opening_the_object() {
    let m = manifest_of(two_instances_on_one_channel());
    let cov = m.instances.get(&ChannelInstance::new(ip("10.0.0.1"), 1, 40000)).unwrap();
    assert_eq!((cov.first_seq, cov.last_seq, cov.count), (100, 199, 100));
    assert_eq!(cov.reset_counts_seen, vec![0]);
}

#[test]
fn drop_totals_are_visible_before_the_archive_is_trusted() {
    let m = manifest_of(a_run_that_dropped(9));
    assert_eq!(m.capture_drop_total, 9);
}
```

- [ ] **Step 2: Implement**

Computed from state the writer already holds, never by re-reading the segment.

**One deliberate exception to "the record path never parses":** the per-instance coverage needs `sequence_number`, `channel_id` and `reset_count`, which are three fixed offsets in the first 24 bytes. Read them as bare little-endian integers at their offsets — **not** via `DatagramHeader::decode`, which would reject an unknown schema version and thereby drop the coverage row for exactly the datagram most worth knowing about. The archive still holds the bytes either way; this only decides whether the manifest can describe them. Guard it with `payload.len() >= 24` and count a short datagram rather than skipping it silently.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-archive`.

---

### Task 6: `dz-recorder-archive` — the watermark, and the rule that matters most

**Files:**
- Create: `rust/recorder/dz-recorder-archive/src/staging.rs`
- Test: `rust/recorder/dz-recorder-archive/tests/backpressure.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_full_staging_directory_evicts_the_oldest_and_never_blocks() {
    // The single most important operational rule in the design. A writer that
    // blocks on a full disk stalls the drain thread, overflows the receive
    // queue, and converts an object-storage outage into a feed-loss incident
    // and into false publisher-loss findings in every archive written during it.
    let mut a = archive_with(staging_max(4 * SEGMENT));
    for _ in 0..6 { a.rotate_a_full_segment(); }
    assert_eq!(a.segments_on_disk(), 4);
    assert_eq!(a.segments_evicted_total(), 2);
    // Time what the write path actually does, and assert a per-datagram budget
    // against a *non-zero* total. A counter that only accumulates somewhere the
    // write path cannot reach is structurally incapable of being non-zero, so
    // asserting it is zero asserts nothing: the rule this test exists for would
    // still pass with the write path stalled for minutes.
    assert!(a.write_path_nanos() > 0, "the write path was actually timed");
    assert!(a.write_path_max_nanos() < 1_000_000, "no single write stalled");
}

#[test]
fn eviction_takes_the_oldest_and_never_the_open_segment() {
    let mut a = archive_with(staging_max(1 * SEGMENT));
    a.rotate_a_full_segment();
    let open = a.open_segment_path();
    a.rotate_a_full_segment();
    assert!(open.exists() || a.open_segment_path() != open);
    assert_eq!(a.oldest_segment_seq(), 1);
}

#[test]
fn a_write_error_drops_and_counts_rather_than_propagating_to_the_drain_thread() {
    let mut a = archive_with_read_only_staging();
    a.write(&a_datagram()).ok();
    assert_eq!(a.datagrams_dropped_total(), 1);
    assert!(a.last_error().is_some(), "counted and visible, not silent");
}
```

- [ ] **Step 2: Implement**

`staging_max` is enforced on rotation and on a periodic sweep, never on the write path. Eviction deletes the oldest completed-but-unshipped segment and its manifest together, and counts. The open segment is never a candidate.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-archive`.

---

### Task 7: `dz-recorder-replay` — the archive as a `Source`

**Files:**
- Create: `rust/recorder/dz-recorder-replay/Cargo.toml`
- Create: `rust/recorder/dz-recorder-replay/src/lib.rs`
- Test: `rust/recorder/dz-recorder-replay/tests/round_trip.rs`

- [ ] **Step 1: Write the failing test — this is the whole contract in one test**

```rust
#[test]
fn replay_yields_exactly_what_was_recorded() {
    // The Source symmetry is the load-bearing property of the design: a live
    // capture and a replayed archive present identically, so the analysis tier
    // runs unchanged over live traffic and the health tier runs unchanged over
    // an archive, and a recorder is testable end-to-end with no network.
    let original = synthetic_datagram_stream(1000);
    let path = record_to_archive(&original);
    let replayed: Vec<Owned> = ArchiveSource::open(&path).unwrap().collect();

    assert_eq!(replayed.len(), original.len());
    for (a, b) in original.iter().zip(&replayed) {
        assert_eq!(a.payload, b.payload);
        assert_eq!(a.src, b.src);
        assert_eq!(a.dst, b.dst);
        assert_eq!(a.role, b.role);
        assert_eq!(a.recv_ts_ns, b.recv_ts_ns, "nanosecond, not microsecond");
        assert_eq!(a.recv_ts_kind, b.recv_ts_kind);
        assert_eq!(a.drop_delta, b.drop_delta);
    }
}

#[test]
fn a_compressed_and_an_uncompressed_archive_replay_identically() {
    assert_eq!(replay(&write_plain(&s)), replay(&write_zstd(&s)));
}

#[test]
fn a_truncated_segment_replays_what_survived_and_says_so() {
    // A recorder killed mid-write leaves a partial block. Returning an error for
    // the whole file would discard every datagram before the tear.
    let path = truncate_mid_block(&write_plain(&stream_of(100)));
    let mut src = ArchiveSource::open(&path).unwrap();
    let n = (&mut src).count();
    assert!(n > 0 && n < 100);
    assert!(matches!(src.terminated_by(), Termination::Truncated));
}
```

- [ ] **Step 2: Implement**

Reads pcapng through `pcap-file`, transparently `zstd`-decoding by extension. Recovers `src`, `dst` and `ttl` from the IP/UDP headers in the block data; `role` from the Interface Description block the packet references; `drop_delta` from `DropCount` (absent means 0); `recv_ts_kind` from the Section Header comment written in Task 3.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-replay`.

---

### Task 8: `dz-recorder-capture` — socket mode

**Files:**
- Create: `rust/recorder/dz-recorder-capture/Cargo.toml`
- Create: `rust/recorder/dz-recorder-capture/src/lib.rs`
- Create: `rust/recorder/dz-recorder-capture/src/socket.rs`
- Create: `rust/recorder/dz-recorder-capture/src/rejoin.rs`
- Test: `rust/recorder/dz-recorder-capture/tests/socket_mode.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn the_first_datagram_on_a_handle_establishes_the_overflow_baseline() {
    // SO_RXQ_OVFL reports a running count, and both counters wrap. Reporting the
    // whole counter as a loss on the first datagram invents an outage.
    let mut t = OverflowTracker::new();
    assert_eq!(t.delta(1_000_000), 0);
    assert_eq!(t.delta(1_000_003), 3);
}

#[test]
fn the_overflow_delta_arithmetic_wraps() {
    let mut t = OverflowTracker::new();
    t.delta(u32::MAX - 1);
    assert_eq!(t.delta(2), 4);
}

#[test]
fn a_missing_timestamp_control_message_falls_back_and_says_so() {
    let dg = receive_without_scm_timestamp();
    assert_eq!(dg.recv_ts_kind, RecvTsKind::ApplicationFallback);
    assert!(dg.recv_ts_ns > 0);
}

#[test]
fn a_stranded_membership_is_rejoined_on_the_stale_cadence() {
    // A membership goes away with the interface it was joined on and nothing
    // reports it: the socket stays open, readable, and permanently silent.
    let mut r = Rejoiner::new(stale_after(secs(30)));
    assert!(!r.should_rejoin(silent_for(secs(29))));
    assert!(r.should_rejoin(silent_for(secs(31))));
}

#[test]
fn a_bind_that_fails_during_a_reprovision_retries_rather_than_ending_the_source() {
    // ENODEV from IP_ADD_MEMBERSHIP against an absent interface. Propagating it
    // ends the task before any drain thread exists, so nothing ever retries and
    // the source is dark until a human notices.
    let outcome = bind_or_retry(&absent_interface(), stale_after(secs(30)));
    assert!(matches!(outcome, Ok(None)));
    // With no cadence to retry on, failing loudly beats a thread that can only sleep.
    assert!(bind_or_retry(&absent_interface(), no_stale_cadence()).is_err());
}

#[test]
fn a_datagram_from_an_unexpected_source_is_delivered() {
    let src = SocketSource::with_expected_sources(&["10.0.0.1"]);
    let dg = src.receive_from("10.9.9.9").unwrap();
    assert_eq!(dg.src.ip().to_string(), "10.9.9.9", "gated for counting, never for the archive");
}
```

- [ ] **Step 2: Implement**

Port the drain thread, `bind_multicast`, `bind_or_retry` and the rejoin policy. Add `sockopt::RxqOvfl` and read `ControlMessageOwned::RxqOvfl` alongside `ScmTimestampns` in the same `recvmsg`; add `Ipv4Ttl` and `Ipv4PacketInfo` so the synthesised headers carry an observed TTL and the real destination address rather than a guess.

One drain thread per port role. Each pushes into a bounded channel the record loop drains; **if that channel is full the datagram is dropped and counted, and the drain thread does not wait** — the same rule as the staging watermark, applied one layer up.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-capture`; a loopback-interface integration test with `IP_MULTICAST_LOOP`, gated behind a feature so CI without multicast still passes.

---

### Task 9: `dz-recorder-capture` — AF_PACKET mode

**Files:**
- Create: `rust/recorder/dz-recorder-capture/src/afpacket.rs`
- Modify: `.github/workflows/*` (add `libpcap-dev` to the Rust job)
- Test: `rust/recorder/dz-recorder-capture/tests/afpacket_mode.rs`

**Build requirement:** `libpcap-dev`. Not present on this workstation — only the `libpcap0.8t64` runtime is — so `sudo apt-get install -y libpcap-dev` is step 0 of this task and a CI package line is part of it.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn the_filter_is_derived_from_the_configured_groups_and_ports() {
    // Without a filter the recorder archives every datagram on the interface.
    let f = bpf_filter_for(&feed_on("239.0.0.10", &[40000, 40001, 40002]));
    assert_eq!(f, "udp and dst host 239.0.0.10 and (dst port 40000 or dst port 40001 or dst port 40002)");
}

#[test]
fn nanosecond_precision_is_requested_and_verified_not_assumed() {
    // libpcap silently gives microseconds if the request is not honoured, and a
    // microsecond archive is indistinguishable from a nanosecond one that
    // happens to end in three zeros.
    let c = AfPacketSource::open(&iface()).unwrap();
    assert_eq!(c.precision(), Precision::Nano);
}

#[test]
fn ring_drops_become_the_per_datagram_delta() {
    let mut s = AfPacketSource::open(&iface()).unwrap();
    s.set_stats_for_test(Stat { received: 10, dropped: 4, if_dropped: 1 });
    assert_eq!(s.next().unwrap().unwrap().drop_delta, 4);
    assert_eq!(s.next().unwrap().unwrap().drop_delta, 0, "the delta, not the total");
}

#[test]
fn interface_drops_are_a_separate_category_from_ring_drops() {
    // "gap, no capture drops, interface drops rising" is loss upstream of the
    // capture point, and folding it into publisher loss is how a switch problem
    // becomes a publisher finding.
    let s = source_with(Stat { received: 10, dropped: 0, if_dropped: 6 });
    assert_eq!((s.capture_drops(), s.interface_drops()), (0, 6));
}

#[test]
fn a_non_ipv4_or_non_udp_frame_that_slips_the_filter_is_skipped_not_archived() {
    assert!(AfPacketSource::parse_frame(&an_arp_frame()).is_none());
}
```

- [ ] **Step 2: Implement**

`pcap::Capture::from_device(iface)` with `.immediate_mode(true)`, `.precision(Precision::Nano)`, `.buffer_size(cfg.capture.buffer)`, `.snaplen(snaplen)` and the compiled filter. `stats()` is polled per read batch, not per datagram, and the delta since the previous poll is attributed to the first datagram of the batch — which is what `epb_dropcount` means and is the best attribution the ring can offer.

The multicast socket is still opened and joined, and its receive path is drained and discarded: the socket exists for the IGMP membership and for nothing else, or the network has no reason to deliver the traffic to this host at all.

`src`, `dst`, `ttl` and the payload come from the captured Ethernet/IPv4/UDP headers directly — nothing is synthesised, which is the whole reason this mode is the default.

Keep the `libpcap` seam. The spec's *Capture frameworks considered* rejects an accelerated capture framework on the measured load, and the only reason that rejection is cheap to revisit is that such a framework ships a `libpcap` shim: swapping it in is a link-time and deployment change with this `Source` untouched. Reaching past `libpcap` to raw `AF_PACKET` for a few microseconds would close that door and buy nothing measurable.

- [ ] **Step 3: Verify** — `cargo test -p dz-recorder-capture --features afpacket`; a live run against a real interface needing `CAP_NET_RAW`, documented in the crate README as not a CI test.

---

### Task 10: end-to-end faults, and their counters

**Files:**
- Create: `rust/recorder/dz-recorder-replay/tests/faults.rs`
- Create: `rust/recorder/dz-recorder-replay/src/synthetic.rs` (the synthetic publisher, shared by every test above)

- [ ] **Step 1: Write the failing test**

```rust
// Each fault maps to exactly one counter, asserted, so a fault that moves no
// counter fails CI rather than waiting to be discovered in production.
#[test]
fn every_injected_fault_survives_the_round_trip_intact() {
    for fault in [
        Fault::SequenceGap,
        Fault::BackwardMotion,
        Fault::ResetCountAdvance,
        Fault::NewSourceAddress,
        Fault::SourceAddressDisappears,
        Fault::Duplicate,
        Fault::ReorderedPair,
        Fault::OversizedDeclaredLength,
        Fault::UnknownSchemaVersion,
        Fault::SilentChannel,
    ] {
        let original = synthetic_stream_with(fault);
        let replayed = replay(&record_to_archive(&original));
        assert_eq!(payloads(&original), payloads(&replayed), "{fault:?} was not archived verbatim");
    }
}

#[test]
fn an_unknown_schema_version_is_archived_and_appears_in_the_manifest() {
    // The bug class a parsing recorder creates is the worst one available: the
    // evidence needed to diagnose the bug is what the bug destroyed.
    let m = manifest_of(record_to_archive(&synthetic_stream_with(Fault::UnknownSchemaVersion)));
    assert_eq!(m.datagram_count, 100);
    assert_eq!(m.short_datagrams, 0);
}

#[test]
fn a_starved_recorder_accounts_for_its_own_gap_in_the_archive() {
    // A gap covered by our own overflow is not a finding. A gap not covered by
    // it is a much stronger one, because the obvious alternative explanation
    // has been excluded by evidence rather than by assumption.
    let path = record_with_a_paused_drain_thread(&contiguous_stream(1000));
    let dgs = replay(&path);
    let missing = missing_sequence_numbers(&dgs);
    let admitted: u64 = dgs.iter().map(|d| u64::from(d.drop_delta)).sum();
    assert!(missing > 0, "the starvation must actually have starved something");
    assert_eq!(missing, admitted, "every gap is attributed to us, none left to blame the publisher for");
}
```

- [ ] **Step 2: Implement** — the synthetic publisher emits a known datagram stream over loopback multicast, or straight into the `Sink` where no socket is wanted.

- [ ] **Step 3: Verify** — `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`.

---

### Task 11: cross-language check and documentation

- [ ] Write a pcapng segment with the Rust writer; read it with `tshark -r` and with a Go `gopacket/pcapgo` reader; assert the same datagram count, the same first and last `sequence_number`, and the same receive timestamps to the nanosecond. The archive is an interface between two languages and needs the same golden-vector discipline the codec has.
- [ ] `rust/recorder/README.md`: what each crate is for, the two capture modes and when each is right, the `libpcap-dev` build requirement, and a worked local run using the synthetic publisher and `IP_MULTICAST_LOOP` so the whole path is exercisable on one host with no network and no credentials.
- [ ] Add `recorder/` to the repository-layout table in `docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md`, which lists `codec/`, `publisher/`, `ingress/`, `conformance/` and `receivers/` and predates this design.

---

## What the next two plans hold

Written out here because plan 1's interfaces are chosen to serve them, not because they are leftovers.

### Plan 2 — health and rollout

- **`DatagramHeader::peek` in `dz-edge-core`** — a lenient header read that validates only the buffer length, so the health tier can count an unknown schema version and an out-of-range declared length by value instead of losing the datagram to `Err`. A change to a landed crate; it needs its own review, and it is the first task of plan 2 because everything else in the tier depends on it.
- `dz-recorder-health`: the header-only `Observer`, keyed on the channel instance, with least-recently-seen eviction, and the `dz_recorder_*` metric set built on `dz-publisher-metrics`' registry pattern. This is the step that makes the recorder's own capture loss visible — a fact you want before you rely on an archive, not after.
- `dz-recorder`: the binary that wires capture, archive and health together.
- The object layout, the manifest index table, and the shipper contract at `completed_dir`.
- The rollout: one host per change, each host proven before the next, with a rollback that does not depend on the new path being healthy.

### Plan 3 — the analysis tier

- **`dz-edge-mbp`** in `rust/codec/`, following the pattern `dz-edge-tob` established. It does not exist yet and every row below at depth grain needs it.
- **Three components at depth grain, none of them venue-specific:** the subscriber state machine (per-instrument sequence, stale snapshot anchors, count mismatches, re-bootstrap after a datagram gap), the book that folds absolute level updates rather than signed deltas, and the fingerprint that makes one book state comparable to another observation of the same book. These are the bulk of the per-flavour work, and all of it is offline.
- Replay into the conformance rule set, unmodified — with lossless replay the verifiability gate opens far more often, so `unverifiable` collapses toward `pass` or `violation`.
- The per-datagram and per-message row loaders, idempotent on `(object key, sha256)`, producing rows at top-of-book grain and at level grain.
- The cross-site join on `(channel instance, sequence number)` — the comparison that isolates publisher-attributable loss. The spec's *Why no venue comparison, and what takes its place* states why this is the comparison the recorder makes, and the one question it does not answer.

Plan 3 needs an archive and nothing else — including one written by plan 1's synthetic publisher — so it does not wait for plan 2's rollout.
