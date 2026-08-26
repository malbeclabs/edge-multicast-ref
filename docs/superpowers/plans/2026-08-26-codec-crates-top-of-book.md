# Codec Crates: Top-of-Book Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land `dz-edge-core`, `dz-edge-refdata` and `dz-edge-tob` with a golden-vector suite, so a publisher emitting the top-of-book feed and a subscriber decoding it share one implementation of the wire format.

**Architecture:** Three crates with no I/O and no async. `dz-edge-core` owns the datagram header, the message header, the datagram builder and the payloads every feed shares. `dz-edge-refdata` and `dz-edge-tob` own their feeds' payloads and depend on core. Every message implements one trait so the builder can pack any of them. Golden vectors are the cross-language contract, checked in CI.

**Tech Stack:** Rust 2021, `thiserror` only. No `serde`, no async runtime, no socket types.

**Spec:** `docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md`

**Scope:** This is plan 1 of several. It covers migration step 1 of the spec, for the top-of-book path only. `dz-edge-mbp`, `dz-edge-mbo` and `dz-edge-perp-stats` follow the pattern established here and get their own plan. The publisher, ingress and Go layers get their own plans after that.

## Global Constraints

- **Vocabulary:** `GLOSSARY.md` in `edge-feed-spec` governs every identifier, comment and commit message. `datagram` never `frame`; `era` never `epoch`; `port role` with the tokens `mktdata`/`refdata`/`snapshot`; `channel` only for the `Channel ID` shard; bare `source` never appears unqualified.
- **One exception, deliberate:** the wire field at datagram-header offset 22 is named **`Frame Length`** in the spec's own field table. Field names quoted from a spec table are proper nouns and are reproduced verbatim in doc comments. The Rust identifier is `datagram_len`. Do not "fix" the spec quotation, and do not name the identifier `frame_length`.
- **No venue names.** This repository is public. No commit message, comment, test name or fixture in this plan names a venue or a venue repository.
- **Schema Version:** encode `3` only. Decode `1` and `3`. Never decode `2`; no publisher emitted the 128-byte layout.
- **Maximum datagram size:** 1232 bytes, mandated by every feed spec. It is a constant, never a parameter an operator can raise.
- **Type ID `0x05` is reserved.** Do not implement a message there.
- **Lints:** every crate carries `#![forbid(unsafe_code)]`.
- **Endianness:** little-endian throughout.

---

### Task 1: `dz-edge-core` — workspace, constants, and the clamp

**Files:**
- Create: `rust/Cargo.toml`
- Create: `rust/codec/dz-edge-core/Cargo.toml`
- Create: `rust/codec/dz-edge-core/src/lib.rs`
- Create: `rust/codec/dz-edge-core/src/constants.rs`
- Test: `rust/codec/dz-edge-core/tests/constants.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `MAGIC_TOB: u16`, `SCHEMA_VERSION: u8`, `SCHEMA_VERSION_V1: u8`, `SUPPORTED_SCHEMA_VERSIONS: [u8; 2]`, `DATAGRAM_HEADER_SIZE: usize`, `MSG_HEADER_SIZE: usize`, `MAX_DATAGRAM_SIZE: usize`, and the `TYPE_*` / `SIZE_*` constants for the shared payloads.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-core/tests/constants.rs
use dz_edge_core as core;

#[test]
fn wire_constants_match_the_spec() {
    assert_eq!(core::MAGIC_TOB, 0x445A, "\"DZ\", top-of-book datagram delimiter");
    assert_eq!(core::SCHEMA_VERSION, 3, "publishers emit schema 3 only");
    assert_eq!(core::DATAGRAM_HEADER_SIZE, 24);
    assert_eq!(core::MSG_HEADER_SIZE, 4);
    assert_eq!(core::MAX_DATAGRAM_SIZE, 1232, "mandated for GRE headroom");
}

#[test]
fn schema_two_is_not_accepted() {
    // The 128-byte InstrumentDefinition was superseded before any publisher
    // emitted it. Accepting it would invent a generation that never existed.
    assert_eq!(core::SUPPORTED_SCHEMA_VERSIONS, [3, 1]);
    assert!(!core::SUPPORTED_SCHEMA_VERSIONS.contains(&2));
}

#[test]
fn shared_payload_sizes_match_the_spec() {
    assert_eq!(core::SIZE_HEARTBEAT, 16);
    assert_eq!(core::SIZE_END_OF_SESSION, 12);
    assert_eq!(core::TYPE_HEARTBEAT, 0x01);
    assert_eq!(core::TYPE_END_OF_SESSION, 0x06);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-core --test constants`
Expected: FAIL — the crate does not exist yet.

- [ ] **Step 3: Create the workspace**

```toml
# rust/Cargo.toml
[workspace]
resolver = "2"
members = ["codec/dz-edge-core"]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.88.0"
license = "Apache-2.0"
authors = ["Malbec Labs"]

[workspace.dependencies]
thiserror = "1.0"
```

```toml
# rust/codec/dz-edge-core/Cargo.toml
[package]
name = "dz-edge-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
description = "Datagram header, message header and shared payloads for the DoubleZero Edge feed family"

[dependencies]
thiserror = { workspace = true }
```

- [ ] **Step 4: Write the constants**

```rust
// rust/codec/dz-edge-core/src/constants.rs
//! Wire constants, transcribed from the edge-feed-spec field tables.

/// Datagram delimiter for the top-of-book feed: "DZ", little-endian on the wire.
pub const MAGIC_TOB: u16 = 0x445A;

/// The schema generation this build emits. A publisher speaks one generation:
/// there is no reader asking it to downgrade, and a mixture would make the
/// version byte meaningless.
pub const SCHEMA_VERSION: u8 = 3;

/// The generation before the 3.0.0 cut: `Symbol` was `char[16]` and there was
/// no `Source ID`. Decode-only; nothing here emits it.
pub const SCHEMA_VERSION_V1: u8 = 1;

/// Generations a *reader* accepts, newest first.
///
/// There is no `2`. A 128-byte `InstrumentDefinition` carrying the widened
/// `Symbol` without `Source ID` was specified and superseded before any
/// publisher emitted it, so accepting it would invent a generation that never
/// reached the wire.
pub const SUPPORTED_SCHEMA_VERSIONS: [u8; 2] = [SCHEMA_VERSION, SCHEMA_VERSION_V1];

/// Datagram header size in bytes.
pub const DATAGRAM_HEADER_SIZE: usize = 24;

/// Application message header size in bytes.
pub const MSG_HEADER_SIZE: usize = 4;

/// Maximum datagram size in bytes.
///
/// **Mandated, not derived.** Every feed spec states 1,232 bytes "to leave room
/// for GRE encapsulation headers used by the DoubleZero network's last-mile
/// delivery". Do not recompute it from a path MTU; that is how it drifted, and
/// a publisher is in production today emitting 1448 because a config key was
/// allowed to say so.
pub const MAX_DATAGRAM_SIZE: usize = 1232;

// Shared message type IDs. `0x05` is reserved in every current spec and is
// deliberately absent.
pub const TYPE_HEARTBEAT: u8 = 0x01;
pub const TYPE_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const TYPE_END_OF_SESSION: u8 = 0x06;
pub const TYPE_MANIFEST_SUMMARY: u8 = 0x07;

// Shared payload sizes, including the 4-byte message header.
pub const SIZE_HEARTBEAT: usize = 16;
pub const SIZE_END_OF_SESSION: usize = 12;
pub const SIZE_MANIFEST_SUMMARY: usize = 24;

/// Message header flag bit 0: set on the snapshot port, cleared elsewhere.
pub const FLAG_SNAPSHOT: u16 = 0x0001;
```

```rust
// rust/codec/dz-edge-core/src/lib.rs
//! Shared wire primitives for the DoubleZero Edge feed family.
//!
//! Venue-agnostic, zero-I/O, zero-async. Every feed in the family uses the
//! 24-byte datagram header and 4-byte message header defined here.

#![forbid(unsafe_code)]

pub mod constants;

pub use constants::*;
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-core --test constants`
Expected: PASS, 3 tests.

- [ ] **Step 6: Commit**

```bash
git add rust/Cargo.toml rust/codec/dz-edge-core
git commit -m "codec: add dz-edge-core with the shared wire constants"
```

---

### Task 2: `dz-edge-core` — the message trait and `DecodeError`

**Files:**
- Create: `rust/codec/dz-edge-core/src/error.rs`
- Create: `rust/codec/dz-edge-core/src/message.rs`
- Modify: `rust/codec/dz-edge-core/src/lib.rs`
- Test: `rust/codec/dz-edge-core/tests/message_trait.rs`

**Interfaces:**
- Consumes: `constants::*` from Task 1.
- Produces: `pub trait AppMessage { const TYPE_ID: u8; const SIZE: usize; fn encode_into(&self, dst: &mut [u8]); }` and `pub enum DecodeError` with variants `ShortBuffer { need: usize, got: usize }`, `UnsupportedSchema(u8)`, `LengthMismatch { type_id: u8, declared: u8, expected: u8 }`, `BadTypeId(u8)`, `DatagramFull { attempted: usize, max: usize }`, `ReservedTypeId(u8)`.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-core/tests/message_trait.rs
use dz_edge_core::{AppMessage, DecodeError};

struct Fake;
impl AppMessage for Fake {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    fn encode_into(&self, dst: &mut [u8]) {
        assert_eq!(dst.len(), Self::SIZE);
        dst.fill(0xAB);
    }
}

#[test]
fn a_message_encodes_into_exactly_its_size() {
    let mut buf = [0u8; 16];
    Fake.encode_into(&mut buf);
    assert_eq!(buf, [0xAB; 16]);
}

#[test]
fn decode_errors_render_the_numbers_a_reader_needs() {
    let e = DecodeError::ShortBuffer { need: 60, got: 12 };
    assert_eq!(e.to_string(), "short buffer: need 60 bytes, got 12");

    let e = DecodeError::UnsupportedSchema(2);
    assert_eq!(e.to_string(), "unsupported schema version 2");

    let e = DecodeError::ReservedTypeId(0x05);
    assert_eq!(e.to_string(), "type id 0x05 is reserved and carries no message");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-core --test message_trait`
Expected: FAIL — `AppMessage` and `DecodeError` are not defined.

- [ ] **Step 3: Write the implementation**

```rust
// rust/codec/dz-edge-core/src/error.rs
/// Errors produced by the decoders in this crate family.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DecodeError {
    #[error("short buffer: need {need} bytes, got {got}")]
    ShortBuffer { need: usize, got: usize },

    #[error("unsupported schema version {0}")]
    UnsupportedSchema(u8),

    #[error("message length {declared} mismatches fixed size {expected} for type {type_id:#04x}")]
    LengthMismatch { type_id: u8, declared: u8, expected: u8 },

    #[error("unknown message type id {0:#04x}")]
    BadTypeId(u8),

    #[error("datagram builder full: {attempted} bytes would exceed max {max}")]
    DatagramFull { attempted: usize, max: usize },

    /// `0x05` is marked reserved in every current feed spec. Two publishers
    /// transmit a private message there; a decoder must not silently invent a
    /// meaning for it.
    #[error("type id {0:#04x} is reserved and carries no message")]
    ReservedTypeId(u8),
}
```

```rust
// rust/codec/dz-edge-core/src/message.rs
/// Implemented by every fixed-size application message in the feed family.
/// `DatagramBuilder` uses it to pack messages without knowing their types.
pub trait AppMessage {
    /// Message type ID byte.
    const TYPE_ID: u8;

    /// Fixed on-the-wire size in bytes, including the 4-byte message header.
    const SIZE: usize;

    /// Encode into `dst`, which MUST be exactly `SIZE` bytes.
    fn encode_into(&self, dst: &mut [u8]);
}
```

```rust
// rust/codec/dz-edge-core/src/lib.rs — replace the body
#![forbid(unsafe_code)]

pub mod constants;
pub mod error;
pub mod message;

pub use constants::*;
pub use error::DecodeError;
pub use message::AppMessage;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-core --test message_trait`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add rust/codec/dz-edge-core
git commit -m "codec: add the AppMessage trait and DecodeError"
```

---

### Task 3: `dz-edge-core` — the datagram header and its clamped builder

This is the task that fixes the oversize-datagram defect. The limit lives here, not in configuration.

**Files:**
- Create: `rust/codec/dz-edge-core/src/datagram.rs`
- Modify: `rust/codec/dz-edge-core/src/lib.rs`
- Test: `rust/codec/dz-edge-core/tests/datagram.rs`

**Interfaces:**
- Consumes: `AppMessage`, `DecodeError`, `constants::*`.
- Produces:
  - `pub struct DatagramHeader { pub magic: u16, pub schema_version: u8, pub channel_id: u8, pub sequence_number: u64, pub send_timestamp_ns: u64, pub msg_count: u8, pub reset_count: u8, pub datagram_len: u16 }`
  - `DatagramHeader::decode(buf: &[u8]) -> Result<DatagramHeader, DecodeError>`
  - `pub struct DatagramBuilder`
  - `DatagramBuilder::new(magic: u16, channel_id: u8, sequence_number: u64, send_timestamp_ns: u64, reset_count: u8, mtu: u16) -> DatagramBuilder`
  - `DatagramBuilder::push<M: AppMessage>(&mut self, msg: &M) -> Result<(), DecodeError>`
  - `DatagramBuilder::push_snapshot<M: AppMessage>(&mut self, msg: &M) -> Result<(), DecodeError>`
  - `DatagramBuilder::remaining(&self) -> usize`
  - `DatagramBuilder::is_empty(&self) -> bool`
  - `DatagramBuilder::finish(self) -> Vec<u8>`

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-core/tests/datagram.rs
use dz_edge_core::{AppMessage, DatagramBuilder, DatagramHeader, DecodeError};
use dz_edge_core::{DATAGRAM_HEADER_SIZE, MAGIC_TOB, MAX_DATAGRAM_SIZE, SCHEMA_VERSION};

struct Sixteen;
impl AppMessage for Sixteen {
    const TYPE_ID: u8 = 0x01;
    const SIZE: usize = 16;
    fn encode_into(&self, dst: &mut [u8]) {
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..].fill(0);
    }
}

#[test]
fn header_fields_land_at_their_spec_offsets() {
    let mut b = DatagramBuilder::new(MAGIC_TOB, 7, 42, 1_700_000_000_000_000_000, 3, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish();

    assert_eq!(&out[0..2], &MAGIC_TOB.to_le_bytes(), "offset 0: Magic");
    assert_eq!(out[2], SCHEMA_VERSION, "offset 2: Schema Version");
    assert_eq!(out[3], 7, "offset 3: Channel ID");
    assert_eq!(&out[4..12], &42u64.to_le_bytes(), "offset 4: Sequence Number");
    assert_eq!(
        &out[12..20],
        &1_700_000_000_000_000_000u64.to_le_bytes(),
        "offset 12: Send Timestamp"
    );
    assert_eq!(out[20], 1, "offset 20: Message Count");
    assert_eq!(out[21], 3, "offset 21: Reset Count");
    // The spec's field table names offset 22 `Frame Length`. The identifier is
    // datagram_len; the wire meaning is the total datagram length.
    assert_eq!(&out[22..24], &(DATAGRAM_HEADER_SIZE as u16 + 16).to_le_bytes());
    assert_eq!(out.len(), DATAGRAM_HEADER_SIZE + 16);
}

#[test]
fn an_mtu_above_the_mandated_cap_is_clamped() {
    // 1448 is the value a publisher is running in production today. The builder
    // must not honour it: the cap is mandated by the spec, so configuration
    // cannot raise it.
    let b = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1448);
    assert_eq!(
        b.remaining(),
        MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE,
        "capacity must clamp to the mandated maximum, not the requested MTU"
    );
}

#[test]
fn a_finished_datagram_never_exceeds_the_cap() {
    let mut b = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1448);
    while b.push(&Sixteen).is_ok() {}
    let out = b.finish();
    assert!(
        out.len() <= MAX_DATAGRAM_SIZE,
        "finished datagram {} exceeds {MAX_DATAGRAM_SIZE}",
        out.len()
    );
}

#[test]
fn a_message_that_does_not_fit_is_refused_rather_than_truncated() {
    let mut b = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1232);
    while b.push(&Sixteen).is_ok() {}
    assert!(matches!(b.push(&Sixteen), Err(DecodeError::DatagramFull { .. })));
}

#[test]
fn message_count_saturates_at_255() {
    // Message Count is a u8. A 256th message would wrap it to 0 and every
    // subscriber would mis-parse the datagram.
    let mut b = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 65535);
    let mut pushed = 0usize;
    while b.push(&Sixteen).is_ok() {
        pushed += 1;
    }
    assert!(pushed <= 255, "pushed {pushed} messages into a u8 count");
    let out = b.finish();
    assert_eq!(out[20] as usize, pushed);
}

#[test]
fn the_snapshot_flag_is_set_only_by_push_snapshot() {
    let mut plain = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1232);
    plain.push(&Sixteen).unwrap();
    let out = plain.finish();
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(flags & 0x0001, 0, "mktdata and refdata messages clear bit 0");

    let mut snap = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1232);
    snap.push_snapshot(&Sixteen).unwrap();
    let out = snap.finish();
    let flags = u16::from_le_bytes([out[DATAGRAM_HEADER_SIZE + 2], out[DATAGRAM_HEADER_SIZE + 3]]);
    assert_eq!(flags & 0x0001, 1, "snapshot-port messages set bit 0");
}

#[test]
fn decode_rejects_a_schema_version_it_does_not_implement() {
    let mut b = DatagramBuilder::new(MAGIC_TOB, 0, 0, 0, 0, 1232);
    b.push(&Sixteen).unwrap();
    let mut out = b.finish();
    out[2] = 2; // the generation that never reached the wire
    assert_eq!(
        DatagramHeader::decode(&out),
        Err(DecodeError::UnsupportedSchema(2))
    );
}

#[test]
fn decode_round_trips_a_built_datagram() {
    let mut b = DatagramBuilder::new(MAGIC_TOB, 9, 1234, 5678, 2, 1232);
    b.push(&Sixteen).unwrap();
    let out = b.finish();
    let h = DatagramHeader::decode(&out).unwrap();
    assert_eq!(h.channel_id, 9);
    assert_eq!(h.sequence_number, 1234);
    assert_eq!(h.send_timestamp_ns, 5678);
    assert_eq!(h.reset_count, 2);
    assert_eq!(h.msg_count, 1);
    assert_eq!(h.datagram_len as usize, out.len());
}

#[test]
fn decode_refuses_a_short_buffer() {
    assert_eq!(
        DatagramHeader::decode(&[0u8; 10]),
        Err(DecodeError::ShortBuffer { need: 24, got: 10 })
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-core --test datagram`
Expected: FAIL — `DatagramBuilder` and `DatagramHeader` are not defined.

- [ ] **Step 3: Write the implementation**

```rust
// rust/codec/dz-edge-core/src/datagram.rs
//! The 24-byte datagram header and the builder that packs messages into one
//! UDP payload.

use crate::constants::{
    DATAGRAM_HEADER_SIZE, FLAG_SNAPSHOT, MAX_DATAGRAM_SIZE, MSG_HEADER_SIZE, SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
};
use crate::error::DecodeError;
use crate::message::AppMessage;

/// The decoded datagram header.
///
/// Field names follow the spec's table. The field at offset 22 is named
/// `Frame Length` there; the identifier here is `datagram_len`, because the
/// glossary requires `datagram` for our own traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub magic: u16,
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub send_timestamp_ns: u64,
    pub msg_count: u8,
    pub reset_count: u8,
    pub datagram_len: u16,
}

impl DatagramHeader {
    /// Decode a header from the front of `buf`.
    ///
    /// Rejects any schema version this build does not implement, per the spec's
    /// "a subscriber MUST discard frames whose version it does not implement".
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < DATAGRAM_HEADER_SIZE {
            return Err(DecodeError::ShortBuffer {
                need: DATAGRAM_HEADER_SIZE,
                got: buf.len(),
            });
        }
        let schema_version = buf[2];
        if !SUPPORTED_SCHEMA_VERSIONS.contains(&schema_version) {
            return Err(DecodeError::UnsupportedSchema(schema_version));
        }
        Ok(Self {
            magic: u16::from_le_bytes([buf[0], buf[1]]),
            schema_version,
            channel_id: buf[3],
            sequence_number: u64::from_le_bytes(buf[4..12].try_into().unwrap_or_default()),
            send_timestamp_ns: u64::from_le_bytes(buf[12..20].try_into().unwrap_or_default()),
            msg_count: buf[20],
            reset_count: buf[21],
            datagram_len: u16::from_le_bytes([buf[22], buf[23]]),
        })
    }
}

/// Accumulates application messages into one datagram.
///
/// Capacity is `min(mtu, MAX_DATAGRAM_SIZE)`. The clamp is the point: the cap is
/// mandated by every feed spec, so no configuration key and no operator can
/// raise it. A publisher is in production today emitting 1448 because its cap
/// lived in configuration instead of here.
pub struct DatagramBuilder {
    buf: Vec<u8>,
    magic: u16,
    channel_id: u8,
    sequence_number: u64,
    send_timestamp_ns: u64,
    reset_count: u8,
    capacity: usize,
    msg_count: u8,
}

impl DatagramBuilder {
    #[must_use]
    pub fn new(
        magic: u16,
        channel_id: u8,
        sequence_number: u64,
        send_timestamp_ns: u64,
        reset_count: u8,
        mtu: u16,
    ) -> Self {
        let capacity = (mtu as usize).min(MAX_DATAGRAM_SIZE);
        let mut buf = Vec::with_capacity(capacity);
        buf.resize(DATAGRAM_HEADER_SIZE, 0);
        Self {
            buf,
            magic,
            channel_id,
            sequence_number,
            send_timestamp_ns,
            reset_count,
            capacity,
            msg_count: 0,
        }
    }

    /// Bytes still available for messages.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.buf.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.msg_count == 0
    }

    /// Append a message with the snapshot flag cleared.
    pub fn push<M: AppMessage>(&mut self, msg: &M) -> Result<(), DecodeError> {
        self.push_with_flags(msg, 0)
    }

    /// Append a message with the snapshot flag set. Correct only on the
    /// `snapshot` port role.
    pub fn push_snapshot<M: AppMessage>(&mut self, msg: &M) -> Result<(), DecodeError> {
        self.push_with_flags(msg, FLAG_SNAPSHOT)
    }

    fn push_with_flags<M: AppMessage>(&mut self, msg: &M, flags: u16) -> Result<(), DecodeError> {
        // Message Count is a u8; a 256th message would wrap it to 0 and every
        // subscriber would mis-parse the rest of the datagram.
        if self.msg_count == u8::MAX {
            return Err(DecodeError::DatagramFull {
                attempted: self.buf.len() + M::SIZE,
                max: self.capacity,
            });
        }
        if M::SIZE > self.remaining() {
            return Err(DecodeError::DatagramFull {
                attempted: self.buf.len() + M::SIZE,
                max: self.capacity,
            });
        }
        let start = self.buf.len();
        self.buf.resize(start + M::SIZE, 0);
        msg.encode_into(&mut self.buf[start..start + M::SIZE]);
        // The message writes its own type and length; the builder owns Flags so
        // a caller cannot set the snapshot bit on a mktdata message by accident.
        self.buf[start + 2..start + 4].copy_from_slice(&flags.to_le_bytes());
        debug_assert_eq!(self.buf[start], M::TYPE_ID);
        debug_assert_eq!(self.buf[start + 1] as usize, M::SIZE);
        debug_assert!(M::SIZE >= MSG_HEADER_SIZE);
        self.msg_count += 1;
        Ok(())
    }

    /// Stamp the header and return the datagram.
    #[must_use]
    pub fn finish(mut self) -> Vec<u8> {
        let len = self.buf.len() as u16;
        self.buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        self.buf[2] = SCHEMA_VERSION;
        self.buf[3] = self.channel_id;
        self.buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        self.buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        self.buf[20] = self.msg_count;
        self.buf[21] = self.reset_count;
        self.buf[22..24].copy_from_slice(&len.to_le_bytes());
        self.buf
    }
}
```

```rust
// rust/codec/dz-edge-core/src/lib.rs — replace the body
#![forbid(unsafe_code)]

pub mod constants;
pub mod datagram;
pub mod error;
pub mod message;

pub use constants::*;
pub use datagram::{DatagramBuilder, DatagramHeader};
pub use error::DecodeError;
pub use message::AppMessage;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-core --test datagram`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add rust/codec/dz-edge-core
git commit -m "codec: add the datagram header and its clamped builder

Capacity clamps to min(mtu, MAX_DATAGRAM_SIZE). The cap is mandated by every
feed spec, so it belongs here rather than in a configuration key a deployment
can raise."
```

---

### Task 4: `dz-edge-core` — Heartbeat and EndOfSession

**Files:**
- Create: `rust/codec/dz-edge-core/src/heartbeat.rs`
- Create: `rust/codec/dz-edge-core/src/end_of_session.rs`
- Modify: `rust/codec/dz-edge-core/src/lib.rs`
- Test: `rust/codec/dz-edge-core/tests/control_messages.rs`

**Interfaces:**
- Consumes: `AppMessage`, `DecodeError`, `constants::*`.
- Produces: `pub struct Heartbeat { pub channel_id: u8, pub timestamp_ns: u64 }` and `pub struct EndOfSession { pub timestamp_ns: u64 }`, both implementing `AppMessage`, each with `decode(body: &[u8]) -> Result<Self, DecodeError>` taking the full message including its header.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-core/tests/control_messages.rs
use dz_edge_core::{AppMessage, EndOfSession, Heartbeat};

#[test]
fn heartbeat_matches_its_spec_layout() {
    let hb = Heartbeat { channel_id: 7, timestamp_ns: 0x0102_0304_0506_0708 };
    let mut buf = [0u8; Heartbeat::SIZE];
    hb.encode_into(&mut buf);

    assert_eq!(buf.len(), 16);
    assert_eq!(buf[0], 0x01, "offset 0: Type");
    assert_eq!(buf[1], 16, "offset 1: Length");
    assert_eq!(buf[4], 7, "offset 4: Channel ID");
    assert_eq!(&buf[5..8], &[0, 0, 0], "offset 5: Reserved, 3 bytes");
    assert_eq!(&buf[8..16], &0x0102_0304_0506_0708u64.to_le_bytes(), "offset 8: Timestamp");

    assert_eq!(Heartbeat::decode(&buf).unwrap(), hb);
}

#[test]
fn end_of_session_matches_its_spec_layout() {
    let eos = EndOfSession { timestamp_ns: 99 };
    let mut buf = [0u8; EndOfSession::SIZE];
    eos.encode_into(&mut buf);

    assert_eq!(buf.len(), 12);
    assert_eq!(buf[0], 0x06, "offset 0: Type");
    assert_eq!(buf[1], 12, "offset 1: Length");
    assert_eq!(&buf[4..12], &99u64.to_le_bytes(), "offset 4: Timestamp");

    assert_eq!(EndOfSession::decode(&buf).unwrap(), eos);
}

#[test]
fn decode_rejects_a_declared_length_that_is_not_the_fixed_size() {
    let mut buf = [0u8; Heartbeat::SIZE];
    Heartbeat { channel_id: 0, timestamp_ns: 0 }.encode_into(&mut buf);
    buf[1] = 20; // lie about the length
    assert!(Heartbeat::decode(&buf).is_err());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-core --test control_messages`
Expected: FAIL — `Heartbeat` and `EndOfSession` are not defined.

- [ ] **Step 3: Write the implementation**

```rust
// rust/codec/dz-edge-core/src/heartbeat.rs
use crate::constants::{SIZE_HEARTBEAT, TYPE_HEARTBEAT};
use crate::error::DecodeError;
use crate::message::AppMessage;

/// `0x01 Heartbeat` (16 bytes). Sent on `mktdata` when there is no other
/// traffic, so a subscriber can tell a quiet channel from a dead one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// Redundant with the datagram header; useful for standalone logging.
    pub channel_id: u8,
    pub timestamp_ns: u64,
}

impl AppMessage for Heartbeat {
    const TYPE_ID: u8 = TYPE_HEARTBEAT;
    const SIZE: usize = SIZE_HEARTBEAT;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4] = self.channel_id;
        dst[5..8].fill(0);
        dst[8..16].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }
}

impl Heartbeat {
    /// Decode from a full message, including its 4-byte header.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            channel_id: buf[4],
            timestamp_ns: u64::from_le_bytes(buf[8..16].try_into().unwrap_or_default()),
        })
    }
}
```

```rust
// rust/codec/dz-edge-core/src/end_of_session.rs
use crate::constants::{SIZE_END_OF_SESSION, TYPE_END_OF_SESSION};
use crate::error::DecodeError;
use crate::message::AppMessage;

/// `0x06 EndOfSession` (12 bytes). No more data for this session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndOfSession {
    pub timestamp_ns: u64,
}

impl AppMessage for EndOfSession {
    const TYPE_ID: u8 = TYPE_END_OF_SESSION;
    const SIZE: usize = SIZE_END_OF_SESSION;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..12].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }
}

impl EndOfSession {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            timestamp_ns: u64::from_le_bytes(buf[4..12].try_into().unwrap_or_default()),
        })
    }
}
```

Add to `lib.rs`:

```rust
pub mod end_of_session;
pub mod heartbeat;

pub use end_of_session::EndOfSession;
pub use heartbeat::Heartbeat;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-core --test control_messages`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add rust/codec/dz-edge-core
git commit -m "codec: add Heartbeat and EndOfSession"
```

---

### Task 5: `dz-edge-tob` — Quote and Trade

**Files:**
- Create: `rust/codec/dz-edge-tob/Cargo.toml`
- Create: `rust/codec/dz-edge-tob/src/lib.rs`
- Create: `rust/codec/dz-edge-tob/src/quote.rs`
- Create: `rust/codec/dz-edge-tob/src/trade.rs`
- Modify: `rust/Cargo.toml` (add the member)
- Test: `rust/codec/dz-edge-tob/tests/wire_layout.rs`

**Interfaces:**
- Consumes: `dz_edge_core::{AppMessage, DecodeError}`.
- Produces:
  - `pub struct Quote { pub instrument_id: u32, pub source_id: u16, pub update_flags: u8, pub source_timestamp_ns: u64, pub bid_price: i64, pub bid_qty: u64, pub ask_price: i64, pub ask_qty: u64, pub bid_source_count: u16, pub ask_source_count: u16 }` with `SIZE = 60`, `TYPE_ID = 0x03`, and `Quote::decode`.
  - `pub struct Trade { pub instrument_id: u32, pub source_id: u16, pub aggressor_side: u8, pub trade_flags: u8, pub source_timestamp_ns: u64, pub trade_price: i64, pub trade_qty: u64, pub trade_id: u64, pub cumulative_volume: u64 }` with `SIZE = 52`, `TYPE_ID = 0x04`, and `Trade::decode`.
  - Flag constants `QUOTE_BID_UPDATED = 0x01`, `QUOTE_ASK_UPDATED = 0x02`, `QUOTE_BID_GONE = 0x04`, `QUOTE_ASK_GONE = 0x08`; `AGGRESSOR_UNKNOWN = 0`, `AGGRESSOR_BUY = 1`, `AGGRESSOR_SELL = 2`; `TRADE_FLAG_BLOCK = 0x01`, `TRADE_FLAG_SWEEP = 0x02`, `TRADE_FLAG_CROSS = 0x04`.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-tob/tests/wire_layout.rs
use dz_edge_core::AppMessage;
use dz_edge_tob::{Quote, Trade};

fn sample_quote() -> Quote {
    Quote {
        instrument_id: 0x1112_1314,
        source_id: 0x2122,
        update_flags: 0x03,
        source_timestamp_ns: 0x3132_3334_3536_3738,
        bid_price: -12_345,
        bid_qty: 6789,
        ask_price: 54_321,
        ask_qty: 9876,
        bid_source_count: 4,
        ask_source_count: 5,
    }
}

#[test]
fn quote_fields_land_at_their_spec_offsets() {
    let q = sample_quote();
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);

    assert_eq!(b.len(), 60);
    assert_eq!(b[0], 0x03, "offset 0: Type");
    assert_eq!(b[1], 60, "offset 1: Length");
    assert_eq!(&b[4..8], &0x1112_1314u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &0x2122u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], 0x03, "offset 10: Update Flags");
    assert_eq!(b[11], 0, "offset 11: Reserved");
    assert_eq!(&b[12..20], &0x3132_3334_3536_3738u64.to_le_bytes(), "offset 12: Source Timestamp");
    assert_eq!(&b[20..28], &(-12_345i64).to_le_bytes(), "offset 20: Bid Price");
    assert_eq!(&b[28..36], &6789u64.to_le_bytes(), "offset 28: Bid Quantity");
    assert_eq!(&b[36..44], &54_321i64.to_le_bytes(), "offset 36: Ask Price");
    assert_eq!(&b[44..52], &9876u64.to_le_bytes(), "offset 44: Ask Quantity");
    assert_eq!(&b[52..54], &4u16.to_le_bytes(), "offset 52: Bid Source Count");
    assert_eq!(&b[54..56], &5u16.to_le_bytes(), "offset 54: Ask Source Count");
    assert_eq!(&b[56..60], &[0, 0, 0, 0], "offset 56: Reserved, 4 bytes");
}

#[test]
fn quote_round_trips() {
    let q = sample_quote();
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);
    assert_eq!(Quote::decode(&b).unwrap(), q);
}

#[test]
fn a_negative_price_survives_the_round_trip() {
    // Price is i64. A venue with negative prices must not wrap to a huge
    // positive quantity on the far side.
    let mut q = sample_quote();
    q.bid_price = i64::MIN + 1;
    let mut b = [0u8; Quote::SIZE];
    q.encode_into(&mut b);
    assert_eq!(Quote::decode(&b).unwrap().bid_price, i64::MIN + 1);
}

#[test]
fn trade_fields_land_at_their_spec_offsets() {
    let t = Trade {
        instrument_id: 7,
        source_id: 9,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 0x4142_4344_4546_4748,
        trade_price: 100,
        trade_qty: 200,
        trade_id: 300,
        cumulative_volume: 400,
    };
    let mut b = [0u8; Trade::SIZE];
    t.encode_into(&mut b);

    assert_eq!(b.len(), 52);
    assert_eq!(b[0], 0x04, "offset 0: Type");
    assert_eq!(b[1], 52, "offset 1: Length");
    assert_eq!(&b[4..8], &7u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &9u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(b[10], 1, "offset 10: Aggressor Side");
    assert_eq!(b[11], 0x02, "offset 11: Trade Flags");
    assert_eq!(&b[12..20], &0x4142_4344_4546_4748u64.to_le_bytes(), "offset 12: Source Timestamp");
    assert_eq!(&b[20..28], &100i64.to_le_bytes(), "offset 20: Trade Price");
    assert_eq!(&b[28..36], &200u64.to_le_bytes(), "offset 28: Trade Quantity");
    assert_eq!(&b[36..44], &300u64.to_le_bytes(), "offset 36: Trade ID");
    assert_eq!(&b[44..52], &400u64.to_le_bytes(), "offset 44: Cumulative Volume");

    assert_eq!(Trade::decode(&b).unwrap(), t);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-tob`
Expected: FAIL — the crate does not exist.

- [ ] **Step 3: Create the crate**

```toml
# rust/codec/dz-edge-tob/Cargo.toml
[package]
name = "dz-edge-tob"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
description = "Top-of-Book & Trades feed wire format for DoubleZero Edge"

[dependencies]
dz-edge-core = { path = "../dz-edge-core" }
```

Add `"codec/dz-edge-tob"` to `members` in `rust/Cargo.toml`.

- [ ] **Step 4: Write the implementation**

```rust
// rust/codec/dz-edge-tob/src/quote.rs
use dz_edge_core::{AppMessage, DecodeError};

pub const QUOTE_BID_UPDATED: u8 = 0x01;
pub const QUOTE_ASK_UPDATED: u8 = 0x02;
pub const QUOTE_BID_GONE: u8 = 0x04;
pub const QUOTE_ASK_GONE: u8 = 0x08;

/// `0x03 Quote` (60 bytes). One two-sided BBO update.
///
/// Prices carry the instrument's Price Exponent and quantities its Qty
/// Exponent, both from `InstrumentDefinition`. This type does no scaling: the
/// caller supplies the raw fixed-point integers, which is what keeps the wire
/// exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub instrument_id: u32,
    pub source_id: u16,
    pub update_flags: u8,
    pub source_timestamp_ns: u64,
    pub bid_price: i64,
    pub bid_qty: u64,
    pub ask_price: i64,
    pub ask_qty: u64,
    pub bid_source_count: u16,
    pub ask_source_count: u16,
}

impl AppMessage for Quote {
    const TYPE_ID: u8 = 0x03;
    const SIZE: usize = 60;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.update_flags;
        dst[11] = 0;
        dst[12..20].copy_from_slice(&self.source_timestamp_ns.to_le_bytes());
        dst[20..28].copy_from_slice(&self.bid_price.to_le_bytes());
        dst[28..36].copy_from_slice(&self.bid_qty.to_le_bytes());
        dst[36..44].copy_from_slice(&self.ask_price.to_le_bytes());
        dst[44..52].copy_from_slice(&self.ask_qty.to_le_bytes());
        dst[52..54].copy_from_slice(&self.bid_source_count.to_le_bytes());
        dst[54..56].copy_from_slice(&self.ask_source_count.to_le_bytes());
        dst[56..60].fill(0);
    }
}

impl Quote {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            instrument_id: u32::from_le_bytes(buf[4..8].try_into().unwrap_or_default()),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            update_flags: buf[10],
            source_timestamp_ns: u64::from_le_bytes(buf[12..20].try_into().unwrap_or_default()),
            bid_price: i64::from_le_bytes(buf[20..28].try_into().unwrap_or_default()),
            bid_qty: u64::from_le_bytes(buf[28..36].try_into().unwrap_or_default()),
            ask_price: i64::from_le_bytes(buf[36..44].try_into().unwrap_or_default()),
            ask_qty: u64::from_le_bytes(buf[44..52].try_into().unwrap_or_default()),
            bid_source_count: u16::from_le_bytes([buf[52], buf[53]]),
            ask_source_count: u16::from_le_bytes([buf[54], buf[55]]),
        })
    }
}
```

```rust
// rust/codec/dz-edge-tob/src/trade.rs
use dz_edge_core::{AppMessage, DecodeError};

pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

pub const TRADE_FLAG_BLOCK: u8 = 0x01;
/// Bit 1. Keeps the name `sweep`: it is the externally defined term for an
/// order sweeping several levels, and it is a wire field name.
pub const TRADE_FLAG_SWEEP: u8 = 0x02;
pub const TRADE_FLAG_CROSS: u8 = 0x04;

/// `0x04 Trade` (52 bytes). One execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub trade_flags: u8,
    pub source_timestamp_ns: u64,
    pub trade_price: i64,
    pub trade_qty: u64,
    /// Venue-assigned. 0 if the venue exposes none.
    pub trade_id: u64,
    /// Session cumulative volume. 0 if unavailable.
    pub cumulative_volume: u64,
}

impl AppMessage for Trade {
    const TYPE_ID: u8 = 0x04;
    const SIZE: usize = 52;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.aggressor_side;
        dst[11] = self.trade_flags;
        dst[12..20].copy_from_slice(&self.source_timestamp_ns.to_le_bytes());
        dst[20..28].copy_from_slice(&self.trade_price.to_le_bytes());
        dst[28..36].copy_from_slice(&self.trade_qty.to_le_bytes());
        dst[36..44].copy_from_slice(&self.trade_id.to_le_bytes());
        dst[44..52].copy_from_slice(&self.cumulative_volume.to_le_bytes());
    }
}

impl Trade {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            instrument_id: u32::from_le_bytes(buf[4..8].try_into().unwrap_or_default()),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            aggressor_side: buf[10],
            trade_flags: buf[11],
            source_timestamp_ns: u64::from_le_bytes(buf[12..20].try_into().unwrap_or_default()),
            trade_price: i64::from_le_bytes(buf[20..28].try_into().unwrap_or_default()),
            trade_qty: u64::from_le_bytes(buf[28..36].try_into().unwrap_or_default()),
            trade_id: u64::from_le_bytes(buf[36..44].try_into().unwrap_or_default()),
            cumulative_volume: u64::from_le_bytes(buf[44..52].try_into().unwrap_or_default()),
        })
    }
}
```

```rust
// rust/codec/dz-edge-tob/src/lib.rs
//! Top-of-Book & Trades feed wire format.

#![forbid(unsafe_code)]

pub mod quote;
pub mod trade;

pub use quote::{Quote, QUOTE_ASK_GONE, QUOTE_ASK_UPDATED, QUOTE_BID_GONE, QUOTE_BID_UPDATED};
pub use trade::{
    Trade, AGGRESSOR_BUY, AGGRESSOR_SELL, AGGRESSOR_UNKNOWN, TRADE_FLAG_BLOCK, TRADE_FLAG_CROSS,
    TRADE_FLAG_SWEEP,
};
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-tob`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add rust/Cargo.toml rust/codec/dz-edge-tob
git commit -m "codec: add dz-edge-tob with Quote and Trade"
```

---

### Task 6: `dz-edge-refdata` — InstrumentDefinition at v3, decoding v1

This is the message the two forks migrated independently. It is the only message whose layout changed between generations, so it carries the whole dual-version burden.

**Files:**
- Create: `rust/codec/dz-edge-refdata/Cargo.toml`
- Create: `rust/codec/dz-edge-refdata/src/lib.rs`
- Create: `rust/codec/dz-edge-refdata/src/instrument_definition.rs`
- Create: `rust/codec/dz-edge-refdata/src/manifest_summary.rs`
- Modify: `rust/Cargo.toml` (add the member)
- Test: `rust/codec/dz-edge-refdata/tests/wire_layout.rs`

**Interfaces:**
- Consumes: `dz_edge_core::{AppMessage, DecodeError, SCHEMA_VERSION_V1}`.
- Produces:
  - `pub const SYMBOL_LEN: usize = 64;` `pub const SYMBOL_LEN_V1: usize = 16;` `pub const LEG_LEN: usize = 8;`
  - `pub const SIZE_V1: usize = 80;`
  - `pub struct InstrumentDefinition { pub instrument_id: u32, pub source_id: u16, pub symbol: [u8; 64], pub leg1: [u8; 8], pub leg2: [u8; 8], pub asset_class: u8, pub price_exponent: i8, pub qty_exponent: i8, pub market_model: u8, pub tick_size: i64, pub lot_size: u64, pub contract_value: u64, pub expiry_ns: u64, pub settle_type: u8, pub price_bound: u8, pub manifest_seq: u16 }` with `SIZE = 130`, `TYPE_ID = 0x02`.
  - `InstrumentDefinition::decode(buf: &[u8], schema_version: u8) -> Result<Self, DecodeError>`
  - `pub struct ManifestSummary { pub channel_id: u8, pub valid: u8, pub manifest_seq: u16, pub instrument_count: u32, pub timestamp_ns: u64 }` with `SIZE = 24`, `TYPE_ID = 0x07`, and `ManifestSummary::decode`. Note it carries **no** `Source ID`.
  - Asset class constants `ASSET_CLASS_UNKNOWN = 0`, `ASSET_CLASS_CRYPTO_SPOT = 1`, `ASSET_CLASS_PREDICTION_BINARY = 2`, `ASSET_CLASS_PREDICTION_SCALAR = 3`, `ASSET_CLASS_PREDICTION_CATEGORICAL = 4`, `ASSET_CLASS_PERPETUAL_FUTURE = 5`; `MARKET_MODEL_UNKNOWN = 0`, `MARKET_MODEL_CLOB = 1`, `MARKET_MODEL_AMM = 2`; `SETTLE_TYPE_NA = 0`, `SETTLE_TYPE_CASH = 1`, `SETTLE_TYPE_PHYSICAL = 2`; `PRICE_BOUND_UNBOUNDED = 0`, `PRICE_BOUND_UNIT_INTERVAL = 1`, `PRICE_BOUND_NON_NEGATIVE = 2`.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-refdata/tests/wire_layout.rs
use dz_edge_core::{AppMessage, SCHEMA_VERSION, SCHEMA_VERSION_V1};
use dz_edge_refdata::{InstrumentDefinition, ManifestSummary, LEG_LEN, SIZE_V1, SYMBOL_LEN};

fn sample() -> InstrumentDefinition {
    let mut symbol = [0u8; SYMBOL_LEN];
    symbol[..8].copy_from_slice(b"BTC-USDT");
    let mut leg1 = [0u8; LEG_LEN];
    leg1[..3].copy_from_slice(b"BTC");
    let mut leg2 = [0u8; LEG_LEN];
    leg2[..4].copy_from_slice(b"USDT");
    InstrumentDefinition {
        instrument_id: 42,
        source_id: 7,
        symbol,
        leg1,
        leg2,
        asset_class: 1,
        price_exponent: -2,
        qty_exponent: -8,
        market_model: 1,
        tick_size: 1,
        lot_size: 1000,
        contract_value: 0,
        expiry_ns: 0,
        settle_type: 0,
        price_bound: 0,
        manifest_seq: 9,
    }
}

#[test]
fn definition_fields_land_at_their_spec_offsets() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);

    assert_eq!(b.len(), 130);
    assert_eq!(b[0], 0x02, "offset 0: Type");
    assert_eq!(b[1], 130, "offset 1: Length");
    assert_eq!(&b[4..8], &42u32.to_le_bytes(), "offset 4: Instrument ID");
    assert_eq!(&b[8..10], &7u16.to_le_bytes(), "offset 8: Source ID");
    assert_eq!(&b[10..74], &d.symbol[..], "offset 10: Symbol, char[64]");
    assert_eq!(&b[74..82], &d.leg1[..], "offset 74: Leg1, char[8]");
    assert_eq!(&b[82..90], &d.leg2[..], "offset 82: Leg2, char[8]");
    assert_eq!(b[90], 1, "offset 90: Asset Class");
    assert_eq!(b[91] as i8, -2, "offset 91: Price Exponent");
    assert_eq!(b[92] as i8, -8, "offset 92: Qty Exponent");
    assert_eq!(b[93], 1, "offset 93: Market Model");
    assert_eq!(&b[94..102], &1i64.to_le_bytes(), "offset 94: Tick Size");
    assert_eq!(&b[102..110], &1000u64.to_le_bytes(), "offset 102: Lot Size");
    assert_eq!(&b[110..118], &0u64.to_le_bytes(), "offset 110: Contract Value");
    assert_eq!(&b[118..126], &0u64.to_le_bytes(), "offset 118: Expiry");
    assert_eq!(b[126], 0, "offset 126: Settle Type");
    assert_eq!(b[127], 0, "offset 127: Price Bound");
    assert_eq!(&b[128..130], &9u16.to_le_bytes(), "offset 128: Manifest Seq");
}

#[test]
fn definition_round_trips_at_v3() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert_eq!(InstrumentDefinition::decode(&b, SCHEMA_VERSION).unwrap(), d);
}

#[test]
fn a_v1_definition_decodes_at_its_own_offsets() {
    // v1: 80 bytes, no Source ID, Symbol is char[16]. Everything after Symbol
    // sits 50 bytes earlier than in v3. A subscriber meets this while one
    // publisher is still on schema 1.
    let mut b = [0u8; SIZE_V1];
    b[0] = 0x02;
    b[1] = SIZE_V1 as u8;
    b[4..8].copy_from_slice(&42u32.to_le_bytes()); // Instrument ID
    b[8..24].copy_from_slice(b"BTC-USDT\0\0\0\0\0\0\0\0"); // Symbol, char[16]
    b[24..32].copy_from_slice(b"BTC\0\0\0\0\0"); // Leg1
    b[32..40].copy_from_slice(b"USDT\0\0\0\0"); // Leg2
    b[40] = 1; // Asset Class
    b[41] = (-2i8) as u8; // Price Exponent
    b[42] = (-8i8) as u8; // Qty Exponent
    b[43] = 1; // Market Model
    b[44..52].copy_from_slice(&1i64.to_le_bytes()); // Tick Size
    b[52..60].copy_from_slice(&1000u64.to_le_bytes()); // Lot Size
    b[60..68].copy_from_slice(&0u64.to_le_bytes()); // Contract Value
    b[68..76].copy_from_slice(&0u64.to_le_bytes()); // Expiry
    b[76] = 0; // Settle Type
    b[77] = 0; // Price Bound
    b[78..80].copy_from_slice(&9u16.to_le_bytes()); // Manifest Seq

    let d = InstrumentDefinition::decode(&b, SCHEMA_VERSION_V1).unwrap();
    assert_eq!(d.instrument_id, 42);
    assert_eq!(&d.symbol[..8], b"BTC-USDT");
    assert_eq!(&d.symbol[8..], &[0u8; SYMBOL_LEN - 8][..], "widened symbol is null-padded");
    assert_eq!(d.source_id, 0, "v1 carries no Source ID; it reads as 0");
    assert_eq!(d.price_exponent, -2);
    assert_eq!(d.qty_exponent, -8);
    assert_eq!(d.lot_size, 1000);
    assert_eq!(d.manifest_seq, 9);
}

#[test]
fn schema_two_is_refused() {
    let d = sample();
    let mut b = [0u8; InstrumentDefinition::SIZE];
    d.encode_into(&mut b);
    assert!(
        InstrumentDefinition::decode(&b, 2).is_err(),
        "the 128-byte layout never reached the wire and must not be decodable"
    );
}

#[test]
fn manifest_summary_carries_count_and_seq() {
    let m = ManifestSummary {
        channel_id: 7,
        valid: 1,
        manifest_seq: 9,
        instrument_count: 1234,
        timestamp_ns: 88,
    };
    let mut b = [0u8; ManifestSummary::SIZE];
    m.encode_into(&mut b);

    assert_eq!(b.len(), 24);
    assert_eq!(b[0], 0x07, "offset 0: Type");
    assert_eq!(b[1], 24, "offset 1: Length");
    assert_eq!(b[4], 7, "offset 4: Channel ID");
    assert_eq!(b[5], 1, "offset 5: Valid");
    assert_eq!(&b[6..8], &[0, 0], "offset 6: Reserved, 2 bytes");
    assert_eq!(&b[8..10], &9u16.to_le_bytes(), "offset 8: Manifest Seq");
    assert_eq!(&b[10..12], &[0, 0], "offset 10: Reserved, 2 bytes");
    assert_eq!(&b[12..16], &1234u32.to_le_bytes(), "offset 12: Instrument Count");
    assert_eq!(&b[16..24], &88u64.to_le_bytes(), "offset 16: Timestamp");
    assert_eq!(ManifestSummary::decode(&b).unwrap(), m);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-refdata`
Expected: FAIL — the crate does not exist.

- [ ] **Step 3: Create the crate**

```toml
# rust/codec/dz-edge-refdata/Cargo.toml
[package]
name = "dz-edge-refdata"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
description = "Reference Data Distribution wire format for DoubleZero Edge"

[dependencies]
dz-edge-core = { path = "../dz-edge-core" }
```

Add `"codec/dz-edge-refdata"` to `members` in `rust/Cargo.toml`.

- [ ] **Step 4: Write `InstrumentDefinition`**

```rust
// rust/codec/dz-edge-refdata/src/instrument_definition.rs
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
        debug_assert_eq!(dst.len(), Self::SIZE);
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
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        let mut symbol = [0u8; SYMBOL_LEN];
        symbol.copy_from_slice(&buf[10..74]);
        let mut leg1 = [0u8; LEG_LEN];
        leg1.copy_from_slice(&buf[74..82]);
        let mut leg2 = [0u8; LEG_LEN];
        leg2.copy_from_slice(&buf[82..90]);
        Ok(Self {
            instrument_id: u32::from_le_bytes(buf[4..8].try_into().unwrap_or_default()),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            symbol,
            leg1,
            leg2,
            asset_class: buf[90],
            price_exponent: buf[91] as i8,
            qty_exponent: buf[92] as i8,
            market_model: buf[93],
            tick_size: i64::from_le_bytes(buf[94..102].try_into().unwrap_or_default()),
            lot_size: u64::from_le_bytes(buf[102..110].try_into().unwrap_or_default()),
            contract_value: u64::from_le_bytes(buf[110..118].try_into().unwrap_or_default()),
            expiry_ns: u64::from_le_bytes(buf[118..126].try_into().unwrap_or_default()),
            settle_type: buf[126],
            price_bound: buf[127],
            manifest_seq: u16::from_le_bytes([buf[128], buf[129]]),
        })
    }

    fn decode_v1(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < SIZE_V1 {
            return Err(DecodeError::ShortBuffer { need: SIZE_V1, got: buf.len() });
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
            instrument_id: u32::from_le_bytes(buf[4..8].try_into().unwrap_or_default()),
            source_id: 0,
            symbol,
            leg1,
            leg2,
            asset_class: buf[40],
            price_exponent: buf[41] as i8,
            qty_exponent: buf[42] as i8,
            market_model: buf[43],
            tick_size: i64::from_le_bytes(buf[44..52].try_into().unwrap_or_default()),
            lot_size: u64::from_le_bytes(buf[52..60].try_into().unwrap_or_default()),
            contract_value: u64::from_le_bytes(buf[60..68].try_into().unwrap_or_default()),
            expiry_ns: u64::from_le_bytes(buf[68..76].try_into().unwrap_or_default()),
            settle_type: buf[76],
            price_bound: buf[77],
            manifest_seq: u16::from_le_bytes([buf[78], buf[79]]),
        })
    }
}
```

- [ ] **Step 5: Write `ManifestSummary`**

```rust
// rust/codec/dz-edge-refdata/src/manifest_summary.rs
use dz_edge_core::{AppMessage, DecodeError};

/// `0x07 ManifestSummary` (24 bytes), on the `refdata` port role.
///
/// The manifest cadence MUST be shorter than the definition cycle period, so a
/// new subscriber sees a summary before it has finished collecting definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSummary {
    /// Redundant with the datagram header; useful for standalone logging.
    pub channel_id: u8,
    /// 1 once the published set is established; 0 while uninitialized or
    /// shutting down.
    pub valid: u8,
    pub manifest_seq: u16,
    pub instrument_count: u32,
    pub timestamp_ns: u64,
}

impl AppMessage for ManifestSummary {
    const TYPE_ID: u8 = 0x07;
    const SIZE: usize = 24;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4] = self.channel_id;
        dst[5] = self.valid;
        dst[6..8].fill(0);
        dst[8..10].copy_from_slice(&self.manifest_seq.to_le_bytes());
        dst[10..12].fill(0);
        dst[12..16].copy_from_slice(&self.instrument_count.to_le_bytes());
        dst[16..24].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }
}

impl ManifestSummary {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer { need: Self::SIZE, got: buf.len() });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            channel_id: buf[4],
            valid: buf[5],
            manifest_seq: u16::from_le_bytes([buf[8], buf[9]]),
            instrument_count: u32::from_le_bytes(buf[12..16].try_into().unwrap_or_default()),
            timestamp_ns: u64::from_le_bytes(buf[16..24].try_into().unwrap_or_default()),
        })
    }
}
```

```rust
// rust/codec/dz-edge-refdata/src/lib.rs
//! Reference Data Distribution wire format.

#![forbid(unsafe_code)]

pub mod instrument_definition;
pub mod manifest_summary;

pub use instrument_definition::*;
pub use manifest_summary::ManifestSummary;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd rust && cargo test -p dz-edge-refdata`
Expected: PASS, 5 tests.

- [ ] **Step 7: Commit**

```bash
git add rust/Cargo.toml rust/codec/dz-edge-refdata
git commit -m "codec: add dz-edge-refdata, encoding schema 3 and decoding 1 and 3"
```

---

### Task 7: Golden vectors and the CI gate

The cross-language contract. Vectors are the only thing binding the Rust codec, the Go decoders, the conformance tool and the dissectors to one interpretation of the specs.

**Files:**
- Create: `testdata/golden/README.md`
- Create: `testdata/golden/manifest.json`
- Create: `rust/codec/dz-edge-tob/tests/golden.rs`
- Create: `.github/workflows/rust-codec.yml`

**Interfaces:**
- Consumes: all three crates.
- Produces: `testdata/golden/<message>-v<schema>.bin` plus `testdata/golden/manifest.json` describing each vector's field values, both consumed later by the Go modules and the conformance suite.

- [ ] **Step 1: Write the failing test**

```rust
// rust/codec/dz-edge-tob/tests/golden.rs
//! Golden vectors: the cross-language contract.
//!
//! These bytes are the specification's meaning made concrete. Every
//! implementation in every language must reproduce them. A change here is a
//! wire change and must be justified against edge-feed-spec, never adjusted to
//! match code that started failing.

use dz_edge_core::AppMessage;
use dz_edge_tob::{Quote, Trade};
use std::path::PathBuf;

fn golden(name: &str) -> Vec<u8> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The canonical Quote. Values are deliberately asymmetric so a transposed
/// field pair cannot pass.
fn canonical_quote() -> Quote {
    Quote {
        instrument_id: 1,
        source_id: 2,
        update_flags: 0x03,
        source_timestamp_ns: 1_700_000_000_000_000_000,
        bid_price: 9_999_500,
        bid_qty: 12_500,
        ask_price: 10_000_500,
        ask_qty: 7_250,
        bid_source_count: 3,
        ask_source_count: 4,
    }
}

fn canonical_trade() -> Trade {
    Trade {
        instrument_id: 1,
        source_id: 2,
        aggressor_side: 1,
        trade_flags: 0x02,
        source_timestamp_ns: 1_700_000_000_000_000_001,
        trade_price: 10_000_000,
        trade_qty: 500,
        trade_id: 987_654_321,
        cumulative_volume: 1_000_000,
    }
}

#[test]
fn quote_matches_its_golden_vector() {
    let mut b = [0u8; Quote::SIZE];
    canonical_quote().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("quote-v3.bin"));
}

#[test]
fn trade_matches_its_golden_vector() {
    let mut b = [0u8; Trade::SIZE];
    canonical_trade().encode_into(&mut b);
    assert_eq!(b.to_vec(), golden("trade-v3.bin"));
}

#[test]
fn golden_vectors_decode_back_to_their_values() {
    assert_eq!(Quote::decode(&golden("quote-v3.bin")).unwrap(), canonical_quote());
    assert_eq!(Trade::decode(&golden("trade-v3.bin")).unwrap(), canonical_trade());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust && cargo test -p dz-edge-tob --test golden`
Expected: FAIL — `testdata/golden/quote-v3.bin` does not exist.

- [ ] **Step 3: Generate the vectors once, by hand, from the spec offsets**

Write the bytes from the field tables rather than from the encoder, so the vector is independent evidence rather than a snapshot of whatever the code did.

```bash
mkdir -p testdata/golden
python3 - <<'PY'
import struct, pathlib
out = pathlib.Path("testdata/golden")

# Quote, 60 bytes. Offsets from edge-feed-spec/top-of-book/spec.md.
q  = struct.pack("<BBH", 0x03, 60, 0)          # 0 Type, 1 Length, 2 Flags
q += struct.pack("<I", 1)                       # 4  Instrument ID
q += struct.pack("<H", 2)                       # 8  Source ID
q += struct.pack("<BB", 0x03, 0)                # 10 Update Flags, 11 Reserved
q += struct.pack("<Q", 1_700_000_000_000_000_000)  # 12 Source Timestamp
q += struct.pack("<q", 9_999_500)               # 20 Bid Price
q += struct.pack("<Q", 12_500)                  # 28 Bid Quantity
q += struct.pack("<q", 10_000_500)              # 36 Ask Price
q += struct.pack("<Q", 7_250)                   # 44 Ask Quantity
q += struct.pack("<HH", 3, 4)                   # 52 Bid/Ask Source Count
q += b"\x00" * 4                                # 56 Reserved
assert len(q) == 60, len(q)
(out / "quote-v3.bin").write_bytes(q)

# Trade, 52 bytes.
t  = struct.pack("<BBH", 0x04, 52, 0)
t += struct.pack("<I", 1)                       # 4  Instrument ID
t += struct.pack("<H", 2)                       # 8  Source ID
t += struct.pack("<BB", 1, 0x02)                # 10 Aggressor Side, 11 Trade Flags
t += struct.pack("<Q", 1_700_000_000_000_000_001)  # 12 Source Timestamp
t += struct.pack("<q", 10_000_000)              # 20 Trade Price
t += struct.pack("<Q", 500)                     # 28 Trade Quantity
t += struct.pack("<Q", 987_654_321)             # 36 Trade ID
t += struct.pack("<Q", 1_000_000)               # 44 Cumulative Volume
assert len(t) == 52, len(t)
(out / "trade-v3.bin").write_bytes(t)
print("wrote", sorted(p.name for p in out.glob("*.bin")))
PY
```

- [ ] **Step 4: Write the manifest and the README**

```json
{
  "spec_revision": "REPLACE with the edge-feed-spec commit these were transcribed from",
  "vectors": [
    {
      "file": "quote-v3.bin",
      "message": "Quote",
      "type_id": "0x03",
      "size": 60,
      "schema_version": 3,
      "fields": {
        "instrument_id": 1,
        "source_id": 2,
        "update_flags": 3,
        "source_timestamp_ns": 1700000000000000000,
        "bid_price": 9999500,
        "bid_qty": 12500,
        "ask_price": 10000500,
        "ask_qty": 7250,
        "bid_source_count": 3,
        "ask_source_count": 4
      }
    },
    {
      "file": "trade-v3.bin",
      "message": "Trade",
      "type_id": "0x04",
      "size": 52,
      "schema_version": 3,
      "fields": {
        "instrument_id": 1,
        "source_id": 2,
        "aggressor_side": 1,
        "trade_flags": 2,
        "source_timestamp_ns": 1700000000000000001,
        "trade_price": 10000000,
        "trade_qty": 500,
        "trade_id": 987654321,
        "cumulative_volume": 1000000
      }
    }
  ]
}
```

```markdown
# Golden vectors

One canonical byte vector per message type per schema version. These are the
cross-language contract: the Rust encoders, the Rust decoders, the Go decoders,
the conformance tool and the Wireshark dissectors must all reproduce them.

The bytes were transcribed by hand from the field tables in `edge-feed-spec`,
not captured from an encoder. That is the point — a vector generated from the
code under test proves only that the code agrees with itself.

`manifest.json` carries each vector's field values, so an implementation in any
language can check both directions without re-reading the specs.

**Changing a vector is a wire change.** Justify it against `edge-feed-spec` and
record the spec revision in `manifest.json`. Never edit a vector to make a
failing test pass.
```

- [ ] **Step 5: Fill in the spec revision**

```bash
# Record the exact edge-feed-spec commit the offsets came from.
# Run in a checkout of edge-feed-spec:
git -C <path-to-edge-feed-spec> rev-parse HEAD
```

Replace the `spec_revision` placeholder in `testdata/golden/manifest.json` with that hash and the date. Provenance is the whole value of a hand-transcribed vector.

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd rust && cargo test -p dz-edge-tob --test golden`
Expected: PASS, 3 tests.

- [ ] **Step 7: Add the CI gate**

```yaml
# .github/workflows/rust-codec.yml
name: rust-codec

on:
  pull_request:
    paths:
      - 'rust/**'
      - 'testdata/golden/**'
      - '.github/workflows/rust-codec.yml'
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: rust
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - name: Format
        run: cargo fmt --all -- --check
      - name: Clippy
        run: cargo clippy --all-targets -- -D warnings
      - name: Test
        run: cargo test --all
```

- [ ] **Step 8: Run the full suite**

Run: `cd rust && cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --all`
Expected: PASS, 29 tests across three crates, no warnings.

- [ ] **Step 9: Commit**

```bash
git add testdata/golden rust/codec/dz-edge-tob/tests/golden.rs .github/workflows/rust-codec.yml
git commit -m "codec: add golden vectors and the CI gate

Vectors are transcribed by hand from the spec field tables rather than
captured from the encoder, so they are independent evidence rather than a
snapshot of whatever the code did."
```

---

## What this plan deliberately leaves out

- **`dz-edge-mbp`, `dz-edge-mbo`, `dz-edge-perp-stats`.** Same pattern, own plan. They need `BatchBoundary`, `InstrumentReset` and `SnapshotEnd` added to core, and the two `SnapshotBegin` variants split across the two depth crates.
- **A message at `0x05`.** Reserved in every current spec. Two publishers transmit one there; whether that handshake earns a real identifier is an upstream question, not an implementation decision.
- **The Go modules.** They consume the same golden vectors and get their own plan.
- **Any publisher adopting these crates.** That is migration steps 2 through 4, and each venue's adoption is its own change in its own repository.

## Definition of done

- `cargo test --all` passes in `rust/`, with no `clippy` warnings at `-D warnings`.
- A datagram built with an MTU of 1448 is 1232 bytes or fewer, asserted by test.
- An `InstrumentDefinition` round-trips at schema 3 and decodes at schema 1; schema 2 is refused.
- The golden vectors exist, carry a real `spec_revision`, and are checked in CI.
- No identifier, comment or commit message names a venue.
- No identifier uses a term the glossary bans.
