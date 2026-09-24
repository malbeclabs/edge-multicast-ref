# Edge Recorder: the rule set reads the segment — Implementation Plan

**Date:** 2026-09-24

**Goal:** Hand `dz-conformance` the pcapng segment the recorder's own writer produces, instead of a classic `pcap` converted from it, so that `epb_dropcount` — the recorder's admission of what it failed to record — reaches the rule set.

**Architecture:** `dz-recorder-conformance` stops owning a capture format. Today it hand-writes a classic `pcap`; after this it calls `dz-recorder-archive`'s `SegmentWriter`, which is the writer the recorder itself uses. The bridge becomes a re-write of the archive through the same code path that wrote it, which is also what makes the spec's cross-language claim literal: *a pcapng segment written by the Rust writer is read by the Go conformance tool.*

**Tech Stack:** Rust 2021. One new path dependency, `dz-recorder-archive`, inside `dz-recorder-conformance`. No new external crate — `pcap-file` arrives with the archive crate and is not named here.

**Spec:** `docs/superpowers/specs/2026-08-28-edge-recorder-crates-design.md`, the *Cross-language* bullet of the acceptance list and step 2 of *Order of work*.

**Scope:** The conversion, the pinned rule-set revision, and the tests that prove what the conversion was destroying. The rollout (step 6) and the acceptance comparison against independent tooling (step 3) need a host and stay where they are.

---

## Why this is a correctness change and not a tidy-up

The bridge's own doc comment states three places where a careless conversion
manufactures a finding, and it handles all three. The one it cannot handle is
the one that is not a conversion detail but a missing field: **a classic `pcap`
record has nowhere to write `epb_dropcount`.**

Every datagram the recorder admits it dropped therefore arrives at the rule set
as a datagram that was never dropped. The sequence gap behind it is real, the
recorder's admission is gone, and the rule set — which grades an admitted gap
`capture_loss` rather than `violation` — grades it against the publisher. The
tool says so itself in `run.go`: *a legacy pcap converted from one has had the
per-packet option stripped by the conversion, so it reports no admitted drop and
means nothing by it — which is the reason to replay the archive directly.*

This is the misattribution the whole recorder exists to prevent, arriving
through the one seam nobody re-checked.

Two smaller things travel with it. The conversion truncates nanoseconds to
microseconds, which no current rule reasons about and which stops being a
question rather than being answered. And a re-written segment carries the
section header, so the recorder identity, build commit, config hash and
`capture_drop_scope` that replay already recovers travel to the tool instead of
being dropped on the floor.

---

## Global constraints

- **Nothing here touches the record path.** `SegmentWriter` is used, not changed. A task that wants to change it has mistaken the bridge for the recorder.
- **The gate keeps its own eyes.** `dz-recorder-e2e`'s conformance gate goes on reading the tool's raw exit code and raw stderr, uninterpreted. It reaches the tool through this crate — one bridge, not two — but it must never reach it through this crate's *reading* of the result.
- **No rule is named, encoded or allow-listed in this repository.** Unchanged, and the reason `dz-recorder-conformance` has no codec dependency.
- **One capture format after this, not two.** The classic-`pcap` writer goes. A lossy alternative left in the tree is one somebody reaches for.
- **Every task is verifiable with no host, no socket and no privileges.** The tool itself is the one external input, and it is a gate rather than a skip: absent tool with the feature on is a failure.

---

## Tasks

### 1. `SPEC_REF` moves to a revision whose reader takes pcapng

`.github/workflows/rust-codec.yml` pins `e68184b`, which predates
`tools/conformance/input/pcapng.go`. Move it to `931d68d`, and keep the comment
that says why a revision is pinned at all.

**Verification:** the `conformance` job builds the tool and `--version` prints
the new ref. A pin that did not move would make task 2's tests fail at the tool,
which is the check that the two tasks are actually coupled.

### 2. `segment.rs`, and `pcap.rs` goes

`write_group_segments(dir, datagrams, provenance) -> Vec<GroupSegment>` replaces
`write_group_pcaps`, with the same grouping and the same refusal to write a file
for a group holding no datagrams. `GroupPcap` becomes `GroupSegment`; the
`pcap_len` estimator, `FILE_HEADER_LEN` and `RECORD_HEADER_LEN` go with the
format they describe.

`provenance` is the section the segment is written under, and it is an argument
rather than a default. `ArchiveSource` exposes `identity()`,
`capture_drop_scope()`, `link_headers()` and `section_recv_ts_kind()` precisely
so that a re-write states what the archive stated. **A default here would be an
invented fact**: a section claiming `link_headers=captured` over synthesised
bytes is a claim the archive does not support, and the writer marks every
datagram that contradicts its section.

`Invocation.pcap` becomes `Invocation.capture`. The flag on the command line
stays `-pcap`, because that is what the tool calls it and it takes either
format; the field is renamed because the field is ours.

**Verification:** `cargo test -p dz-recorder-conformance`. `tests/bridge.rs`
becomes `tests/segment.rs` and keeps every case that is about the archive rather
than about classic `pcap`: the over-cap datagram whose original length must
exceed its captured length, the captured headers reproduced rather than rebuilt,
and the grouping.

### 3. The group split keeps its behaviour and loses its reason

The split is right and the stated reason is wrong. `-group` is **inert in
replay**: `buildSource` consults it only on the live path, and nothing in the
tool's `core`, `engine` or `report` reads it. A single file holding every group
would not be "read under one group's flags"; it would be read under the port
map, which is all replay has.

The reason to split is the case the old one hid: **two groups on the same
destination ports.** The port map is keyed on the destination port alone, so one
file holding both would merge two channels' sequence spaces into one series and
report the interleaving as loss. State that, because a reader who checks the old
reason against the tool will find it false and delete the split.

The behaviour is already covered:
`two_groups_in_one_archive_produce_two_files_each_holding_only_its_own` builds
both groups over `port_of(role)`, so the two already share one set of ports —
which is exactly the case the real reason names and the stated one does not.
Nothing about the split changes. What changes is the comment on that test and the
paragraph in the module doc above it, both of which currently argue from
`-group`.

**Verification:** the test passes unmodified except for its comment, which is the
check that this task changed a justification and not a behaviour.

### 4. The admission reaches the rule set

The test this whole plan is for. Build an archive whose datagrams carry a
non-zero `drop_delta`, replay it, write the segment, run the tool, and assert
that its stderr carries the capture-loss warning naming the number the archive
admitted.

**This must fail on the old path**, and the plan records how: with
`write_pcap` in place of `write_group_segments`, the tool reports no admitted
drop and the assertion on the warning fails. A test that passes both ways has
documented the tree instead of changing it.

**Verification:** `cargo test -p dz-recorder-e2e --features conformance`, with
the revert performed and the failure recorded before the fix is pushed.

### 5. The crate says what it is

`lib.rs`'s module list, the `pcap` bullet, and the paragraph in
`dz-recorder-e2e/tests/common/conformance.rs` that opens *"It reads classic
`pcap` and the archive is `pcapng`"* — all three describe a bridge that no
longer exists. The crate-level doc states the new seam: the tool is handed the
archive, re-written by the recorder's own writer, and what the re-write adds to
the chain is still nothing.

**Verification:** `cargo clippy -p dz-recorder-conformance -p dz-recorder-e2e
--all-targets -- -D warnings`, and `scripts/check-public-repo-rules.sh`.

---

## What this does not close

**Step 3 and step 6 need a host.** Nothing in this plan brings either closer,
and nothing in it should be read as having.

**The rule set over a real archive.** After this, the gate replays traffic a
test generated. That the mechanism is now lossless does not make the traffic
real, and the analysis tier has still never been pointed at an archive a
recorder on a host wrote.

**The tool does not stamp its own version.** The `conformance` job patches
`main.version` and `main.commit` with `-ldflags` because an unstamped build
answers `dev+none`, and the runner refuses a verdict it cannot attribute. Moving
the pin does not change that; the `-ldflags` stand-in and the comment explaining
it both stay.
