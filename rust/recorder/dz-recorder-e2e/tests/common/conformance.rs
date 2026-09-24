//! The chain the whole design rests on, judged by the specification's own rule
//! set: **publisher's encoder → recorder's archive → replay → conformance**.
//!
//! Every other test in this crate checks the chain against itself — the bytes
//! that came back are the bytes that went out. That catches corruption and
//! cannot catch agreement on something the spec forbids: an encoder writing an
//! invalid stream and an archive faithfully keeping it pass every round trip in
//! this repository. `dz-conformance` is the third party. It is the
//! specification's own tool, in the specification's own repository, and it
//! knows 88 rules this repository has never encoded.
//!
//! The tool is handed the segment itself: replay's output, written back out as
//! pcapng by the recorder's own `SegmentWriter` — **inside
//! `dz-recorder-conformance`, and not here.** That crate is the analysis tier's
//! bridge, and this gate reaches the tool through it deliberately: a bridge with
//! two implementations is a bridge where the gate and the runner can disagree
//! about what the tool was shown, and the gate is the one nobody would think to
//! re-check. What the re-write adds to the chain is still nothing: the datagram
//! bytes handed to the writer are exactly the bytes replay produced.
//!
//! pcapng and not the classic `pcap` the tool also accepts, because a classic
//! record has nowhere to write `epb_dropcount` — the recorder's own admission of
//! what it failed to record. Over a converted file the rule set sees every gap
//! the recorder caused and nothing saying the recorder caused it, so it grades
//! them against the publisher. `a_recorders_own_loss_is_not_charged_to_the_
//! publisher` is the case that holds this open.
//!
//! What stays here is the assertion. This gate reads the tool's raw exit code
//! and its raw standard error, uninterpreted, because a gate that shared the
//! runner's reading of an exit code could not catch the runner reading it
//! wrongly.
//!
//! Behind the `conformance` feature, and it does not skip. If the feature is on
//! and the tool is absent the test fails, because a conformance gate that
//! quietly passes when it cannot run is worse than no gate: it reports a clean
//! feed for a stream nobody validated.
//! A submodule of `common` rather than a suite of its own: it holds no tests,
//! and every suite that validates a chain reaches it through the same helpers.

use std::path::PathBuf;

use super::{port_of, replay, Recorded, ALL_ROLES, GROUP};
use dz_recorder_conformance::segment::{write_group_segments, SectionProvenance};
use dz_recorder_conformance::tool::{ConformanceTool, Invocation, PortRoles};
use dz_recorder_replay::ArchiveSource;

/// Where the tool is. Set by whatever runs the suite, because it is built from
/// a sibling repository this one does not vendor.
const TOOL_ENV: &str = "DZ_CONFORMANCE_BIN";

fn tool() -> PathBuf {
    let path = std::env::var(TOOL_ENV).unwrap_or_else(|_| {
        panic!(
            "{TOOL_ENV} is unset. This suite validates the archive against \
             edge-feed-spec's dz-conformance, which lives in that repository: build it with \
             `go build -o <path> ./tools/conformance` and point {TOOL_ENV} at the result. \
             Skipping instead would report a clean feed for a stream nobody checked."
        )
    });
    let path = PathBuf::from(path);
    assert!(
        path.is_file(),
        "{TOOL_ENV} points at {}, which is not a file",
        path.display()
    );
    path
}

/// What one conformance run concluded.
pub struct Verdict {
    pub code: i32,
    pub stderr: String,
}

impl Verdict {
    /// The tool's own contract: 0 passed, 1 found a violation, 2 could not run.
    pub fn assert_clean(&self) {
        assert_ne!(
            self.code, 2,
            "dz-conformance could not run at all:\n{}",
            self.stderr
        );
        assert_eq!(
            self.code, 0,
            "the specification's own rule set found violations in what this \
             publisher wrote and this recorder kept:\n{}",
            self.stderr
        );
    }
}

/// What the archive states about itself, so the segment written from it states
/// the same and invents nothing.
fn provenance_of(archive: &Recorded) -> SectionProvenance {
    let source = ArchiveSource::open(&archive.object).expect("the archive opens");
    SectionProvenance::of(&source).expect("an archive this recorder wrote states its own section")
}

/// Replays the archive, writes it back out as a segment and runs the tool over
/// it.
pub fn conformance_of(archive: &Recorded, feed: &str) -> Verdict {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let segments = write_group_segments(
        dir.path(),
        &replay(&archive.object),
        &provenance_of(archive),
    )
    .expect("the bridge writes a segment");

    // One group throughout this crate, so one invocation. The bridge writes one
    // file per group because the tool's port map is keyed on the destination
    // port alone, and a second file here would mean a second group nobody in
    // this suite joined.
    assert_eq!(
        segments.iter().map(|s| s.group).collect::<Vec<_>>(),
        vec![GROUP],
        "these fixtures publish to one group, and the tool judges one at a time"
    );

    let mut ports = PortRoles::none();
    for role in ALL_ROLES {
        ports = ports.with(*role, port_of(*role));
    }

    let run = ConformanceTool::new(tool(), dir.path())
        .run(
            &Invocation {
                capture: &segments[0].path,
                group: segments[0].group,
                feed,
                ports,
            },
            None,
        )
        .expect("the conformance tool runs");

    Verdict {
        code: run.code.expect("the tool was not signalled"),
        stderr: run.stderr,
    }
}
