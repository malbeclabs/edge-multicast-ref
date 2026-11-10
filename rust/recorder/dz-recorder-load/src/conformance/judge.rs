//! One object, judged: vouched for, replayed, handed to the rule set one group
//! at a time, and its verdicts made into rows.
//!
//! **An object either yields all of its rows or none of them.** A digest that
//! does not match, a replay that ends on a tear, a section that changes its
//! claims part way, a rule set that could not run or a report it cannot read —
//! each is an error, and an error writes nothing. In particular a tool that did
//! not start never becomes a table of `unverifiable`: that is a statement about
//! us, and it would move the panel that measures how often the archive opens
//! the gate every time a binary went missing.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use dz_recorder_archive::SegmentManifest;
use dz_recorder_conformance::{
    write_group_segments, BridgeError, Invocation, PinnedRuleSet, PortRoles, RuleSet,
    RuleSetReport, SectionProvenance, ToolError,
};
use dz_recorder_core::{OwnedDatagram, SourceError};
use dz_recorder_replay::{ArchiveSource, Termination};
use dz_recorder_rows::derive::verify_digest;
use dz_recorder_rows::{DeriveError, RowBatch};
use thiserror::Error;

use super::findings::{place, role_named, HoleFinder, Refused};
use super::rows::{finding_rows, RunStamp};

#[derive(Debug, Error)]
pub enum JudgeError {
    /// The object's bytes are not the ones its manifest describes.
    #[error("{0}")]
    Digest(DeriveError),
    #[error("{path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: SourceError,
    },
    #[error(
        "{object_key}: the replay ended as {termination} rather than at the end of the archive, \
         so the window is short and a pass over it would be a pass over traffic that was never \
         read"
    )]
    Incomplete {
        object_key: String,
        termination: String,
    },
    /// The archive's own section does not state what a re-write must carry.
    #[error("{object_key}: {source}")]
    Section {
        object_key: String,
        #[source]
        source: BridgeError,
    },
    #[error(
        "{object_key}: the archive's section changed its claims part way through, and a \
         re-write under the first section's claims would misstate the rest"
    )]
    SectionChanged { object_key: String },
    #[error(
        "{object_key}: the archive states its capture drops at `{section}` and its manifest at \
         `{manifest}`; holes found under the wrong scope are not this object's holes"
    )]
    ScopeDisagreement {
        object_key: String,
        section: String,
        manifest: String,
    },
    #[error(
        "{object_key}: the manifest joins a role it spells `{role}`, which is not one of the \
         three the specification names"
    )]
    UnknownRole { object_key: String, role: String },
    #[error("scratch {path}: {source}")]
    Scratch {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{object_key}: re-writing the segment for the rule set: {source}")]
    Bridge {
        object_key: String,
        #[source]
        source: BridgeError,
    },
    #[error("{object_key}: {source}")]
    Tool {
        object_key: String,
        #[source]
        source: ToolError,
    },
}

impl JudgeError {
    /// Whether this is the rule set failing to run, as opposed to the object.
    #[must_use]
    pub const fn is_tool(&self) -> bool {
        matches!(self, Self::Tool { .. })
    }
}

/// What one object needs to be judged.
#[derive(Debug)]
pub struct JudgeInput<'a, R> {
    pub object: &'a Path,
    pub manifest: &'a SegmentManifest,
    pub rule_set: &'a PinnedRuleSet<R>,
    /// The rule set's own name for the feed (its `-feed`), which is not the
    /// manifest's spec name.
    pub rule_set_feed: &'a str,
    /// The runner's own writable path. Never a directory the recorder's
    /// staging budget has to reach.
    pub scratch: &'a Path,
    pub run_ts: u64,
}

/// One object's verdicts, and what could not be placed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Judged {
    /// `conformance_finding` only; every other grain is empty, and stays the
    /// load's to write.
    pub batch: RowBatch,
    pub refused: Refused,
    /// Runs of the rule set, which is groups in the object.
    pub invocations: u64,
}

/// Judges one object.
///
/// # Errors
///
/// Any of [`JudgeError`]; on every one of them nothing is written.
pub fn judge_object<R: RuleSet>(input: &JudgeInput<'_, R>) -> Result<Judged, JudgeError> {
    let manifest = input.manifest;
    let key = || manifest.object_key.clone();

    // 1. Vouched for: the bytes are the ones the manifest describes.
    verify_digest(input.object, manifest).map_err(JudgeError::Digest)?;

    // 2. Replayed to its end, under one section's claims, with its own holes
    //    found in the same walk.
    let mut source = ArchiveSource::open(input.object).map_err(|source| JudgeError::Open {
        path: input.object.to_path_buf(),
        source,
    })?;
    let provenance = SectionProvenance::of(&source).map_err(|source| JudgeError::Section {
        object_key: key(),
        source,
    })?;
    let scope = provenance.capture_drop_scope;
    if manifest.capture_drop_scope != scope.as_str() {
        return Err(JudgeError::ScopeDisagreement {
            object_key: key(),
            section: scope.as_str().to_owned(),
            manifest: manifest.capture_drop_scope.clone(),
        });
    }
    let mut holes = HoleFinder::new(scope);
    let mut datagrams: Vec<OwnedDatagram> = Vec::new();
    for dg in &mut source {
        holes.observe(&dg.as_recorded());
        datagrams.push(dg);
    }
    if source.terminated_by() != Termination::Eof {
        return Err(JudgeError::Incomplete {
            object_key: key(),
            termination: format!("{:?}", source.terminated_by()),
        });
    }
    if SectionProvenance::of(&source).ok().as_ref() != Some(&provenance) {
        return Err(JudgeError::SectionChanged { object_key: key() });
    }
    let holes = holes.finish();

    // 3. One file per group, in a directory of this object's own.
    let dir = tempfile::Builder::new()
        .prefix("dz-judge-")
        .tempdir_in(input.scratch)
        .map_err(|source| JudgeError::Scratch {
            path: input.scratch.to_path_buf(),
            source,
        })?;
    let segments = write_group_segments(dir.path(), &datagrams, &provenance).map_err(|source| {
        JudgeError::Bridge {
            object_key: key(),
            source,
        }
    })?;
    // The bytes are on disk now; the copy in memory is not needed for the runs.
    drop(datagrams);

    // 4. The rule set, once per group, with the ports the manifest says were
    //    joined in that group — never the runner's configuration.
    let mut reports: Vec<(Ipv4Addr, RuleSetReport)> = Vec::with_capacity(segments.len());
    for segment in &segments {
        let ports = ports_joined(manifest, segment.group)?;
        let report = input
            .rule_set
            .judge(&Invocation {
                capture: &segment.path,
                group: segment.group,
                feed: input.rule_set_feed,
                ports,
            })
            .map_err(|source| JudgeError::Tool {
                object_key: key(),
                source,
            })?;
        reports.push((segment.group, report));
    }

    // 5. Rows.
    let stamp = RunStamp {
        manifest,
        rule_set_version: input.rule_set.version(),
        run_ts: input.run_ts,
    };
    let mut refused = Refused::default();
    let mut rows = Vec::new();
    for (group, report) in &reports {
        let (placed, r) = place(manifest, *group, report, &holes);
        refused.add(&r);
        rows.extend(finding_rows(&stamp, &placed));
    }
    Ok(Judged {
        batch: RowBatch {
            object_key: manifest.object_key.clone(),
            object_sha256: manifest.sha256.clone(),
            conformance_finding: rows,
            ..RowBatch::default()
        },
        refused,
        invocations: reports.len() as u64,
    })
}

/// The ports each role was joined on in `group`, as the manifest states them.
fn ports_joined(manifest: &SegmentManifest, group: Ipv4Addr) -> Result<PortRoles, JudgeError> {
    let mut ports = PortRoles::none();
    for join in manifest.roles_joined.iter().filter(|j| j.group == group) {
        let role = role_named(&join.role).ok_or_else(|| JudgeError::UnknownRole {
            object_key: manifest.object_key.clone(),
            role: join.role.clone(),
        })?;
        ports = ports.with(role, join.port);
    }
    Ok(ports)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::RefCell;

    use dz_edge_core::PortRole;
    use dz_recorder_conformance::report::REPORT_FORMAT;
    use dz_recorder_conformance::{EvidenceRange, Outcome, ReportInstance, RuleOutcome};
    use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, CHANNEL_ID, PRIMARY_SOURCE};
    use dz_recorder_rows::{ConformanceFinding, FileSink, FindingVerdict, Grain, Nanos, RowSink};

    use super::*;
    use crate::conformance::testing::record;

    pub const VERSION: &str = "0.4.2+e68184bc25e25f1a9f0c26c465fdaf3bb5f23268";

    /// A rule set that answers with a stated report, and counts its runs.
    #[derive(Debug, Clone)]
    pub struct StubRuleSet {
        pub version: String,
        pub rules: Vec<RuleOutcome>,
        /// Fails every run as the tool would when it could not start.
        pub broken: bool,
        pub runs: std::rc::Rc<RefCell<Vec<(Ipv4Addr, PortRoles)>>>,
    }

    impl StubRuleSet {
        pub fn stating(rules: Vec<RuleOutcome>) -> Self {
            Self {
                version: VERSION.to_owned(),
                rules,
                broken: false,
                runs: std::rc::Rc::default(),
            }
        }
    }

    impl RuleSet for StubRuleSet {
        fn judge(&self, invocation: &Invocation<'_>) -> Result<RuleSetReport, ToolError> {
            assert!(
                invocation.capture.is_file(),
                "the segment is on disk while judged"
            );
            self.runs
                .borrow_mut()
                .push((invocation.group, invocation.ports));
            if self.broken {
                return Err(ToolError::CouldNotRun {
                    tool: self.describe(),
                    stderr: "no such rule set".to_owned(),
                });
            }
            Ok(RuleSetReport {
                report_format: REPORT_FORMAT,
                feed: invocation.feed.to_owned(),
                rules: self.rules.clone(),
            })
        }

        fn rule_set_version(&self) -> Result<String, ToolError> {
            Ok(self.version.clone())
        }

        fn describe(&self) -> String {
            "a stand-in rule set".to_owned()
        }
    }

    pub fn on_mktdata(rule: &str, outcome: Outcome, first: u64, last: u64) -> RuleOutcome {
        RuleOutcome {
            rule_id: rule.to_owned(),
            outcome,
            instance: Some(ReportInstance {
                source: PRIMARY_SOURCE,
                channel_id: CHANNEL_ID,
                dst_port: port_for(PortRole::Mktdata),
            }),
            evidence: Some(EvidenceRange {
                reset_count: 0,
                first_seq: first,
                last_seq: last,
            }),
            detail: String::new(),
            needs: Vec::new(),
        }
    }

    fn judge(
        recorded: &crate::conformance::testing::Recorded,
        rule_set: StubRuleSet,
        scratch: &Path,
    ) -> Result<Judged, JudgeError> {
        let pinned = PinnedRuleSet::resolve(rule_set, VERSION).expect("the stub names itself");
        judge_object(&JudgeInput {
            object: &recorded.object,
            manifest: &recorded.manifest,
            rule_set: &pinned,
            rule_set_feed: "tob",
            scratch,
            run_ts: 77,
        })
    }

    /// A clean object: one row per rule per instance, all `pass`, through the
    /// file sink that writes exactly what the column store would be sent.
    #[test]
    fn a_clean_object_produces_one_pass_row_per_rule_per_instance() {
        let (dir, recorded) = record(&SyntheticPublisher::clean(200));
        let scratch = tempfile::tempdir().expect("scratch");
        let stub = StubRuleSet::stating(vec![
            on_mktdata("TEST.ONE", Outcome::Pass, 0, 199),
            on_mktdata("TEST.TWO", Outcome::Pass, 0, 199),
            on_mktdata("TEST.THREE", Outcome::Pass, 0, 199),
        ]);
        let runs = std::rc::Rc::clone(&stub.runs);
        let judged = judge(&recorded, stub, scratch.path()).expect("judged");

        // One group, and the ports are the manifest's joins — both roles,
        // although only mktdata carried anything.
        let runs = runs.borrow();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].1.port(PortRole::Mktdata),
            Some(port_for(PortRole::Mktdata))
        );
        assert_eq!(
            runs[0].1.port(PortRole::Snapshot),
            Some(port_for(PortRole::Snapshot))
        );
        assert_eq!(
            runs[0].1.port(PortRole::Refdata),
            None,
            "never joined, never offered"
        );

        let mut sink = FileSink::create(dir.path().join("rows")).expect("a sink");
        sink.write_batch(judged.batch.clone(), 0).expect("written");
        sink.flush(0).expect("flushed");
        let text = std::fs::read_to_string(sink.path(Grain::ConformanceFinding))
            .expect("the findings file");
        let rows: Vec<ConformanceFinding> = text
            .lines()
            .map(|l| serde_json::from_str(l).expect("a row"))
            .collect();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.verdict, FindingVerdict::Pass);
            assert_eq!(row.rule_set_version, VERSION);
            assert_eq!(row.run_ts, Nanos(77));
            assert_eq!(row.window_start, Nanos(recorded.manifest.start_ns));
            assert_eq!(row.window_end, Nanos(recorded.manifest.end_ns));
            assert_eq!(row.site, recorded.manifest.site);
            assert_eq!(row.feed, recorded.manifest.feed);
        }
        // And nothing in any other grain: the runner writes only this table.
        for grain in Grain::ALL {
            if grain != Grain::ConformanceFinding {
                assert!(!sink.path(grain).exists(), "{grain} was written");
            }
        }
        // The scratch directory holds nothing once the object is done.
        assert_eq!(
            std::fs::read_dir(scratch.path()).expect("scratch").count(),
            0,
            "the re-written segments outlived the object"
        );
    }

    /// No row for an object whose digest did not match, and the rule set is
    /// never asked: a refusal is an absence, not a window of passes.
    #[test]
    fn an_object_whose_digest_does_not_match_yields_no_rows_and_runs_nothing() {
        let (_dir, mut recorded) = record(&SyntheticPublisher::clean(50));
        recorded.manifest.sha256 = "0".repeat(64);
        let scratch = tempfile::tempdir().expect("scratch");
        let stub = StubRuleSet::stating(vec![on_mktdata("TEST.ONE", Outcome::Pass, 0, 49)]);
        let runs = std::rc::Rc::clone(&stub.runs);
        let err = judge(&recorded, stub, scratch.path()).expect_err("refused");
        assert!(matches!(err, JudgeError::Digest(_)), "{err}");
        assert!(runs.borrow().is_empty());
    }

    /// A rule set that could not run is an error and never a verdict.
    #[test]
    fn a_rule_set_that_could_not_run_yields_no_rows() {
        let (_dir, recorded) = record(&SyntheticPublisher::clean(50));
        let scratch = tempfile::tempdir().expect("scratch");
        let mut stub = StubRuleSet::stating(vec![on_mktdata("TEST.ONE", Outcome::Pass, 0, 49)]);
        stub.broken = true;
        let err = judge(&recorded, stub, scratch.path()).expect_err("refused");
        assert!(err.is_tool(), "{err}");
    }

    /// A manifest that disagrees with its own section about the drop scope is
    /// not judged: holes found at the wrong scope are not this object's.
    #[test]
    fn a_scope_the_manifest_and_the_section_disagree_on_is_refused() {
        let (_dir, mut recorded) = record(&SyntheticPublisher::clean(50));
        recorded.manifest.capture_drop_scope = "capture-handle".to_owned();
        let scratch = tempfile::tempdir().expect("scratch");
        let stub = StubRuleSet::stating(Vec::new());
        let err = judge(&recorded, stub, scratch.path()).expect_err("refused");
        assert!(matches!(err, JudgeError::ScopeDisagreement { .. }), "{err}");
    }

    /// The table's sort key, from the DDL that creates it, and the
    /// `ReplacingMergeTree(run_ts)` it keys: what survives a merge of the rows
    /// two runs wrote.
    fn merged(rows: &[ConformanceFinding]) -> Vec<ConformanceFinding> {
        let ddl =
            include_str!("../../../dz-recorder-clickhouse/db/clickhouse/001_recorder_rows.sql");
        let table = &ddl[ddl
            .find("CREATE TABLE IF NOT EXISTS recorder.conformance_finding")
            .expect("the table is in the DDL")..];
        let order = &table[table.find("ORDER BY (").expect("a sort key") + "ORDER BY (".len()..];
        let columns: Vec<&str> = order[..order.find(')').expect("closed")]
            .split(',')
            .map(str::trim)
            .collect();
        assert!(columns.contains(&"rule_set_version"), "{columns:?}");
        assert!(!columns.contains(&"run_ts"), "{columns:?}");

        let key = |row: &ConformanceFinding| {
            let value = serde_json::to_value(row).expect("a row serialises");
            columns
                .iter()
                .map(|c| {
                    value
                        .get(*c)
                        .unwrap_or_else(|| panic!("no column {c}"))
                        .to_string()
                })
                .collect::<Vec<_>>()
        };
        let mut kept: std::collections::BTreeMap<Vec<String>, ConformanceFinding> =
            std::collections::BTreeMap::new();
        for row in rows {
            let k = key(row);
            match kept.get(&k) {
                Some(have) if have.run_ts >= row.run_ts => {}
                _ => {
                    kept.insert(k, row.clone());
                }
            }
        }
        kept.into_values().collect()
    }

    fn rows_under(
        version: &str,
        run_ts: u64,
        recorded: &crate::conformance::testing::Recorded,
    ) -> Vec<ConformanceFinding> {
        let scratch = tempfile::tempdir().expect("scratch");
        let mut stub = StubRuleSet::stating(vec![on_mktdata("TEST.ONE", Outcome::Pass, 0, 49)]);
        stub.version = version.to_owned();
        let pinned = PinnedRuleSet::resolve(stub, version).expect("named");
        judge_object(&JudgeInput {
            object: &recorded.object,
            manifest: &recorded.manifest,
            rule_set: &pinned,
            rule_set_feed: "tob",
            scratch: scratch.path(),
            run_ts,
        })
        .expect("judged")
        .batch
        .conformance_finding
    }

    #[test]
    fn two_versions_over_one_window_both_stand_and_a_rerun_replaces_only_its_own() {
        let (_dir, recorded) = record(&SyntheticPublisher::clean(50));
        let old = rows_under("v1+aaa", 100, &recorded);
        let new = rows_under("v2+bbb", 200, &recorded);
        let old_again = rows_under("v1+aaa", 300, &recorded);

        // Two versions over one window: both rows stand.
        let both: Vec<_> = old.iter().chain(&new).cloned().collect();
        assert_eq!(merged(&both).len(), 2);

        // The same version again: one row, the later run's.
        let rerun: Vec<_> = old.iter().chain(&old_again).cloned().collect();
        let m = merged(&rerun);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].run_ts, Nanos(300));

        // An older version re-run after a newer one does not displace it.
        let all: Vec<_> = old.iter().chain(&new).chain(&old_again).cloned().collect();
        let m = merged(&all);
        assert_eq!(m.len(), 2);
        let newer = m
            .iter()
            .find(|r| r.rule_set_version == "v2+bbb")
            .expect("kept");
        assert_eq!(newer.run_ts, Nanos(200));
    }
}
