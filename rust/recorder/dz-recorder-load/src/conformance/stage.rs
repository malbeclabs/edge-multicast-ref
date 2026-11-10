//! One conformance pass: the objects of every judged feed that no rule set
//! version has judged yet, judged, their findings handed to a sink of their own,
//! and the ledger written once they land.
//!
//! **Separate from the load, and leaving it untouched.** The load pass and its
//! ledger answer whether an object's rows are in; this answers whether it has
//! been judged, under which version. The two run over the same directory and
//! share nothing else — not the sink, whose landed objects are what each
//! ledger records, and not the ledger. A configuration with no
//! `[[conformance]]` entry never constructs one of these.
//!
//! **A version bump does not re-judge the archive.** An object judged under any
//! version is not judged again by a pass; see
//! [`JudgeLedger::judged_under_any`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use dz_recorder_conformance::{PinnedRuleSet, RuleSet, ToolError};
use dz_recorder_rows::{ObjectId, RowSink};

use super::judge::{judge_object, JudgeInput};
use super::ledger::{JudgeEntry, JudgeLedger};
use super::ConformanceRefusal;
use crate::loader::{now_unix_nanos, now_unix_seconds, oldest_age, read_manifest_if_present};
use crate::metrics::{ErrorKind, LoaderMetrics};

const MANIFEST_SUFFIX: &str = ".manifest.json";

/// One feed to judge, with the rule set that judges it.
#[derive(Debug, Clone)]
pub struct JudgedFeed<R> {
    /// The manifest's feed, and the subdirectory of `objects_dir` it is under.
    pub feed: String,
    /// The rule set's own name for the feed.
    pub rule_set_feed: String,
    /// What the rule set must answer with.
    pub rule_set_version: String,
    pub rule_set: R,
}

/// An object whose findings the sink holds, not yet in the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldJudgement {
    pub id: ObjectId,
    pub feed: String,
    pub rule_set_version: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JudgePass {
    /// Objects judged and handed to the sink this pass.
    pub judged: u64,
    /// Objects whose judgement is now in the ledger.
    pub recorded: u64,
    pub failed: u64,
    /// Objects of judged feeds no version has judged, held ones included.
    pub unjudged: u64,
    pub held: u64,
    pub oldest_unjudged_age_seconds: i64,
}

pub struct Judge<'a, S: RowSink, R: RuleSet + Clone> {
    pub objects_dir: &'a Path,
    pub site: &'a str,
    pub recorder: &'a str,
    pub max_objects: usize,
    pub feeds: &'a [JudgedFeed<R>],
    pub scratch: &'a Path,
    pub ledger: &'a mut JudgeLedger,
    pub sink: &'a mut S,
    pub metrics: &'a LoaderMetrics,
    pub held: &'a mut Vec<HeldJudgement>,
}

struct Candidate {
    object: PathBuf,
    manifest_path: PathBuf,
    start_ns: u64,
}

impl<S: RowSink, R: RuleSet + Clone> Judge<'_, S, R> {
    /// Walks every judged feed once. Never returns an error for an object:
    /// every failure is counted, named in the returned messages and left
    /// unjudged.
    pub fn run_once(&mut self, stop: &dyn Fn() -> bool) -> (JudgePass, Vec<String>) {
        let mut pass = JudgePass::default();
        let mut errors = Vec::new();
        let mut present: HashSet<(String, String)> = HashSet::new();
        let mut unjudged: Vec<(ObjectId, u64)> = Vec::new();
        let mut complete = true;
        let mut budget_spent = false;

        for judged_feed in self.feeds {
            // Asked every pass, so a binary replaced under a running loader is
            // caught at the next pass rather than stamped on a month of rows.
            let pinned = match PinnedRuleSet::resolve(
                judged_feed.rule_set.clone(),
                &judged_feed.rule_set_version,
            ) {
                Ok(pinned) => Some(pinned),
                Err(e) => {
                    let reason = match e {
                        ToolError::VersionDisagrees { .. } => ConformanceRefusal::VersionDisagrees,
                        _ => ConformanceRefusal::VersionUnresolvable,
                    };
                    self.metrics.conformance_refused(reason, 1);
                    errors.push(format!("conformance {}: {e}", judged_feed.feed));
                    None
                }
            };

            let (candidates, read_whole) =
                self.candidates(&self.objects_dir.join(&judged_feed.feed), &mut errors);
            complete &= read_whole;
            let mut tool_failed = false;
            for candidate in candidates {
                let manifest = match read_manifest_if_present(&candidate.manifest_path) {
                    Ok(Some(manifest)) => manifest,
                    Ok(None) => continue,
                    Err(message) => {
                        self.metrics.error(ErrorKind::Manifest, now_unix_seconds());
                        errors.push(message);
                        complete = false;
                        continue;
                    }
                };
                if manifest.site != self.site
                    || manifest.recorder != self.recorder
                    || manifest.feed != judged_feed.feed
                {
                    continue;
                }
                let id = ObjectId {
                    key: manifest.object_key.clone(),
                    sha256: manifest.sha256.clone(),
                };
                present.insert((id.key.clone(), id.sha256.clone()));
                if self.ledger.judged_under_any(&id.key, &id.sha256) {
                    continue;
                }
                unjudged.push((id.clone(), manifest.end_ns));
                if self.held.iter().any(|h| h.id == id) {
                    continue;
                }
                let Some(pinned) = pinned.as_ref() else {
                    continue;
                };
                if tool_failed || budget_spent || stop() {
                    continue;
                }
                if self.max_objects != 0 && pass.judged + pass.failed >= self.max_objects as u64 {
                    budget_spent = true;
                    continue;
                }

                let judged = judge_object(&JudgeInput {
                    object: &candidate.object,
                    manifest: &manifest,
                    rule_set: pinned,
                    rule_set_feed: &judged_feed.rule_set_feed,
                    scratch: self.scratch,
                    run_ts: now_unix_nanos(),
                });
                let judged = match judged {
                    Ok(judged) => judged,
                    Err(e) => {
                        pass.failed += 1;
                        if e.is_tool() {
                            // The binary, not the object: every later object
                            // of this feed would fail the same way, so the
                            // feed waits for the next pass.
                            self.metrics
                                .conformance_refused(ConformanceRefusal::ToolCouldNotRun, 1);
                            tool_failed = true;
                        } else {
                            self.metrics.error(ErrorKind::Replay, now_unix_seconds());
                        }
                        errors.push(format!("conformance {}: {e}", judged_feed.feed));
                        continue;
                    }
                };
                self.metrics.conformance_refused(
                    ConformanceRefusal::NoInstance,
                    judged.refused.no_instance,
                );
                self.metrics.conformance_refused(
                    ConformanceRefusal::UndescribedInstance,
                    judged.refused.undescribed_instance,
                );
                self.metrics.conformance_refused(
                    ConformanceRefusal::ChannelUnresolved,
                    judged.refused.channel_unresolved,
                );
                for (rule, count) in &judged.refused.no_instance_by_rule {
                    self.metrics.conformance_unplaced(rule, *count);
                }

                match self.sink.write_batch(judged.batch, now_unix_nanos()) {
                    Ok(accepted) => {
                        pass.judged += 1;
                        self.metrics.bytes_posted(accepted.bytes_posted);
                        self.held.push(HeldJudgement {
                            id,
                            feed: judged_feed.feed.clone(),
                            rule_set_version: pinned.version().to_owned(),
                        });
                        pass.recorded += self.record_landed(&accepted.landed, &mut errors);
                    }
                    Err(e) => {
                        // The sink lost what it held: none of it is judged.
                        self.held.clear();
                        self.metrics.batch_failed();
                        self.metrics.error(ErrorKind::Sink, now_unix_seconds());
                        pass.failed += 1;
                        errors.push(format!("conformance sink: {e}"));
                        budget_spent = true;
                    }
                }
            }
        }

        match self.sink.post_if_due(now_unix_nanos()) {
            Ok(landed) => {
                self.metrics.bytes_posted(landed.bytes_posted);
                pass.recorded += self.record_landed(&landed.objects, &mut errors);
            }
            Err(e) => {
                self.held.clear();
                self.metrics.error(ErrorKind::Sink, now_unix_seconds());
                pass.failed += 1;
                errors.push(format!("conformance sink: {e}"));
            }
        }

        pass.held = self.held.len() as u64;
        let still: Vec<u64> = unjudged
            .iter()
            .filter(|(id, _)| !self.ledger.judged_under_any(&id.key, &id.sha256))
            .map(|(_, end)| *end)
            .collect();
        pass.unjudged = still.len() as u64;
        pass.oldest_unjudged_age_seconds = oldest_age(still.into_iter(), now_unix_seconds());

        // Only against a complete enumeration, as the load ledger is: a feed
        // that could not be read this pass must not lose its entries.
        if complete {
            let scope: HashSet<String> = self.feeds.iter().map(|f| f.feed.clone()).collect();
            if let Err(e) = self.ledger.compact(&present, &scope) {
                self.metrics.error(ErrorKind::Ledger, now_unix_seconds());
                errors.push(e.to_string());
            }
        }
        self.metrics
            .conformance_pass_finished(pass.unjudged as i64, pass.oldest_unjudged_age_seconds);
        (pass, errors)
    }

    /// Writes a ledger entry for every held object whose findings landed.
    /// Returns how many were recorded.
    pub fn record_landed(&mut self, landed: &[ObjectId], errors: &mut Vec<String>) -> u64 {
        record_judged(landed, self.held, self.ledger, self.metrics, errors)
    }

    /// The objects under one feed's directory, oldest first, and whether the
    /// directory was read whole. A directory that does not exist yet is a feed
    /// whose recorder has not published, and is whole.
    fn candidates(&self, dir: &Path, errors: &mut Vec<String>) -> (Vec<Candidate>, bool) {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (Vec::new(), true),
            Err(e) => {
                self.metrics.error(ErrorKind::Io, now_unix_seconds());
                errors.push(format!("{}: {e}", dir.display()));
                return (Vec::new(), false);
            }
        };
        let mut whole = true;
        let mut out = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    self.metrics.error(ErrorKind::Io, now_unix_seconds());
                    errors.push(format!("{}: {e}", dir.display()));
                    whole = false;
                    continue;
                }
            };
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(stem) = name.strip_suffix(MANIFEST_SUFFIX) else {
                continue;
            };
            // A manifest lands before its object; one with no object yet is
            // left for the next pass.
            let Some(object) = ["pcapng.zst", "pcapng"]
                .into_iter()
                .map(|suffix| dir.join(format!("{stem}.{suffix}")))
                .find(|p| p.is_file())
            else {
                continue;
            };
            out.push(Candidate {
                object,
                manifest_path: entry.path(),
                start_ns: stem
                    .split('-')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
            });
        }
        out.sort_by_key(|c| (c.start_ns, c.object.clone()));
        (out, whole)
    }
}

/// Records every held judgement whose findings are in `landed`. Shared with
/// the flush on the way out, which has no pass to call it from.
pub fn record_judged(
    landed: &[ObjectId],
    held: &mut Vec<HeldJudgement>,
    ledger: &mut JudgeLedger,
    metrics: &LoaderMetrics,
    errors: &mut Vec<String>,
) -> u64 {
    let mut recorded = 0;
    for id in landed {
        let Some(at) = held.iter().position(|h| &h.id == id) else {
            continue;
        };
        let h = held.remove(at);
        match ledger.record(JudgeEntry {
            object_key: h.id.key,
            object_sha256: h.id.sha256,
            rule_set_version: h.rule_set_version,
            feed: h.feed,
            judged_at_ns: now_unix_nanos(),
        }) {
            Ok(()) => {
                recorded += 1;
                metrics.conformance_judged();
            }
            Err(e) => {
                metrics.error(ErrorKind::Ledger, now_unix_seconds());
                errors.push(e.to_string());
            }
        }
    }
    recorded
}

#[cfg(test)]
mod tests {
    use dz_recorder_conformance::Outcome;
    use dz_recorder_replay::synthetic::SyntheticPublisher;
    use dz_recorder_rows::{FileSink, Grain};

    use super::*;
    use crate::conformance::judge::tests::{on_mktdata, StubRuleSet, VERSION};
    use crate::conformance::testing::{record_segments, FEED, RECORDER, SITE};
    use crate::ledger::Ledger;
    use crate::loader::Loader;

    struct Host {
        dir: tempfile::TempDir,
        completed: PathBuf,
    }

    /// A recorder's completed directory holding `objects` objects of one feed.
    fn host(objects: u64) -> Host {
        let dir = tempfile::tempdir().expect("a directory");
        let completed = dir.path().join("completed");
        record_segments(
            dir.path(),
            &completed.join(FEED),
            &SyntheticPublisher::clean(100),
            objects,
        );
        Host { dir, completed }
    }

    fn feed(rule_set: StubRuleSet) -> Vec<JudgedFeed<StubRuleSet>> {
        vec![JudgedFeed {
            feed: FEED.to_owned(),
            rule_set_feed: "tob".to_owned(),
            rule_set_version: VERSION.to_owned(),
            rule_set,
        }]
    }

    fn clean() -> StubRuleSet {
        StubRuleSet::stating(vec![on_mktdata("TEST.ONE", Outcome::Pass, 0, 99)])
    }

    /// One load pass into `rows`, the way the binary runs it.
    fn load(host: &Host, rows: &Path, metrics: &LoaderMetrics) {
        let mut ledger = Ledger::in_memory();
        let mut sink = FileSink::create(rows).expect("a sink");
        let mut pending = Vec::new();
        let (pass, errors) = Loader {
            objects_dir: &host.completed,
            feeds: None,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics,
            market_data: &[],
            pending: &mut pending,
        }
        .run_once(&|| false);
        assert!(errors.is_empty(), "{errors:?}");
        assert!(pass.derived > 0);
        sink.flush(0).expect("flushed");
    }

    /// One conformance pass, its findings into `rows`.
    fn judge_pass(
        host: &Host,
        rows: &Path,
        feeds: &[JudgedFeed<StubRuleSet>],
        ledger: &mut JudgeLedger,
        metrics: &LoaderMetrics,
        max_objects: usize,
    ) -> (JudgePass, Vec<String>) {
        let scratch = host.dir.path().join("scratch");
        std::fs::create_dir_all(&scratch).expect("scratch");
        let mut sink = FileSink::create(rows).expect("a sink");
        let mut held = Vec::new();
        let out = Judge {
            objects_dir: &host.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects,
            feeds,
            scratch: &scratch,
            ledger,
            sink: &mut sink,
            metrics,
            held: &mut held,
        }
        .run_once(&|| false);
        sink.flush(0).expect("flushed");
        out
    }

    fn findings(rows: &Path) -> usize {
        std::fs::read_to_string(FileSink::path_in(rows, Grain::ConformanceFinding))
            .map(|t| t.lines().count())
            .unwrap_or(0)
    }

    /// The datagram rows a load writes are the same bytes whether or not a
    /// feed is judged beside it; the only thing judging adds is the findings
    /// file, and a configuration that judges nothing adds nothing at all.
    #[test]
    fn judging_changes_nothing_but_the_findings() {
        let host = host(2);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let alone = host.dir.path().join("alone");
        let beside = host.dir.path().join("beside");
        let nothing = host.dir.path().join("nothing");

        load(&host, &alone, &metrics);

        load(&host, &beside, &metrics);
        let mut ledger = JudgeLedger::in_memory();
        let (pass, errors) = judge_pass(&host, &beside, &feed(clean()), &mut ledger, &metrics, 0);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.judged, 2);

        load(&host, &nothing, &metrics);
        let mut ledger = JudgeLedger::in_memory();
        let (pass, _) = judge_pass(&host, &nothing, &[], &mut ledger, &metrics, 0);
        assert_eq!(pass, JudgePass::default(), "no feed, no work");

        for grain in Grain::ALL {
            if grain == Grain::ConformanceFinding {
                continue;
            }
            assert_eq!(
                std::fs::read(FileSink::path_in(&alone, grain)).ok(),
                std::fs::read(FileSink::path_in(&beside, grain)).ok(),
                "{grain} differs with a feed judged beside the load"
            );
        }
        assert_eq!(findings(&alone), 0);
        assert_eq!(
            findings(&nothing),
            0,
            "a configuration that says nothing judges nothing"
        );
        assert_eq!(findings(&beside), 2, "one rule, one instance, two objects");
    }

    /// Judged once, recorded once, and not again — not on the next pass, and
    /// not when the rule set's version moves: re-judging the archive is a
    /// deliberate act, never a deploy's side effect.
    #[test]
    fn an_object_is_judged_once_and_a_version_bump_does_not_rejudge_the_archive() {
        let host = host(2);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::open(host.dir.path().join("judged.jsonl")).expect("ledger");

        let (pass, _) = judge_pass(&host, &rows, &feed(clean()), &mut ledger, &metrics, 0);
        assert_eq!((pass.judged, pass.recorded, pass.unjudged), (2, 2, 0));
        assert_eq!(ledger.entries(), 2);

        let (pass, _) = judge_pass(&host, &rows, &feed(clean()), &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 0, "already judged");

        let mut bumped = clean();
        bumped.version = "v9+fff".to_owned();
        let mut feeds = feed(bumped);
        feeds[0].rule_set_version = "v9+fff".to_owned();
        let (pass, _) = judge_pass(&host, &rows, &feeds, &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 0, "a new pin judges new objects only");
        assert_eq!(findings(&rows), 2);
    }

    /// A tool that answers with another version judges nothing, says so, and
    /// leaves the objects unjudged — counted in the lag, not hidden.
    #[test]
    fn a_rule_set_that_disagrees_about_its_version_judges_nothing() {
        let host = host(2);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut feeds = feed(clean());
        feeds[0].rule_set_version = "v0+configured".to_owned();
        let (pass, errors) = judge_pass(&host, &rows, &feeds, &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 0);
        assert_eq!(pass.unjudged, 2);
        assert!(pass.oldest_unjudged_age_seconds > 0);
        assert!(errors[0].contains("v0+configured"), "{errors:?}");
        assert_eq!(findings(&rows), 0);
        let text = metrics.render();
        assert!(
            text.contains("dz_loader_conformance_refused_total{reason=\"version_disagrees\",recorder=\"recorder-1\",site=\"site-1\"} 1"),
            "{text}"
        );
        assert!(text.contains(
            "dz_loader_conformance_unjudged_objects{recorder=\"recorder-1\",site=\"site-1\"} 2"
        ));
    }

    /// A rule set that cannot run writes nothing for the object, records
    /// nothing, and the feed waits for the next pass rather than failing every
    /// object behind it the same way.
    #[test]
    fn a_rule_set_that_could_not_run_leaves_the_feed_unjudged() {
        let host = host(3);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut broken = clean();
        broken.broken = true;
        let runs = std::rc::Rc::clone(&broken.runs);
        let (pass, errors) = judge_pass(&host, &rows, &feed(broken), &mut ledger, &metrics, 0);
        assert_eq!((pass.judged, pass.failed, pass.unjudged), (0, 1, 3));
        assert_eq!(runs.borrow().len(), 1, "one attempt, not one per object");
        assert_eq!(ledger.entries(), 0);
        assert_eq!(findings(&rows), 0, "never a table of unverifiable");
        assert!(!errors.is_empty());
        assert!(metrics.render().contains(
            "dz_loader_conformance_refused_total{reason=\"tool_could_not_run\",recorder=\"recorder-1\",site=\"site-1\"} 1"
        ));
    }

    /// The pass bound is the load's: a runner far behind still publishes its
    /// lag every pass.
    #[test]
    fn a_pass_is_bounded_and_the_rest_wait() {
        let host = host(3);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let (pass, _) = judge_pass(&host, &rows, &feed(clean()), &mut ledger, &metrics, 2);
        assert_eq!((pass.judged, pass.unjudged), (2, 1));
        let (pass, _) = judge_pass(&host, &rows, &feed(clean()), &mut ledger, &metrics, 2);
        assert_eq!((pass.judged, pass.unjudged), (1, 0));
    }

    /// Verdicts the rule set placed on no instance are counted by rule.
    #[test]
    fn an_unplaced_verdict_is_counted_by_rule() {
        let host = host(1);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut channel_wide = on_mktdata("TEST.CHANNEL_WIDE", Outcome::Pass, 0, 99);
        channel_wide.instance = None;
        let stub = StubRuleSet::stating(vec![
            on_mktdata("TEST.ONE", Outcome::Pass, 0, 99),
            channel_wide,
        ]);
        let (pass, _) = judge_pass(&host, &rows, &feed(stub), &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 1);
        assert_eq!(findings(&rows), 1);
        let text = metrics.render();
        assert!(text.contains(
            "dz_loader_conformance_unplaced_total{recorder=\"recorder-1\",rule_id=\"TEST.CHANNEL_WIDE\",site=\"site-1\"} 1"
        ), "{text}");
        assert!(text.contains(
            "dz_loader_conformance_refused_total{reason=\"no_instance\",recorder=\"recorder-1\",site=\"site-1\"} 1"
        ));
    }
}
