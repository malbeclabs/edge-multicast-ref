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

use super::findings::Refused;
use super::judge::{judge_object, JudgeInput};
use super::ledger::{JudgeEntry, JudgeLedger};
use super::{ConformanceRefusal, KeptVerdicts};
use crate::loader::{
    now_unix_nanos, now_unix_seconds, oldest_age, read_manifest_if_present, take_turns,
};
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
    /// Which verdicts become rows.
    pub verdicts: KeptVerdicts,
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
    /// Failed attempts per object, carried across passes. In memory: a
    /// restart gives every object its attempts again, which costs at most
    /// [`GIVE_UP_AFTER`] more runs of each.
    pub failures: &'a mut std::collections::HashMap<ObjectId, u32>,
}

/// Attempts an object gets before the runner gives up on it and records it
/// as refused. Without a bound, an object that makes the rule set fail sorts
/// first in its feed on every pass and holds every object behind it back until
/// it is evicted.
pub const GIVE_UP_AFTER: u32 = 3;

struct Candidate {
    object: PathBuf,
    manifest_path: PathBuf,
    start_ns: u64,
}

/// One object a pass may judge.
struct Work {
    feed_index: usize,
    object: PathBuf,
    start_ns: u64,
    manifest: dz_recorder_archive::SegmentManifest,
    id: ObjectId,
}

impl<S: RowSink, R: RuleSet + Clone> Judge<'_, S, R> {
    /// Walks every judged feed once. Never returns an error for an object:
    /// every failure is counted in `failed`, named in the returned messages and
    /// left unjudged.
    pub fn run_once(&mut self, stop: &dyn Fn() -> bool) -> (JudgePass, Vec<String>) {
        let mut pass = JudgePass::default();
        let mut errors = Vec::new();
        let mut present: HashSet<(String, String)> = HashSet::new();
        let mut unjudged: Vec<(ObjectId, u64)> = Vec::new();
        let mut complete = true;

        // 1. Which rule set each feed is judged by this pass. Asked every pass,
        //    so a binary replaced under a running loader is caught at the next
        //    pass rather than stamped on a month of rows. A feed whose rule set
        //    cannot be pinned is a failure of the pass, not of an object.
        let mut pinned: Vec<Option<PinnedRuleSet<R>>> = Vec::with_capacity(self.feeds.len());
        for judged_feed in self.feeds {
            match PinnedRuleSet::resolve(
                judged_feed.rule_set.clone(),
                &judged_feed.rule_set_version,
            ) {
                Ok(p) => pinned.push(Some(p)),
                Err(e) => {
                    let reason = match e {
                        ToolError::VersionDisagrees { .. } => ConformanceRefusal::VersionDisagrees,
                        _ => ConformanceRefusal::VersionUnresolvable,
                    };
                    self.metrics.conformance_refused(reason, 1);
                    pass.failed += 1;
                    errors.push(format!("conformance {}: {e}", judged_feed.feed));
                    pinned.push(None);
                }
            }
        }

        // 2. The scan, over every feed and every object, whole: the lag it
        //    publishes is the whole backlog, not the part a bounded pass reached.
        let mut work: Vec<Work> = Vec::new();
        for (feed_index, judged_feed) in self.feeds.iter().enumerate() {
            let (candidates, read_whole) =
                self.candidates(&self.objects_dir.join(&judged_feed.feed), &mut errors);
            if !read_whole {
                complete = false;
                pass.failed += 1;
            }
            for candidate in candidates {
                let manifest = match read_manifest_if_present(&candidate.manifest_path) {
                    Ok(Some(manifest)) => manifest,
                    // Evicted between the listing and the read.
                    Ok(None) => continue,
                    Err(message) => {
                        self.metrics.error(ErrorKind::Manifest, now_unix_seconds());
                        errors.push(message);
                        pass.failed += 1;
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
                if self.held.iter().any(|h| h.id == id) || pinned[feed_index].is_none() {
                    continue;
                }
                work.push(Work {
                    feed_index,
                    object: candidate.object,
                    start_ns: candidate.start_ns,
                    manifest,
                    id,
                });
            }
        }

        // 3. Oldest first within a feed, the feeds taking turns: one feed's
        //    backlog never holds another's objects back until they are evicted.
        //    The same order the load takes.
        work.sort_by(|a, b| (a.start_ns, &a.object).cmp(&(b.start_ns, &b.object)));
        let order = take_turns(
            work.iter()
                .enumerate()
                .map(|(i, w)| (i, self.feeds[w.feed_index].feed.clone()))
                .collect(),
        );
        // A feed stops for the pass when its binary cannot start, or when two
        // objects in a row make it fail: one failure is the object's, two in
        // a row is more likely the tool's.
        let mut tool_failed = vec![false; self.feeds.len()];
        let mut last_failed_on_object = vec![false; self.feeds.len()];
        let mut attempted = 0usize;
        for index in order {
            if stop() {
                break;
            }
            // Against the objects attempted, which is the work done; a feed
            // whose rule set could not be pinned took none of it.
            if self.max_objects != 0 && attempted >= self.max_objects {
                break;
            }
            let w = &work[index];
            if tool_failed[w.feed_index] {
                continue;
            }
            let (Some(pinned), Some(judged_feed)) =
                (pinned[w.feed_index].as_ref(), self.feeds.get(w.feed_index))
            else {
                continue;
            };
            attempted += 1;
            let judged = match judge_object(&JudgeInput {
                object: &w.object,
                manifest: &w.manifest,
                rule_set: pinned,
                rule_set_feed: &judged_feed.rule_set_feed,
                verdicts: judged_feed.verdicts,
                scratch: self.scratch,
                run_ts: now_unix_nanos(),
            }) {
                Ok(judged) => judged,
                Err(e) => {
                    pass.failed += 1;
                    errors.push(format!("conformance {}: {e}", judged_feed.feed));
                    if e.tool_cannot_start() {
                        // The binary, not the object: every object of this
                        // feed would fail the same way, so the feed waits for
                        // the next pass and no object is charged with it.
                        self.metrics
                            .conformance_refused(ConformanceRefusal::ToolCouldNotRun, 1);
                        tool_failed[w.feed_index] = true;
                        continue;
                    }
                    if e.is_tool() {
                        self.metrics
                            .conformance_refused(ConformanceRefusal::ToolFailedOnObject, 1);
                        if last_failed_on_object[w.feed_index] {
                            tool_failed[w.feed_index] = true;
                        }
                        last_failed_on_object[w.feed_index] = true;
                    } else {
                        self.metrics.error(ErrorKind::Replay, now_unix_seconds());
                    }
                    self.charge(
                        w,
                        judged_feed,
                        pinned.version(),
                        &e.to_string(),
                        &mut pass,
                        &mut errors,
                    );
                    continue;
                }
            };
            last_failed_on_object[w.feed_index] = false;
            self.failures.remove(&w.id);
            self.count_refusals(&judged.refused);

            match self.sink.write_batch(judged.batch, now_unix_nanos()) {
                Ok(accepted) => {
                    pass.judged += 1;
                    self.metrics.bytes_posted(accepted.bytes_posted);
                    self.held.push(HeldJudgement {
                        id: w.id.clone(),
                        feed: judged_feed.feed.clone(),
                        rule_set_version: pinned.version().to_owned(),
                    });
                    let (recorded, failed) = self.record_landed(&accepted.landed, &mut errors);
                    pass.recorded += recorded;
                    pass.failed += failed;
                }
                Err(e) => {
                    // The sink lost what it held: none of it is judged, and
                    // nothing more goes to it this pass.
                    self.held.clear();
                    self.metrics.batch_failed();
                    self.metrics.error(ErrorKind::Sink, now_unix_seconds());
                    pass.failed += 1;
                    errors.push(format!("conformance sink: {e}"));
                    break;
                }
            }
        }

        match self.sink.post_if_due(now_unix_nanos()) {
            Ok(landed) => {
                self.metrics.bytes_posted(landed.bytes_posted);
                let (recorded, failed) = self.record_landed(&landed.objects, &mut errors);
                pass.recorded += recorded;
                pass.failed += failed;
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
        // that could not be read this pass must not lose its entries. Not a
        // failure when it does not happen: the ledger is still correct, only
        // longer than it needs to be.
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

    /// Counts one failed attempt against an object, and gives up on it once it
    /// has had [`GIVE_UP_AFTER`]: it is recorded in the ledger as refused, with
    /// the last reason, so it is skipped from then on — across restarts — and
    /// counted, rather than retried first in its feed on every pass.
    fn charge(
        &mut self,
        w: &Work,
        judged_feed: &JudgedFeed<R>,
        version: &str,
        reason: &str,
        pass: &mut JudgePass,
        errors: &mut Vec<String>,
    ) {
        let attempts = self.failures.entry(w.id.clone()).or_insert(0);
        *attempts += 1;
        if *attempts < GIVE_UP_AFTER {
            return;
        }
        let attempts = *attempts;
        self.failures.remove(&w.id);
        match self.ledger.record(JudgeEntry {
            object_key: w.id.key.clone(),
            object_sha256: w.id.sha256.clone(),
            rule_set_version: version.to_owned(),
            feed: judged_feed.feed.clone(),
            judged_at_ns: now_unix_nanos(),
            refused: Some(format!(
                "gave up after {attempts} failed attempts: {reason}"
            )),
        }) {
            Ok(()) => {
                self.metrics
                    .conformance_refused(ConformanceRefusal::GivenUp, 1);
                errors.push(format!(
                    "conformance {}: gave up on {} after {attempts} failed attempts",
                    judged_feed.feed, w.id.key
                ));
            }
            Err(e) => {
                pass.failed += 1;
                self.metrics.error(ErrorKind::Ledger, now_unix_seconds());
                errors.push(e.to_string());
            }
        }
    }

    fn count_refusals(&self, refused: &Refused) {
        for (reason, count) in [
            (ConformanceRefusal::NoInstance, refused.no_instance),
            (
                ConformanceRefusal::UndescribedInstance,
                refused.undescribed_instance,
            ),
            (
                ConformanceRefusal::ChannelUnresolved,
                refused.channel_unresolved,
            ),
            (ConformanceRefusal::Overflow, refused.overflow),
        ] {
            self.metrics.conformance_refused(reason, count);
        }
        for (rule, count) in &refused.no_instance_by_rule {
            self.metrics.conformance_unplaced(rule, *count);
        }
    }

    /// Writes a ledger entry for every held object whose findings landed.
    /// Returns how many were recorded and how many could not be.
    pub fn record_landed(&mut self, landed: &[ObjectId], errors: &mut Vec<String>) -> (u64, u64) {
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

/// Records every held judgement whose findings are in `landed`, returning how
/// many were recorded and how many could not be. Shared with the flush on the
/// way out, which has no pass to call it from.
///
/// An entry that could not be written is a failure: the findings are in the
/// store and the runner has forgotten it put them there, so the object is
/// judged again — which the table makes a replace, and which is still work the
/// operator was told was done.
pub fn record_judged(
    landed: &[ObjectId],
    held: &mut Vec<HeldJudgement>,
    ledger: &mut JudgeLedger,
    metrics: &LoaderMetrics,
    errors: &mut Vec<String>,
) -> (u64, u64) {
    let mut recorded = 0;
    let mut failed = 0;
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
            refused: None,
        }) {
            Ok(()) => {
                recorded += 1;
                metrics.conformance_judged();
            }
            Err(e) => {
                failed += 1;
                metrics.error(ErrorKind::Ledger, now_unix_seconds());
                errors.push(e.to_string());
            }
        }
    }
    (recorded, failed)
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
            FEED,
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
            verdicts: KeptVerdicts::All,
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
        judge_pass_carrying(
            host,
            rows,
            feeds,
            ledger,
            metrics,
            max_objects,
            &mut std::collections::HashMap::new(),
        )
    }

    /// The same, with the failed attempts carried in from earlier passes.
    fn judge_pass_carrying(
        host: &Host,
        rows: &Path,
        feeds: &[JudgedFeed<StubRuleSet>],
        ledger: &mut JudgeLedger,
        metrics: &LoaderMetrics,
        max_objects: usize,
        failures: &mut std::collections::HashMap<ObjectId, u32>,
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
            failures,
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

    /// Under the default, a clean object writes no row and is still judged:
    /// its empty batch lands, the ledger records it, and the next pass does
    /// not judge it again. A violation beside it is the only row written.
    #[test]
    fn by_default_a_clean_object_writes_no_row_and_is_still_judged_once() {
        let clean_host = host(2);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = clean_host.dir.path().join("rows");
        let mut ledger =
            JudgeLedger::open(clean_host.dir.path().join("judged.jsonl")).expect("ledger");
        let mut feeds = feed(clean());
        feeds[0].verdicts = KeptVerdicts::default();

        let (pass, errors) = judge_pass(&clean_host, &rows, &feeds, &mut ledger, &metrics, 0);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!((pass.judged, pass.recorded, pass.unjudged), (2, 2, 0));
        assert_eq!(ledger.entries(), 2, "a clean object is ledgered");
        assert_eq!(findings(&rows), 0, "a pass writes no row");

        let (pass, _) = judge_pass(&clean_host, &rows, &feeds, &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 0, "and is not judged again");

        let violating = host(1);
        let rows = violating.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut feeds = feed(StubRuleSet::stating(vec![
            on_mktdata("TEST.ONE", Outcome::Pass, 0, 99),
            on_mktdata("TEST.TWO", Outcome::Violation, 0, 99),
        ]));
        feeds[0].verdicts = KeptVerdicts::default();
        let (pass, _) = judge_pass(&violating, &rows, &feeds, &mut ledger, &metrics, 0);
        assert_eq!(pass.recorded, 1);
        assert_eq!(findings(&rows), 1, "only the violation");
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
        assert_eq!(
            pass.failed, 1,
            "a pass that judged nothing it was asked to is a failed one"
        );
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
        assert_eq!((pass.judged, pass.failed, pass.unjudged), (0, 2, 3));
        assert_eq!(
            runs.borrow().len(),
            2,
            "two objects in a row, then the feed waits: not one attempt per object"
        );
        assert_eq!(ledger.entries(), 0);
        assert_eq!(findings(&rows), 0, "never a table of unverifiable");
        assert!(!errors.is_empty());
        assert!(metrics.render().contains(
            "dz_loader_conformance_refused_total{reason=\"tool_failed_on_object\",recorder=\"recorder-1\",site=\"site-1\"} 2"
        ));
    }

    /// One object that makes the rule set fail does not hold back the
    /// objects behind it: the next is judged in the same pass.
    #[test]
    fn an_object_the_rule_set_fails_on_does_not_stall_the_feed() {
        let host = host(3);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut first_fails = clean();
        first_fails.fail_runs.insert(0);
        let (pass, _) = judge_pass(&host, &rows, &feed(first_fails), &mut ledger, &metrics, 0);
        assert_eq!((pass.judged, pass.failed, pass.unjudged), (2, 1, 1));
        assert_eq!(findings(&rows), 2);
    }

    /// A binary that cannot start is nothing to do with any object: the feed
    /// stops at the first attempt, and no object is charged with it.
    #[test]
    fn a_binary_that_cannot_start_stops_the_feed_at_once() {
        let host = host(3);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut missing = clean();
        missing.not_started = true;
        let runs = std::rc::Rc::clone(&missing.runs);
        let mut failures = std::collections::HashMap::new();
        let (pass, _) = judge_pass_carrying(
            &host,
            &rows,
            &feed(missing),
            &mut ledger,
            &metrics,
            0,
            &mut failures,
        );
        assert_eq!(runs.borrow().len(), 1);
        assert_eq!((pass.judged, pass.failed, pass.unjudged), (0, 1, 3));
        assert!(failures.is_empty(), "no object is charged with the binary");
    }

    /// An object that fails every attempt is given up on after three, recorded
    /// as refused, and stops being tried — on this runner and the next.
    #[test]
    fn an_object_that_always_fails_is_given_up_on_after_three_attempts() {
        let host = host(3);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let ledger_path = host.dir.path().join("judged.jsonl");
        let mut ledger = JudgeLedger::open(&ledger_path).expect("ledger");
        // Runs 0, 3 and 4 are the same object's attempts: it is first in
        // every pass, and the other two are judged in the first.
        let mut poisoned = clean();
        poisoned.fail_runs.extend([0, 3, 4]);
        let runs = std::rc::Rc::clone(&poisoned.runs);
        let feeds = feed(poisoned);
        let mut failures = std::collections::HashMap::new();
        let mut last = JudgePass::default();
        for _ in 0..3 {
            last = judge_pass_carrying(
                &host,
                &rows,
                &feeds,
                &mut ledger,
                &metrics,
                0,
                &mut failures,
            )
            .0;
        }
        assert_eq!(runs.borrow().len(), 5);
        assert_eq!(last.unjudged, 0, "given up on, so no longer waiting");
        assert!(failures.is_empty());

        let reopened = JudgeLedger::open(&ledger_path).expect("reopened");
        assert_eq!(reopened.entries(), 3);
        let (pass, _) = judge_pass_carrying(
            &host,
            &rows,
            &feeds,
            &mut ledger,
            &metrics,
            0,
            &mut failures,
        );
        assert_eq!(runs.borrow().len(), 5, "not tried again");
        assert_eq!(pass.failed, 0);
        assert!(metrics.render().contains(
            "dz_loader_conformance_refused_total{reason=\"given_up\",recorder=\"recorder-1\",site=\"site-1\"} 1"
        ));
        assert_eq!(
            findings(&rows),
            2,
            "no finding vouches for the refused object"
        );
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

    /// A manifest that cannot be read, and a judgement that cannot be written
    /// to the ledger, are failures of the pass: `--once` must not exit clean
    /// over work it did not do or could not persist.
    #[test]
    fn a_manifest_it_cannot_read_and_a_ledger_it_cannot_write_are_failures() {
        let host = host(1);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let dir = host.completed.join(FEED);
        std::fs::write(dir.join("1-2-9.manifest.json"), "not json").expect("a bad manifest");
        std::fs::write(dir.join("1-2-9.pcapng.zst"), "").expect("its object");

        let ledger_path = host.dir.path().join("judged.jsonl");
        let mut ledger = JudgeLedger::open(&ledger_path).expect("ledger");
        // The ledger's file becomes something nothing can be appended to.
        std::fs::create_dir_all(&ledger_path).expect("a directory where the file was");

        let (pass, errors) = judge_pass(&host, &rows, &feed(clean()), &mut ledger, &metrics, 0);
        assert_eq!(pass.judged, 1, "the readable object was judged");
        assert_eq!(pass.recorded, 0, "and could not be recorded");
        assert_eq!(pass.failed, 2, "one manifest, one ledger entry: {errors:?}");
    }

    /// Two feeds and a budget of two: each feed gets a turn, oldest first
    /// within it, however deep the first feed's backlog. The second feed's
    /// objects sort after all of the first's, so a pass that simply walked
    /// them in order would spend the whole budget on the first.
    #[test]
    fn the_feeds_take_turns_within_the_budget() {
        let host = host(3);
        record_segments(
            host.dir.path(),
            &host.completed.join("zz-sorted-last"),
            "zz-sorted-last",
            &SyntheticPublisher::clean(100),
            1,
        );
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let rows = host.dir.path().join("rows");
        let mut ledger = JudgeLedger::in_memory();
        let mut feeds = feed(clean());
        feeds.push(JudgedFeed {
            feed: "zz-sorted-last".to_owned(),
            rule_set_feed: "tob".to_owned(),
            rule_set_version: VERSION.to_owned(),
            rule_set: clean(),
            verdicts: KeptVerdicts::All,
        });
        let first_runs = std::rc::Rc::clone(&feeds[0].rule_set.runs);
        let second_runs = std::rc::Rc::clone(&feeds[1].rule_set.runs);
        let (pass, _) = judge_pass(&host, &rows, &feeds, &mut ledger, &metrics, 2);
        assert_eq!(pass.judged, 2);
        assert_eq!(first_runs.borrow().len(), 1, "one turn for the deep feed");
        assert_eq!(second_runs.borrow().len(), 1, "and one for the other");
        assert_eq!(pass.unjudged, 2, "the rest wait, and are counted");
    }
}
