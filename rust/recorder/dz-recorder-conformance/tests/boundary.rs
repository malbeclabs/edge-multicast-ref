//! The boundary against the rule set, held against a stand-in for the tool and
//! against recorded reports.
//!
//! No `dz-conformance` here, and none needed: what is under test is what this
//! repository does with an exit code and a file, and every one of those cases
//! is a case the real tool can produce. The stand-in is a script that exits
//! with a stated code and copies a stated report into place, which is exactly
//! the surface the real binary presents.
//!
//! Every stand-in is written once, before the first of these tests runs a
//! child. Writing an executable and running it from a threaded process is a
//! race with a name — `ETXTBSY`, because a fork elsewhere in the process holds
//! the write handle open across the window — and it shows up as a rule set that
//! could not run, which is exactly the failure several of these tests assert.
//! A flake that mimics the thing under test is worse than a slow suite, so the
//! writing all happens up front and the tests only ever exec.
//!
//! The reports are files in `tests/fixtures`, in the shape the design asks
//! `edge-feed-spec` for: one entry per rule evaluated, naming the rule, the
//! outcome, the channel instance and the sequence range its evidence lies in.
//! **Nothing here parses standard error**, and there is no test that does,
//! because there is no code that does.

#![cfg(unix)]

use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use dz_edge_core::PortRole;
use dz_recorder_conformance::report::{Outcome, ReportError, RuleSetReport};
use dz_recorder_conformance::tool::{
    ConformanceTool, Invocation, PinnedRuleSet, PortRoles, RuleSet, ToolError,
};
use tempfile::TempDir;

const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const VERSION: &str = "0.4.2+e68184bc25e25f1a9f0c26c465fdaf3bb5f23268";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// What the stand-in leaves where `-json-report` pointed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Report {
    /// Nothing at all, which the tool does when it fails before it can write.
    None,
    Clean,
    Violation,
    /// A file that is not a report: the log line somebody would have been
    /// tempted to parse.
    Garbage,
}

impl Report {
    const ALL: [Self; 4] = [Self::None, Self::Clean, Self::Violation, Self::Garbage];

    fn token(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Clean => "clean",
            Self::Violation => "violation",
            Self::Garbage => "garbage",
        }
    }

    /// Where the stand-in copies this report from.
    ///
    /// Everything is staged under [`STAGED`] rather than read out of
    /// `tests/fixtures` directly, so that every report path handed to the
    /// stand-in has a space in it. The path is interpolated into a shell
    /// script, and an unquoted one splits into two words: without this the
    /// quoting would be tested only on a checkout under `/home/some one/`, and
    /// the failure it produces — an empty file where a report should be — is
    /// one several of these tests already assert for other reasons.
    fn source(self, dir: &Path) -> Option<PathBuf> {
        let staged = dir.join(STAGED);
        match self {
            Self::None => None,
            Self::Clean => Some(staged.join("report-clean.json")),
            Self::Violation => Some(staged.join("report-violation.json")),
            Self::Garbage => Some(staged.join("garbage.json")),
        }
    }
}

/// The directory the stand-ins' reports are staged in, named on purpose.
const STAGED: &str = "reports with a space";

/// One shell word that is exactly `s`, whatever is in it.
///
/// Single quotes and not none: a fixture path is a workspace path, and a
/// workspace path may hold a space or a `$`. A stand-in that failed for that
/// reason would fail as a report that could not be read, which is a verdict
/// several of these tests are here to assert about the boundary rather than
/// about the harness.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// What the stand-in answers `--version` with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version {
    /// One line, as the tool's own contract states.
    OneLine,
    /// A banner and then a version, which is not an answer.
    Paragraph,
}

impl Version {
    const ALL: [Self; 2] = [Self::OneLine, Self::Paragraph];

    fn token(self) -> &'static str {
        match self {
            Self::OneLine => "one-line",
            Self::Paragraph => "paragraph",
        }
    }

    fn stdout(self) -> String {
        match self {
            Self::OneLine => VERSION.to_owned(),
            Self::Paragraph => format!("dz-conformance\n{VERSION}"),
        }
    }
}

/// Which stand-in a test wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FakeSpec {
    exit: i32,
    version: Version,
    version_exit: i32,
    report: Report,
}

impl Default for FakeSpec {
    fn default() -> Self {
        Self {
            exit: 0,
            version: Version::OneLine,
            version_exit: 0,
            report: Report::None,
        }
    }
}

impl FakeSpec {
    const EXITS: [i32; 4] = [0, 1, 2, 3];
    const VERSION_EXITS: [i32; 2] = [0, 2];

    fn name(self) -> String {
        format!(
            "stand-in-{}-{}-{}-{}",
            self.exit,
            self.version.token(),
            self.version_exit,
            self.report.token()
        )
    }
}

/// Every stand-in this suite can ask for, written before any of it runs.
struct StandIns {
    _dir: TempDir,
    dir: PathBuf,
}

impl StandIns {
    fn build() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let at = dir.path().to_path_buf();
        let staged = at.join(STAGED);
        std::fs::create_dir(&staged).expect("somewhere to stage the reports");
        for name in ["report-clean.json", "report-violation.json"] {
            std::fs::copy(fixture(name), staged.join(name)).expect("the fixture is readable");
        }
        std::fs::write(
            staged.join("garbage.json"),
            b"time=12:00 level=INFO msg=\"5 rules evaluated\"\n",
        )
        .expect("a file that is not a report");

        for exit in FakeSpec::EXITS {
            for version in Version::ALL {
                for version_exit in FakeSpec::VERSION_EXITS {
                    for report in Report::ALL {
                        let spec = FakeSpec {
                            exit,
                            version,
                            version_exit,
                            report,
                        };
                        Self::write(&at, spec);
                    }
                }
            }
        }
        Self { _dir: dir, dir: at }
    }

    /// The stand-in reads its own arguments back to the directory the pcap is
    /// in, so that one script serves every test without any of them sharing a
    /// file.
    fn write(at: &Path, spec: FakeSpec) {
        let copy = spec.report.source(at).map_or_else(String::new, |src| {
            format!(
                "[ -n \"$out\" ] && cat {} > \"$out\"\n",
                sh_quote(&src.display().to_string())
            )
        });
        let script = format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then\n\
             \x20 printf '%s' {version_stdout}\n\
             \x20 exit {version_exit}\n\
             fi\n\
             prev=\"\"\n\
             out=\"\"\n\
             pcap=\"\"\n\
             for a in \"$@\"; do\n\
             \x20 [ \"$prev\" = \"-json-report\" ] && out=\"$a\"\n\
             \x20 [ \"$prev\" = \"-pcap\" ] && pcap=\"$a\"\n\
             \x20 prev=\"$a\"\n\
             done\n\
             argv=\"$(dirname \"$pcap\")/argv\"\n\
             : > \"$argv\"\n\
             for a in \"$@\"; do printf '%s\\n' \"$a\" >> \"$argv\"; done\n\
             {copy}\
             exit {exit}\n",
            version_stdout = sh_quote(&spec.version.stdout()),
            version_exit = spec.version_exit,
            copy = copy,
            exit = spec.exit,
        );
        let path = at.join(spec.name());
        std::fs::write(&path, script).expect("the stand-in is writable");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("and executable");
    }
}

static STAND_INS: LazyLock<StandIns> = LazyLock::new(StandIns::build);

/// One test's own directory, and the stand-in it points the boundary at.
struct FakeTool {
    dir: TempDir,
    tool: ConformanceTool,
}

impl FakeTool {
    fn new(spec: &FakeSpec) -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let scratch = dir.path().join("scratch");
        std::fs::create_dir(&scratch).expect("the runner's own writable path");
        let tool = ConformanceTool::new(STAND_INS.dir.join(spec.name()), &scratch);
        Self { dir, tool }
    }

    /// The arguments the last run was given, in order.
    fn argv(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.path().join("argv"))
            .expect("the stand-in recorded its arguments")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn pcap(&self) -> PathBuf {
        let path = self.dir.path().join("replayed.pcap");
        std::fs::write(&path, b"not read by the stand-in").expect("a file to point at");
        path
    }
}

fn ports() -> PortRoles {
    PortRoles::none()
        .with(PortRole::Mktdata, 40_000)
        .with(PortRole::Refdata, 40_001)
        .with(PortRole::Snapshot, 40_002)
}

/// A rule set that is asked nothing about traffic, for the version refusals.
struct StubRuleSet {
    version: Result<String, ()>,
}

impl RuleSet for StubRuleSet {
    fn judge(&self, _invocation: &Invocation<'_>) -> Result<RuleSetReport, ToolError> {
        panic!("nothing may be judged before the version resolves");
    }

    fn rule_set_version(&self) -> Result<String, ToolError> {
        self.version
            .clone()
            .map_err(|()| ToolError::VersionUnresolvable {
                tool: self.describe(),
                detail: "--version printed nothing".to_owned(),
            })
    }

    fn describe(&self) -> String {
        "dz-conformance at /nowhere/dz-conformance".to_owned()
    }
}

#[test]
fn an_exit_of_two_is_an_error_and_yields_no_report_at_all() {
    // The case the whole error type exists for. Exit 2 is *could not run*: a
    // statement about us and not about the traffic. Turning it into a table of
    // `unverifiable` rows would move the panel that measures how often the
    // archive opens the gate, every time a binary went missing.
    //
    // The stand-in leaves a clean report behind on its way out, so a boundary
    // that read the file before it read the exit code would report two passes
    // over a run that never happened.
    let fake = FakeTool::new(&FakeSpec {
        exit: 2,
        report: Report::Clean,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();
    let tool_path = fake.tool.path().display().to_string();

    let err = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect_err("exit 2 is not a report");

    assert!(
        matches!(err, ToolError::CouldNotRun { .. }),
        "and it is its own category, not an unknown exit: {err}"
    );
    assert!(
        err.to_string().contains(&tool_path),
        "the refusal names the tool, so an operator knows which binary to look at: {err}"
    );
}

#[test]
fn an_exit_of_one_yields_the_report_it_wrote() {
    // 0 and 1 are both reports, and the difference between them is in the
    // entries rather than in the code. A boundary that treated 1 as a failure
    // would drop every violation the rule set ever stated.
    let fake = FakeTool::new(&FakeSpec {
        exit: 1,
        report: Report::Violation,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();

    let report = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect("exit 1 is a report");

    assert_eq!(report.feed, "mbp");
    assert_eq!(
        report
            .rules
            .iter()
            .map(|r| (r.rule_id.as_str(), r.outcome))
            .collect::<Vec<_>>(),
        vec![
            ("MBP.DELTA.ABSOLUTE_APPLY", Outcome::Violation),
            ("MBP.SNAP.GROUP_STRUCTURE", Outcome::Pass),
        ]
    );
    let violation = &report.rules[0];
    assert_eq!(
        violation
            .instance
            .expect("the entry names an instance")
            .source,
        Ipv4Addr::new(192, 0, 2, 1),
        "which is what places the row, and is read rather than guessed"
    );
    assert_eq!(
        violation
            .evidence
            .expect("and the range its evidence lies in")
            .first_seq,
        1
    );
}

#[test]
fn an_exit_of_zero_yields_the_report_and_not_an_inferred_pass() {
    let fake = FakeTool::new(&FakeSpec {
        exit: 0,
        report: Report::Clean,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();

    let report = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect("exit 0 is a report");

    assert_eq!(report.rules.len(), 2);
    assert!(report.rules.iter().all(|r| r.outcome == Outcome::Pass));
    assert!(
        report.rules.iter().all(|r| r.instance.is_some()),
        "every pass names the instance it passed on: a pass nobody can place is a pass \
         nobody can check"
    );
}

#[test]
fn an_unparseable_report_is_an_error_and_never_an_empty_set_of_passes() {
    // The tempting failure: a report that will not parse, read as *no
    // violations stated*, written out as a clean window. The exit code says 0
    // and the file says nothing, and the two together must produce no row.
    let fake = FakeTool::new(&FakeSpec {
        exit: 0,
        report: Report::Garbage,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();
    let tool_path = fake.tool.path().display().to_string();

    let err = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect_err("a report that will not parse is not a clean run");

    assert!(
        matches!(err, ToolError::Unreportable { code: 0, .. }),
        "{err}"
    );
    assert!(err.to_string().contains(&tool_path), "{err}");
}

#[test]
fn an_exit_within_the_contract_with_no_report_is_an_error() {
    // Not the same failure as an unparseable one and worth its own case: the
    // tool exited cleanly and wrote nothing, which is indistinguishable from a
    // clean feed to anything that infers a verdict from a code.
    let fake = FakeTool::new(&FakeSpec {
        exit: 0,
        report: Report::None,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();

    let err = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect_err("exit 0 and no report is not a clean run");

    assert!(
        matches!(err, ToolError::ReportMissing { code: 0, .. }),
        "{err}"
    );
}

#[test]
fn an_exit_outside_the_contract_is_refused_rather_than_rounded() {
    // A code this runner does not understand is not one it may read as the
    // nearest one it does. The tool's contract may grow a fourth code, and the
    // day it does the honest answer is that this build cannot interpret it.
    let fake = FakeTool::new(&FakeSpec {
        exit: 3,
        report: Report::Clean,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();

    let err = fake
        .tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect_err("an unknown code is not a report");

    assert!(matches!(err, ToolError::UnknownExit { .. }), "{err}");
    assert!(err.to_string().contains('3'));
}

#[test]
fn a_tool_that_is_not_there_is_an_error_that_names_it() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let tool = ConformanceTool::new(dir.path().join("absent-dz-conformance"), dir.path());
    let pcap = dir.path().join("replayed.pcap");
    std::fs::write(&pcap, b"").expect("a file to point at");

    let err = tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: ports(),
        })
        .expect_err("a binary that is not there judges nothing");

    assert!(matches!(err, ToolError::NotStarted { .. }), "{err}");
    assert!(err.to_string().contains("absent-dz-conformance"));
}

#[test]
fn a_role_nobody_joined_leaves_its_flag_unset() {
    // A port role that was never joined must not be handed a port. The tool
    // warns about the rules it starved only when the flag is *unset*, so a zero
    // there turns a rule that never ran into one that ran and found nothing
    // wrong — the vacuity the snapshot negative control exists to rule out.
    let fake = FakeTool::new(&FakeSpec {
        exit: 0,
        report: Report::Clean,
        ..FakeSpec::default()
    });
    let pcap = fake.pcap();

    fake.tool
        .judge(&Invocation {
            pcap: &pcap,
            group: GROUP,
            feed: "mbp",
            ports: PortRoles::none().with(PortRole::Mktdata, 40_000),
        })
        .expect("the run is a run either way");

    let argv = fake.argv();
    assert!(argv.contains(&"-mktdata-port".to_owned()));
    assert!(
        !argv.contains(&"-snapshot-port".to_owned()),
        "and no flag at all for a role nobody joined: {argv:?}"
    );
    assert!(!argv.contains(&"-refdata-port".to_owned()), "{argv:?}");
    assert!(
        argv.contains(&"-json-report".to_owned()),
        "the report is asked for by the same invocation that judges: {argv:?}"
    );
    assert!(
        argv.contains(&GROUP.to_string()),
        "one group, named: {argv:?}"
    );
}

#[test]
fn a_version_that_cannot_be_resolved_produces_no_rows_and_names_the_tool() {
    // A verdict that cannot say which rule set produced it is unattributable,
    // and an unattributable verdict is worse than an absent one: it sits in the
    // same column as the attributable ones and quietly breaks every comparison
    // across versions. So the refusal happens before anything is judged, which
    // is why the stub panics if it is.
    let Err(err) = PinnedRuleSet::resolve(StubRuleSet { version: Err(()) }, VERSION) else {
        panic!("a rule set that cannot name itself judges nothing");
    };

    assert!(
        matches!(err, ToolError::VersionUnresolvable { .. }),
        "{err}"
    );
    assert!(err.to_string().contains("dz-conformance"), "{err}");
}

#[test]
fn a_version_that_disagrees_with_the_configuration_names_both() {
    // Configured against one rule set and running another is a refusal and not
    // a warning, for the reason the row deriver refuses an object whose digest
    // does not match its manifest: a finding drawn from bytes nobody checked is
    // a finding about a file rather than about a feed.
    let reported = "0.4.2+017aa581d2748adc028174af02a2582235b16005";
    let Err(err) = PinnedRuleSet::resolve(
        StubRuleSet {
            version: Ok(reported.to_owned()),
        },
        VERSION,
    ) else {
        panic!("two versions is a refusal");
    };

    let message = err.to_string();
    assert!(
        matches!(err, ToolError::VersionDisagrees { .. }),
        "{message}"
    );
    assert!(
        message.contains(reported),
        "the one the tool reported: {message}"
    );
    assert!(
        message.contains(VERSION),
        "and the one configured: {message}"
    );
}

#[test]
fn the_resolved_version_is_the_one_the_tool_stated() {
    let fake = FakeTool::new(&FakeSpec::default());

    let pinned = PinnedRuleSet::resolve(fake.tool, VERSION).expect("the versions agree");

    assert_eq!(pinned.version(), VERSION);
}

#[test]
fn a_tool_that_answers_version_with_a_paragraph_resolves_to_nothing() {
    // One line, and exactly one. A binary that answered with a banner and then
    // a version would have its first line believed, and a rule set version
    // chosen rather than read is the claim this resolution exists to avoid.
    let fake = FakeTool::new(&FakeSpec {
        version: Version::Paragraph,
        ..FakeSpec::default()
    });

    let Err(err) = PinnedRuleSet::resolve(fake.tool, VERSION) else {
        panic!("two lines is not an answer");
    };

    assert!(
        matches!(err, ToolError::VersionUnresolvable { .. }),
        "{err}"
    );
}

#[test]
fn a_tool_that_fails_its_own_version_flag_resolves_to_nothing() {
    // It prints a plausible version and then exits non-zero, which is the case
    // that separates *asked and answered* from *asked*. A run that failed is
    // not one whose output may be believed, and believing it here would stamp
    // every row of the pass with a version the binary did not stand behind.
    let fake = FakeTool::new(&FakeSpec {
        version_exit: 2,
        ..FakeSpec::default()
    });

    let Err(err) = PinnedRuleSet::resolve(fake.tool, VERSION) else {
        panic!("a non-zero --version is not an answer");
    };

    assert!(
        matches!(err, ToolError::VersionUnresolvable { .. }),
        "{err}"
    );
}

#[test]
fn a_report_format_this_runner_does_not_know_is_refused() {
    // Read leniently, a later format's entries would mean what this one's
    // fields mean, which is how a verdict acquires a meaning nobody stated.
    let bytes = std::fs::read(fixture("report-unknown-format.json")).expect("the fixture");

    let err = RuleSetReport::from_json(&bytes).expect_err("a format this runner cannot read");

    assert!(
        matches!(err, ReportError::UnknownFormat { found: 2 }),
        "{err}"
    );
}

#[test]
fn an_entry_may_name_no_instance_and_no_range_and_still_parse() {
    // Both are `Option` and both absences are real: a rule that did not run
    // names no instance, and one whose evidence is not in the sequence space
    // cites no range. Refusing the report over either would discard every other
    // entry in it — and what to do about the absence is a judgement above this
    // seam, not a parse failure at it.
    let bytes = std::fs::read(fixture("report-partial.json")).expect("the fixture");

    let report = RuleSetReport::from_json(&bytes).expect("the report parses");

    assert_eq!(report.rules.len(), 3);
    assert_eq!(report.rules[0].outcome, Outcome::Unverifiable);
    assert!(report.rules[0].evidence.is_none());
    assert_eq!(report.rules[1].outcome, Outcome::Na);
    assert!(
        report.rules[1].instance.is_none(),
        "and an entry naming no instance survives the parse so that the runner can refuse \
         it deliberately rather than never seeing it"
    );
    assert_eq!(
        report.rules[2].outcome,
        Outcome::Suspected,
        "the rule set's own fifth word, carried rather than flattened into one of the \
         table's four"
    );
    assert_eq!(
        report.rules[2].evidence.expect("a range").reset_count,
        2,
        "a sequence range means nothing without the era it was read under"
    );
}

#[test]
fn an_unknown_field_does_not_break_the_parse() {
    // The rule set is another repository's and it will grow fields. A runner
    // that refused the report over one would stop judging on the day upstream
    // added a number nobody here has to read.
    let bytes = std::fs::read(fixture("report-partial.json")).expect("the fixture");
    let text = String::from_utf8(bytes).expect("the fixture is text");

    assert!(
        text.contains("cycles_confirmed"),
        "the fixture carries a field this crate has no name for"
    );
    assert!(RuleSetReport::from_json(text.as_bytes()).is_ok());
}
