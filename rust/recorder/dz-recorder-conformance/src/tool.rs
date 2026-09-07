//! The boundary against the specification's rule set, as a type.
//!
//! The judgement comes from a process this repository does not build: the rule
//! set lives in `edge-feed-spec`, is written in Go, and is pinned by commit.
//! Everything above this module is therefore written against [`RuleSet`] and a
//! recorded report, and only this module knows there is a binary at all.
//!
//! # The three exit codes are three different things
//!
//! The tool's own contract is 0 for *found no violations*, 1 for *found one*
//! and 2 for *could not run*, and the three must never collapse into two.
//!
//! - **0 and 1 both yield a report.** Neither is a verdict: exit 0 means found
//!   no violations, which includes *evaluated nothing* — a `-snapshot-port` set
//!   but wrong evaluates zero snapshot datagrams and still exits 0, and there
//!   is a checked-in negative control in `dz-recorder-e2e` that exists to
//!   demonstrate it. The verdicts come from the report's entries and from
//!   nowhere else.
//! - **2 is an error, and it must not become a table full of `unverifiable`.**
//!   `unverifiable` is a statement the rule set made about the *traffic*; a tool
//!   that did not start is a statement about *us*. Writing the first where the
//!   second happened would move the `unverifiable` share panel — the panel that
//!   measures how often the archive opens the gate — every time a binary went
//!   missing. The object stays unjudged and the absence of rows is the honest
//!   record.
//! - **An exit the runner cannot interpret is an error**, and so is a report it
//!   cannot parse or one the tool did not write. Exit 0 with no report is *not*
//!   an empty set of passes.
//!
//! # The version is asked of the binary, never read from a file
//!
//! `rule_set_version` is in the findings table's sort key so that one window can
//! legally hold two verdicts from two versions, which is only worth anything if
//! the value is a fact. A version taken from configuration is a claim: two
//! builds of one tag, a local patch, a tag moved. So [`PinnedRuleSet::resolve`]
//! asks the tool which rule set it is before anything is judged, and a tool that
//! cannot say — or that says something other than the configured value — is a
//! refusal that names both.
//!
//! # A report is held against the invocation that asked for it
//!
//! The report states the feed it is about, and [`RuleSet::judge`] checks it
//! against the feed it asked for rather than believing it. This is the version
//! disagreement again in a second column: a report about another feed parses as
//! well as the right one, so the mismatch would survive every check below this
//! seam and arrive as rows filed under a feed whose traffic they do not
//! describe. Both refusals name both values, because *which of the two is
//! wrong* is a question only an operator can answer.

use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Command;

use dz_edge_core::PortRole;

use crate::report::{ReportError, RuleSetReport};

/// Found no violations. Not a verdict: it also covers *evaluated nothing*.
pub const EXIT_CLEAN: i32 = 0;
/// Stated at least one violation.
pub const EXIT_VIOLATION: i32 = 1;
/// Could not run at all — a statement about us and not about the traffic.
pub const EXIT_COULD_NOT_RUN: i32 = 2;

/// The ports the three roles were joined on, as the tool's flags take them.
///
/// `Option` per role and not a bare port, because *joined* and *joined on this
/// port* are different facts and both differ from *never joined*. The tool warns
/// about a starved rule only when the flag is unset, so a role nobody joined has
/// to arrive here as `None`: setting it to a port nothing was joined on turns a
/// rule that never ran into a clean one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PortRoles {
    mktdata: Option<u16>,
    refdata: Option<u16>,
    snapshot: Option<u16>,
}

impl PortRoles {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            mktdata: None,
            refdata: None,
            snapshot: None,
        }
    }

    /// The same set with one role's port stated.
    #[must_use]
    pub const fn with(mut self, role: PortRole, port: u16) -> Self {
        match role {
            PortRole::Mktdata => self.mktdata = Some(port),
            PortRole::Refdata => self.refdata = Some(port),
            PortRole::Snapshot => self.snapshot = Some(port),
        }
        self
    }

    #[must_use]
    pub const fn port(&self, role: PortRole) -> Option<u16> {
        match role {
            PortRole::Mktdata => self.mktdata,
            PortRole::Refdata => self.refdata,
            PortRole::Snapshot => self.snapshot,
        }
    }
}

/// One run of the rule set: one capture file, one group, one feed.
///
/// One group and not several, because the tool takes one `-group`. An archive
/// holding two groups is two invocations over two files, and the per-object
/// process count follows from what the recorder was asked to join rather than
/// from anything the runner chose.
#[derive(Debug, Clone, Copy)]
pub struct Invocation<'a> {
    pub pcap: &'a Path,
    pub group: Ipv4Addr,
    /// The feed specification's name, as the manifest states it.
    pub feed: &'a str,
    pub ports: PortRoles,
}

/// Why nothing may be written for this object.
///
/// Every variant names the tool, because the operator reading it has to know
/// which binary to go and look at, and because a refusal that named only the
/// object would send them to the archive instead.
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("{tool} could not be run at all: {detail}")]
    NotStarted { tool: String, detail: String },
    #[error(
        "{tool} exited {}, which means it could not run: {stderr}",
        EXIT_COULD_NOT_RUN
    )]
    CouldNotRun { tool: String, stderr: String },
    /// Neither 0, 1 nor 2, or no code at all because the process was signalled.
    /// Refused rather than read as the nearest known code: a contract this
    /// runner does not understand is not one it may guess at.
    #[error("{tool} exited {code}, which is not a code this runner can interpret: {stderr}")]
    UnknownExit {
        tool: String,
        code: String,
        stderr: String,
    },
    /// The tool exited within its contract and then said nothing this runner
    /// can read. Never an empty set of passes.
    #[error("{tool} exited {code} and wrote no report this runner can read: {source}")]
    Unreportable {
        tool: String,
        code: i32,
        #[source]
        source: ReportError,
    },
    #[error("{tool} exited {code} and wrote no report at {path}")]
    ReportMissing {
        tool: String,
        code: i32,
        path: PathBuf,
    },
    #[error("{tool} cannot say which rule set it is: {detail}")]
    VersionUnresolvable { tool: String, detail: String },
    #[error(
        "{tool} is rule set {reported}, and the configuration says {configured}; a verdict \
         stamped with either would be attributed to a rule set that did not produce it"
    )]
    VersionDisagrees {
        tool: String,
        configured: String,
        reported: String,
    },
    /// The report names a feed other than the one the tool was invoked for.
    /// The same shape as [`ToolError::VersionDisagrees`] and for the same
    /// reason: a finding filed under the wrong name is a finding about
    /// something nobody asked about.
    #[error(
        "{tool} was run for feed {asked} and its report is for feed {reported}; a finding \
         attributed to either would be a finding about a feed nobody asked about"
    )]
    FeedDisagrees {
        tool: String,
        asked: String,
        reported: String,
    },
}

/// What a rule set can be asked.
///
/// Two methods rather than one: the plan describes the seam as *given a pcap, a
/// group, the three ports and a feed, return a report*, and the design requires
/// besides that the runner ask the tool which rule set it is. Resolving the
/// version anywhere but through this same trait would leave the refusal that
/// matters most — a tool that cannot name itself — with nothing to stand in for
/// it in a test.
pub trait RuleSet {
    /// Runs the rule set over one group's capture file.
    fn judge(&self, invocation: &Invocation<'_>) -> Result<RuleSetReport, ToolError>;

    /// Which rule set this is, as the rule set itself states it.
    fn rule_set_version(&self) -> Result<String, ToolError>;

    /// How to name this rule set in a refusal.
    fn describe(&self) -> String;
}

/// What one run of the tool did, before anything is made of it.
///
/// The raw exit code and the raw standard error, interpreted by nobody. The
/// conformance gate in `dz-recorder-e2e` asserts on exactly these, and it must
/// go on asserting on exactly these: a gate that shared the runner's
/// interpretation of an exit code could not catch the runner interpreting it
/// wrongly.
#[derive(Debug, Clone)]
pub struct ToolRun {
    /// `None` when the process was signalled rather than exiting.
    pub code: Option<i32>,
    pub stderr: String,
}

/// The rule set as the binary `edge-feed-spec` builds.
///
/// The path is stated and never searched for. A runner that found the tool on
/// `PATH` would stamp its verdicts with whatever rule set happened to be
/// installed, which is the failure `magic` is required to prevent one layer
/// down.
#[derive(Debug, Clone)]
pub struct ConformanceTool {
    path: PathBuf,
    scratch: PathBuf,
}

impl ConformanceTool {
    /// `scratch` is the runner's own writable path, and never a directory the
    /// recorder's staging budget has to reach: a file eviction cannot classify,
    /// sitting beside the objects it has to delete, is a way to lose the
    /// archive.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>, scratch: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            scratch: scratch.into(),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs the tool and returns what it did, uninterpreted.
    ///
    /// `report` is where the tool is asked to write its per-rule report, or
    /// `None` for a caller that wants only the exit code.
    pub fn run(
        &self,
        invocation: &Invocation<'_>,
        report: Option<&Path>,
    ) -> Result<ToolRun, ToolError> {
        let mut cmd = Command::new(&self.path);
        cmd.arg("-feed")
            .arg(invocation.feed)
            .arg("-pcap")
            .arg(invocation.pcap)
            .arg("-group")
            .arg(invocation.group.to_string());
        for role in [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot] {
            // Unset and not zero for a role nobody joined: the tool reads an
            // unset flag as *this role was not offered* and warns about the
            // rules it starved, which is the only signal that distinguishes a
            // silent port from a clean one.
            if let Some(port) = invocation.ports.port(role) {
                cmd.arg(format!("-{}-port", role.as_str()))
                    .arg(port.to_string());
            }
        }
        if let Some(report) = report {
            cmd.arg("-json-report").arg(report);
        }

        let out = cmd.output().map_err(|e| ToolError::NotStarted {
            tool: self.describe(),
            detail: e.to_string(),
        })?;
        Ok(ToolRun {
            code: out.status.code(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

impl RuleSet for ConformanceTool {
    fn judge(&self, invocation: &Invocation<'_>) -> Result<RuleSetReport, ToolError> {
        // A directory of its own per invocation, so that a report left behind
        // by an earlier run can never be read as this one's. The tool writes
        // the file in one `WriteFile`, so a report that is present is a report
        // this run wrote.
        let dir = tempfile::Builder::new()
            .prefix("dz-conformance-")
            .tempdir_in(&self.scratch)
            .map_err(|e: io::Error| ToolError::NotStarted {
                tool: self.describe(),
                detail: format!("no scratch directory under {}: {e}", self.scratch.display()),
            })?;
        let report_path = dir.path().join("report.json");

        let run = self.run(invocation, Some(&report_path))?;
        let code = match run.code {
            Some(EXIT_CLEAN) => EXIT_CLEAN,
            Some(EXIT_VIOLATION) => EXIT_VIOLATION,
            Some(EXIT_COULD_NOT_RUN) => {
                return Err(ToolError::CouldNotRun {
                    tool: self.describe(),
                    stderr: run.stderr,
                })
            }
            other => {
                return Err(ToolError::UnknownExit {
                    tool: self.describe(),
                    code: other.map_or_else(|| "on a signal".to_owned(), |c| c.to_string()),
                    stderr: run.stderr,
                })
            }
        };

        // Read only now, and only for a code within the contract. Reading it
        // first would let a run that could not start be reported out of a file
        // it never wrote.
        let bytes = std::fs::read(&report_path).map_err(|_| ToolError::ReportMissing {
            tool: self.describe(),
            code,
            path: report_path.clone(),
        })?;
        let report =
            RuleSetReport::from_json(&bytes).map_err(|source| ToolError::Unreportable {
                tool: self.describe(),
                code,
                source,
            })?;

        // The report names the feed it is about, and it is checked rather than
        // trusted. A report that answers about another feed parses exactly as
        // well as the right one, so nothing downstream could tell: every row
        // derived from it would be filed under the feed the manifest states
        // while describing traffic from somewhere else.
        if report.feed != invocation.feed {
            return Err(ToolError::FeedDisagrees {
                tool: self.describe(),
                asked: invocation.feed.to_owned(),
                reported: report.feed,
            });
        }
        Ok(report)
    }

    fn rule_set_version(&self) -> Result<String, ToolError> {
        let out = Command::new(&self.path)
            .arg("--version")
            .output()
            .map_err(|e| ToolError::VersionUnresolvable {
                tool: self.describe(),
                detail: e.to_string(),
            })?;
        if out.status.code() != Some(EXIT_CLEAN) {
            return Err(ToolError::VersionUnresolvable {
                tool: self.describe(),
                detail: format!(
                    "--version exited {}",
                    out.status
                        .code()
                        .map_or_else(|| "on a signal".to_owned(), |c| c.to_string())
                ),
            });
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        let mut lines = stdout.lines().map(str::trim).filter(|l| !l.is_empty());
        let version = lines.next().ok_or_else(|| ToolError::VersionUnresolvable {
            tool: self.describe(),
            detail: "--version printed nothing".to_owned(),
        })?;
        // One line, and exactly one. A binary that answered with a paragraph is
        // one whose first line this runner would be choosing to believe, and a
        // rule set version chosen rather than read is the claim this whole
        // resolution exists to avoid.
        if lines.next().is_some() {
            return Err(ToolError::VersionUnresolvable {
                tool: self.describe(),
                detail: format!("--version printed more than one line: {stdout:?}"),
            });
        }
        Ok(version.to_owned())
    }

    fn describe(&self) -> String {
        format!("dz-conformance at {}", self.path.display())
    }
}

/// A rule set that has said which rule set it is, and agreed with the
/// configuration.
///
/// The only way to hold a rule set version in this crate, and deliberately: the
/// value stamped on every row is the one the binary answered with, so a runner
/// cannot be constructed that stamps a version nobody asked the tool about.
#[derive(Debug, Clone)]
pub struct PinnedRuleSet<R> {
    rule_set: R,
    version: String,
}

impl<R: RuleSet> PinnedRuleSet<R> {
    /// Asks the rule set which one it is, and refuses unless it is the one
    /// configured.
    ///
    /// Both refusals produce no rows at all. An unattributable verdict is worse
    /// than an absent one: it sits in the same column as the attributable ones
    /// and quietly breaks every comparison across versions, which is the one
    /// comparison this table exists to make.
    pub fn resolve(rule_set: R, configured: &str) -> Result<Self, ToolError> {
        let reported = rule_set.rule_set_version()?;
        if reported != configured {
            return Err(ToolError::VersionDisagrees {
                tool: rule_set.describe(),
                configured: configured.to_owned(),
                reported,
            });
        }
        Ok(Self {
            rule_set,
            version: reported,
        })
    }

    /// The value every row this rule set produces is stamped with.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn judge(&self, invocation: &Invocation<'_>) -> Result<RuleSetReport, ToolError> {
        self.rule_set.judge(invocation)
    }
}
