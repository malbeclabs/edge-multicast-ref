//! The join itself, and the report it produces.
//!
//! Both sides are grouped by [`JoinKey`] and the groups are matched. Nothing
//! here looks at order, position, proximity or time: two messages are the same
//! message when their keys are equal, and are two different messages otherwise.
//! That is what makes this a join rather than the heuristic alignment a
//! time-based comparison would have to be — and the reason `Per-Instrument Seq`
//! is stamped by the runtime, once, where both copies inherit it.

use std::collections::{BTreeMap, BTreeSet};

use dz_adapter_core::Adapter;
use dz_recorder_core::Source;

use crate::archive::PayloadArchive;
use crate::diff::diff;
use crate::error::RelowerError;
use crate::finding::{Caveat, Finding, Outcome};
use crate::join::JoinKey;
use crate::refdata::MissingDefinition;
use crate::relower::{relower, LoweredMessage, ParseFailure, ReLowered, Refusal};
use crate::wire::{MessageBody, Skipped, WireCapture, WireMessage};

/// How much of each of the four outcomes the join reached.
///
/// [`identical`](Self::identical) is the fourth: present in both, every compared
/// field equal, framing and pacing not compared. It is a count and never a
/// finding — and it is the number that says whether a clean report checked
/// anything at all. A report with no findings and `identical == 0` compared
/// nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Joinable messages the archive held.
    pub on_wire: usize,
    /// Joinable messages the re-lowering produced.
    pub re_lowered: usize,
    /// Distinct join keys either side named.
    pub keys: usize,
    /// Present in both, every compared field equal. **The healthy case.**
    pub identical: usize,
    /// Present in both, at least one field differing.
    pub fields_differ: usize,
    /// Re-lowered and not on the wire.
    pub re_lowered_not_on_wire: usize,
    /// On the wire and not re-lowered.
    pub on_wire_not_re_lowered: usize,
}

impl Summary {
    /// How many of one outcome the join reached.
    #[must_use]
    pub const fn of(&self, outcome: Outcome) -> usize {
        match outcome {
            Outcome::ReLoweredNotOnWire => self.re_lowered_not_on_wire,
            Outcome::OnWireNotReLowered => self.on_wire_not_re_lowered,
            Outcome::FieldsDiffer => self.fields_differ,
            Outcome::IdenticalDifferentTiming => self.identical,
        }
    }
}

/// What one comparison concluded.
///
/// The findings are what is wrong. Everything else is what a reader needs in
/// order to trust them: the caveats say where the evidence is thin, the refusals
/// and the missing definitions say where an absence is ours rather than the
/// publisher's, and the summary says how much was actually checked.
#[derive(Debug, Clone, Default)]
pub struct RelowerReport {
    /// Every finding, ordered by join key — which is instrument, then sequence
    /// or timestamp. Deterministic, because a report that reorders itself
    /// between two runs over one archive cannot be diffed against itself.
    pub findings: Vec<Finding>,
    pub summary: Summary,
    /// Everything the comparison could not establish, or had to choose. Read
    /// these before the findings.
    pub caveats: Vec<Caveat>,
    /// Events the re-lowering refused. Each one explains an
    /// [`Finding::OnWireNotReLowered`] that is not the publisher's fault.
    pub refusals: Vec<Refusal>,
    /// Payloads the adapter refused, exactly as the publisher's own adapter did.
    pub parse_failures: Vec<ParseFailure>,
    /// Instruments the adapter offered that the archive holds no definition
    /// for.
    pub missing_definitions: Vec<MissingDefinition>,
    /// What the wire side read and did not join.
    pub skipped: Skipped,
}

impl RelowerReport {
    /// Whether the comparison found nothing wrong.
    ///
    /// Not the same as *the publisher is correct*: read
    /// [`caveats`](Self::caveats) and [`Summary::identical`] too. A window whose
    /// reference data was incomplete can be clean because almost nothing was
    /// compared.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }

    /// Findings of one outcome.
    pub fn findings_of(&self, outcome: Outcome) -> impl Iterator<Item = &Finding> {
        self.findings
            .iter()
            .filter(move |finding| finding.outcome() == outcome)
    }
}

/// Join a multicast capture against a re-lowered stream.
///
/// Neither argument can be a live anything: [`WireCapture`] is built from an
/// archive and [`ReLowered`] from an archive of upstream payloads and that
/// capture's own reference data.
#[must_use]
pub fn compare(wire: &WireCapture, re_lowered: &ReLowered) -> RelowerReport {
    let mut by_key: BTreeMap<JoinKey, Sides<'_>> = BTreeMap::new();
    for message in wire.messages() {
        by_key
            .entry(message.body.join_key())
            .or_default()
            .on_wire
            .push(message);
    }
    for message in re_lowered.messages() {
        by_key
            .entry(message.body.join_key())
            .or_default()
            .re_lowered
            .push(message);
    }

    let mut report = RelowerReport {
        caveats: wire.caveats(),
        refusals: re_lowered.refusals().to_vec(),
        parse_failures: re_lowered.parse_failures().to_vec(),
        missing_definitions: re_lowered.missing_definitions().to_vec(),
        skipped: wire.skipped(),
        ..RelowerReport::default()
    };
    report.caveats.extend(re_lowered.caveats().iter().cloned());
    report.summary.on_wire = wire.messages().len();
    report.summary.re_lowered = re_lowered.messages().len();
    report.summary.keys = by_key.len();

    for (key, sides) in &by_key {
        if sides.on_wire.len() > 1 || sides.re_lowered.len() > 1 {
            // Declared rather than resolved. Which copy of an indistinguishable
            // pair is matched with which is arbitrary, so a field difference at
            // an ambiguous key may be an artefact of the pairing.
            report.caveats.push(Caveat::AmbiguousJoinKey {
                key: *key,
                on_wire: sides.on_wire.len(),
                re_lowered: sides.re_lowered.len(),
            });
        }

        let paired = sides.on_wire.len().min(sides.re_lowered.len());
        for (wire_message, re_message) in sides
            .on_wire
            .iter()
            .zip(sides.re_lowered.iter())
            .take(paired)
        {
            let fields = diff(&wire_message.body, &re_message.body);
            if fields.is_empty() {
                // The fourth outcome, and the reason this tool is usable: the
                // datagram that carried it, its position in it, its sequence
                // number and its send timestamp all differ freely, and none of
                // them is in the comparison.
                report.summary.identical += 1;
                continue;
            }
            report.summary.fields_differ += 1;
            report.findings.push(Finding::FieldsDiffer {
                key: *key,
                message_type: wire_message.body.message_type(),
                fields,
                wire: wire_message.provenance,
                re_lowered: re_message.provenance,
            });
        }

        for wire_message in sides.on_wire.iter().skip(paired) {
            report.summary.on_wire_not_re_lowered += 1;
            report.findings.push(Finding::OnWireNotReLowered {
                key: *key,
                message_type: wire_message.body.message_type(),
                wire: wire_message.provenance,
            });
        }
        for re_message in sides.re_lowered.iter().skip(paired) {
            report.summary.re_lowered_not_on_wire += 1;
            report.findings.push(Finding::ReLoweredNotOnWire {
                key: *key,
                message_type: re_message.body.message_type(),
                re_lowered: re_message.provenance,
            });
        }
    }

    report.caveats.extend(era_caveats(wire));
    report
}

/// Read both archives, re-lower, and join.
///
/// The whole of Mode C, for the ordinary case of one multicast archive holding
/// one feed's port roles. A caller whose `mktdata` and `refdata` roles are in
/// separate archives builds the [`WireCapture`] itself with one
/// [`WireCapture::absorb`] per source, and calls [`relower`] and [`compare`]
/// directly.
///
/// `expected_magic` is required, because `Magic` is the only thing that stops a
/// datagram misrouted from a sibling feed being parsed at the wrong layout.
///
/// # Errors
///
/// [`RelowerError`], every variant of which means there is no comparison to
/// report rather than a comparison that found nothing.
pub fn compare_archives<A, P, S>(
    adapter: &mut A,
    payloads: &mut P,
    multicast: &mut S,
    expected_magic: u16,
) -> Result<RelowerReport, RelowerError>
where
    A: Adapter + ?Sized,
    P: PayloadArchive + ?Sized,
    S: Source + ?Sized,
{
    let mut wire = WireCapture::new();
    wire.absorb(multicast, expected_magic)?;
    let source_id = wire.source_id()?;
    let re_lowered = relower(adapter, payloads, wire.refdata(), source_id)?;
    Ok(compare(&wire, &re_lowered))
}

/// Both copies of the messages at one key.
#[derive(Debug, Default)]
struct Sides<'a> {
    on_wire: Vec<&'a WireMessage>,
    re_lowered: Vec<&'a LoweredMessage>,
}

/// Whether the wire's depth numbering starts where a re-lowering's does.
///
/// The re-lowered side stamps every instrument's first delta of an era as `1`,
/// because that is what an era is. So a window whose first depth message for an
/// instrument is numbered above 1 is one where the two numberings are offset,
/// and *every* key for that instrument is then wrong in both directions — which
/// is a caveat worth more than the hundreds of findings it explains.
fn era_caveats(wire: &WireCapture) -> Vec<Caveat> {
    let mut first_seq: BTreeMap<u32, u32> = BTreeMap::new();
    for message in wire.messages() {
        let (instrument_id, seq) = match &message.body {
            MessageBody::Level(level) => (level.instrument_id, level.per_instrument_seq),
            MessageBody::Clear(clear) => (clear.instrument_id, clear.per_instrument_seq),
            MessageBody::Quote(_) | MessageBody::Trade(_) => continue,
        };
        let held = first_seq.entry(instrument_id).or_insert(seq);
        *held = (*held).min(seq);
    }
    first_seq
        .into_iter()
        .filter(|(_, seq)| *seq > 1)
        .map(
            |(instrument_id, first_seq)| Caveat::WindowMayNotStartAtEraBoundary {
                instrument_id,
                first_seq,
            },
        )
        .collect()
}

/// Distinct join keys, for a caller that wants the join's own shape.
///
/// Exposed because it is the one number that says whether two archives describe
/// the same window at all: a comparison whose key sets barely intersect is one
/// whose windows do not line up, and the findings from it are noise.
#[must_use]
pub fn key_overlap(wire: &WireCapture, re_lowered: &ReLowered) -> (usize, usize, usize) {
    let on_wire: BTreeSet<JoinKey> = wire
        .messages()
        .iter()
        .map(|message| message.body.join_key())
        .collect();
    let re: BTreeSet<JoinKey> = re_lowered
        .messages()
        .iter()
        .map(|message| message.body.join_key())
        .collect();
    let shared = on_wire.intersection(&re).count();
    (on_wire.len(), re.len(), shared)
}
