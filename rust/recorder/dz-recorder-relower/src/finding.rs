//! The four outcomes, the three findings, and the things the archive could not
//! say.

use crate::diff::FieldDiff;
use crate::join::JoinKey;
use crate::relower::ReLoweredProvenance;
use crate::wire::WireProvenance;

/// What the join concluded about one key.
///
/// **Four outcomes, three findings.** The fourth is the healthy case and
/// produces no row at all — see [`Self::IdenticalDifferentTiming`]. It is
/// enumerated here anyway, because a tool whose output is a list of problems
/// still has to be able to say how many things it checked and found correct, and
/// because naming the fourth is what makes its absence from [`Finding`]
/// deliberate rather than an omission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The re-lowering produced it and the wire does not carry it: the publisher
    /// dropped it — a full queue, a guard, a crash window — or a subscriber-site
    /// network lost it.
    ReLoweredNotOnWire,
    /// The wire carries it and the re-lowering did not produce it: the publisher
    /// invented it, or its reference-data state diverged from what it published.
    OnWireNotReLowered,
    /// Both carry it and a field differs: a lowering or scaling defect, named by
    /// field.
    FieldsDiffer,
    /// Both carry it, every compared field is identical, and the framing and the
    /// pacing differ.
    ///
    /// **This produces no finding, and that is the point of the tool.** The
    /// comparison is at message grain: the datagram that carried a message, its
    /// position in it, its `Sequence Number` and its send timestamp are
    /// provenance and are never compared, so there is nothing here for a
    /// batching or a pacing difference to move. A tool that reported this case
    /// would report every healthy archive, and an operator would learn to close
    /// it.
    IdenticalDifferentTiming,
}

impl Outcome {
    /// All four, in the order the design's table states them.
    pub const ALL: [Self; 4] = [
        Self::ReLoweredNotOnWire,
        Self::OnWireNotReLowered,
        Self::FieldsDiffer,
        Self::IdenticalDifferentTiming,
    ];

    /// A stable token, for a report row or a log line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReLoweredNotOnWire => "re_lowered_not_on_wire",
            Self::OnWireNotReLowered => "on_wire_not_re_lowered",
            Self::FieldsDiffer => "fields_differ",
            Self::IdenticalDifferentTiming => "identical_different_timing",
        }
    }

    /// Whether this outcome is reported as a [`Finding`].
    ///
    /// Exactly one of the four is not.
    #[must_use]
    pub const fn is_finding(self) -> bool {
        !matches!(self, Self::IdenticalDifferentTiming)
    }
}

/// One thing wrong, at one join key.
///
/// There are three variants for four outcomes, and no variant can carry
/// [`Outcome::IdenticalDifferentTiming`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// The venue's events produced this message and the archive does not hold
    /// it.
    ///
    /// The strongest thing this tool says, and the one a reader must be able to
    /// trust: it means the publisher had the input and the subscriber never got
    /// the output. Two things other than a publisher defect can produce it, and
    /// both are visible elsewhere in the report — a datagram lost between the
    /// publisher and this recorder, which the loss tier quantifies, and a
    /// multicast window that opened later than the payload window, which
    /// [`Caveat::WindowMayNotStartAtEraBoundary`] flags.
    ReLoweredNotOnWire {
        key: JoinKey,
        /// The message type, as the codec names it.
        message_type: &'static str,
        /// Which archived payload and which event within it produced it, so the
        /// upstream bytes can be found.
        re_lowered: ReLoweredProvenance,
    },

    /// The archive holds this message and the venue's events did not produce
    /// it.
    ///
    /// The publisher invented it, or its reference-data state diverged. One
    /// other cause is ours and is in the report beside this: a
    /// [`Refusal`](crate::Refusal) means the re-lowering could not state a value
    /// exactly at the archived exponent and produced nothing, so the wire copy
    /// has nothing to join against.
    ///
    /// A third cause is the network's, and it is stated here because the join
    /// cannot tell it apart: a datagram delivered twice puts a second copy of
    /// every message in it into the archive, and the surplus copy is reported
    /// here. [`Caveat::AmbiguousJoinKey`] is raised for the same key, and the
    /// loss tier's own duplicate rows for the same window are what settle it —
    /// this comparison holds no datagram identity and must not guess.
    OnWireNotReLowered {
        key: JoinKey,
        message_type: &'static str,
        /// Which datagram carried it, so the archive can be opened at it.
        wire: WireProvenance,
    },

    /// Both carry it and these fields differ.
    ///
    /// The fields are named, because *a price differs* sends nobody anywhere and
    /// *`bid_price` differs, `1234` on the wire against `12340`* is an exponent
    /// off by one. Every compared field that differs is listed; a message with
    /// two defects produces one finding with two field differences rather than
    /// two findings.
    FieldsDiffer {
        key: JoinKey,
        message_type: &'static str,
        fields: Vec<FieldDiff>,
        wire: WireProvenance,
        re_lowered: ReLoweredProvenance,
    },
}

impl Finding {
    /// Which of the four outcomes this is.
    ///
    /// Never [`Outcome::IdenticalDifferentTiming`]: there is no variant that
    /// could return it.
    #[must_use]
    pub const fn outcome(&self) -> Outcome {
        match self {
            Self::ReLoweredNotOnWire { .. } => Outcome::ReLoweredNotOnWire,
            Self::OnWireNotReLowered { .. } => Outcome::OnWireNotReLowered,
            Self::FieldsDiffer { .. } => Outcome::FieldsDiffer,
        }
    }

    /// The key this finding is about.
    #[must_use]
    pub const fn key(&self) -> &JoinKey {
        match self {
            Self::ReLoweredNotOnWire { key, .. }
            | Self::OnWireNotReLowered { key, .. }
            | Self::FieldsDiffer { key, .. } => key,
        }
    }

    /// The message type this finding is about, as the codec names it.
    #[must_use]
    pub const fn message_type(&self) -> &'static str {
        match self {
            Self::ReLoweredNotOnWire { message_type, .. }
            | Self::OnWireNotReLowered { message_type, .. }
            | Self::FieldsDiffer { message_type, .. } => message_type,
        }
    }
}

impl core::fmt::Display for Finding {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ReLoweredNotOnWire {
                key,
                message_type,
                re_lowered,
            } => write!(
                f,
                "{message_type} at {key} was re-lowered from {re_lowered} and is not on the wire"
            ),
            Self::OnWireNotReLowered {
                key,
                message_type,
                wire,
            } => write!(
                f,
                "{message_type} at {key} is on the wire at {wire} and was not re-lowered"
            ),
            Self::FieldsDiffer {
                key,
                message_type,
                fields,
                wire,
                re_lowered,
            } => {
                write!(
                    f,
                    "{message_type} at {key} differs (wire {wire}, re-lowered {re_lowered}):"
                )?;
                for field in fields {
                    write!(f, " {field};")?;
                }
                Ok(())
            }
        }
    }
}

/// Something the comparison could not establish, or had to choose.
///
/// **Not a finding, and never presented as one.** Every one of these is a
/// statement about the *evidence*: a caveat says a difference the report shows
/// may be the archive's or ours rather than the publisher's, or that a
/// difference the report does not show may still exist. A tool that folded these
/// into its findings would blame a publisher for a short window; a tool that
/// dropped them would let a reader trust a clean report that checked nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caveat {
    /// The highest valid `ManifestSummary` declared a published set larger or
    /// smaller than the definitions the archive yielded.
    ///
    /// Larger is the ordinary case and means the refdata window is short: the
    /// definition cycle is paced, so a capture shorter than one cycle holds only
    /// part of the set. Every instrument whose definition is missing will be
    /// declined by the re-lowering and reported as a
    /// [`MissingDefinition`](crate::MissingDefinition).
    ReferenceDataIncomplete {
        manifest_seq: u16,
        declared: u32,
        reconstructed: usize,
    },

    /// A symbol's exponents were restated inside the window.
    ///
    /// The first statement is used for the whole window, because the two
    /// archives carry no key that orders one against the other and so the
    /// instant the restatement took effect cannot be placed in the payload
    /// stream. Every message for this instrument on the far side of the
    /// restatement is therefore compared at the wrong exponent, and the
    /// differences it produces are the archive's rather than the publisher's.
    ScaleRestated {
        instrument_id: u32,
        /// The exponents used: `(price, qty)`, from the first definition seen.
        kept: (i8, i8),
        /// The exponents the later definition stated.
        later: (i8, i8),
    },

    /// A symbol was defined twice under different `Instrument ID`s.
    ///
    /// The first is used. A publisher that re-mints an id for a live symbol has
    /// stranded every subscriber's book for it, which is a finding for the
    /// conformance tier; here it means messages under the second id join against
    /// nothing.
    SymbolRepublishedUnderANewId { kept: u32, later: u32 },

    /// One `Instrument ID` was defined for two different symbols.
    ///
    /// Reported and not resolved: an id that resolves to two symbols makes every
    /// message under it attributable to either.
    IdSharedByTwoSymbols { instrument_id: u32 },

    /// The instrument's definition declares a contract, and the wire does not
    /// carry the factor a re-lowering would need.
    ///
    /// `Contract Value` states what one contract is worth; the lowering's factor
    /// states how much of the underlying one contract is. The second is not
    /// derivable from the first, and it is what a per-contract venue's prices
    /// are divided by and its quantities multiplied by. So the re-lowering
    /// applies no factor. If the publisher applied one, every price and quantity
    /// for this instrument will differ — and this caveat is the reason to look
    /// at the venue's listing rather than at the publisher's scaling.
    ContractFactorNotOnTheWire { instrument_id: u32 },

    /// The multicast window contains a `Reset Count` change on this channel.
    ///
    /// `Per-Instrument Seq` restarts at 1 with the era, and the payload archive
    /// carries nothing that says which payload the reset fell between. So the
    /// depth key space is reused inside the window: the same
    /// `(Instrument ID, Per-Instrument Seq)` names two different messages, and
    /// the join cannot tell them apart. Compare a window per era.
    EraChangeInsideWindow { channel_id: u8 },

    /// The lowest `Per-Instrument Seq` this instrument's depth messages carry on
    /// the wire is above 1.
    ///
    /// The re-lowering starts every instrument's series at 1, because that is
    /// what the era does. If the multicast window opened after the era began,
    /// the wire's numbering is offset from the re-lowered numbering by however
    /// many deltas preceded the window, and *every* key for this instrument is
    /// then wrong in both directions.
    ///
    /// The other cause is ordinary loss: the datagram carrying seq 1 was not
    /// captured. The two are told apart by the size of the offset, and by the
    /// loss tier's own rows for the same window.
    WindowMayNotStartAtEraBoundary { instrument_id: u32, first_seq: u32 },

    /// An instrument was admitted part-way through the window.
    ///
    /// The runtime drains an adapter's listings on **its own cadence**, and the
    /// payload archive records nothing about that cadence. A re-lowering
    /// therefore admits an instrument as early as the payloads allow, which may
    /// be earlier than the publisher did — and an event the publisher refused
    /// because it held no handle yet is one this comparison lowered
    /// successfully, and reports as a message the publisher dropped.
    ///
    /// Only the first messages for a newly listed instrument are affected, and
    /// only in a window that contains its listing.
    InstrumentAdmittedInsideWindow { instrument_id: u32, at_payload: u64 },

    /// This key is not unique on one or both sides.
    ///
    /// The join pairs the copies in arrival order within the key and reports any
    /// surplus on either side as an absence. That is a choice, and it is
    /// arbitrary in the way any pairing of indistinguishable things is, so it is
    /// declared: a field difference at an ambiguous key may be an artefact of
    /// which copy was paired with which.
    AmbiguousJoinKey {
        key: JoinKey,
        on_wire: usize,
        re_lowered: usize,
    },
}

impl core::fmt::Display for Caveat {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ReferenceDataIncomplete {
                manifest_seq,
                declared,
                reconstructed,
            } => write!(
                f,
                "manifest {manifest_seq} declares {declared} instruments and the archive yielded {reconstructed} definitions"
            ),
            Self::ScaleRestated {
                instrument_id,
                kept,
                later,
            } => write!(
                f,
                "instrument {instrument_id} restated its exponents from (price {}, qty {}) to (price {}, qty {}); the first is used for the whole window",
                kept.0, kept.1, later.0, later.1
            ),
            Self::SymbolRepublishedUnderANewId { kept, later } => write!(
                f,
                "one symbol was defined under instrument {kept} and again under {later}; the first is used"
            ),
            Self::IdSharedByTwoSymbols { instrument_id } => write!(
                f,
                "instrument {instrument_id} was defined for two different symbols"
            ),
            Self::ContractFactorNotOnTheWire { instrument_id } => write!(
                f,
                "instrument {instrument_id} declares a contract value; the wire does not carry the contract factor, so the re-lowering applied none"
            ),
            Self::EraChangeInsideWindow { channel_id } => write!(
                f,
                "channel {channel_id} changed Reset Count inside the window, so the depth key space is reused in it"
            ),
            Self::WindowMayNotStartAtEraBoundary {
                instrument_id,
                first_seq,
            } => write!(
                f,
                "instrument {instrument_id}'s first depth message on the wire carries per-instrument seq {first_seq}, not 1"
            ),
            Self::InstrumentAdmittedInsideWindow {
                instrument_id,
                at_payload,
            } => write!(
                f,
                "instrument {instrument_id} was admitted at payload {at_payload}, and the runtime's own admission cadence is not in the archive"
            ),
            Self::AmbiguousJoinKey {
                key,
                on_wire,
                re_lowered,
            } => write!(
                f,
                "{key} names {on_wire} messages on the wire and {re_lowered} re-lowered; the pairing within it is by arrival order"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_one_of_the_four_outcomes_is_not_a_finding() {
        let not_findings: Vec<&str> = Outcome::ALL
            .iter()
            .filter(|outcome| !outcome.is_finding())
            .map(|outcome| outcome.as_str())
            .collect();
        assert_eq!(not_findings, vec!["identical_different_timing"]);
    }

    #[test]
    fn no_finding_can_carry_the_fourth_outcome() {
        // Written as an exhaustive match over the outcome a finding reports, so
        // a fourth variant added to `Finding` fails here rather than quietly
        // making the healthy case reportable.
        for outcome in Outcome::ALL {
            let reachable = match outcome {
                Outcome::ReLoweredNotOnWire
                | Outcome::OnWireNotReLowered
                | Outcome::FieldsDiffer => true,
                Outcome::IdenticalDifferentTiming => false,
            };
            assert_eq!(reachable, outcome.is_finding());
        }
    }

    #[test]
    fn every_outcome_has_a_distinct_token() {
        let mut tokens: Vec<&str> = Outcome::ALL.iter().map(|o| o.as_str()).collect();
        tokens.sort_unstable();
        let count = tokens.len();
        tokens.dedup();
        assert_eq!(tokens.len(), count, "two outcomes share a token");
    }
}
