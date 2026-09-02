//! What an adapter produces: one market event, resolved, in the venue's own
//! units.

use crate::instrument::InstrumentRef;
use crate::scalar::Scalar;

/// One market event, ready to be lowered onto the wire.
///
/// **Resolved, not raw.** An adapter emits the state its upstream has already
/// been folded into, never the upstream's own deltas: a venue quoting absolute
/// depth emits levels directly, a venue quoting increments keeps its own book
/// and emits the level that results. The book state machine stays with the
/// venue because it follows the venue's microstructure, and one existing
/// publisher deliberately runs two that are not converged. What crosses this
/// boundary is the outcome.
///
/// `#[non_exhaustive]` because feeds are added over time and a venue pinned to
/// an old tag must not be broken by a message type it does not emit. Adding a
/// variant is a minor version; adding a field to one is not, which is why the
/// market-by-order variants are absent rather than guessed — the codec crate
/// that fixes their fields does not exist yet, and specifying them against
/// nothing would buy a breaking change later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event<'a> {
    /// A two-sided top-of-book update.
    ///
    /// Both sides are always stated, each as one of three cases, and that is
    /// the whole point: `Update Flags` is derived from the pair above this
    /// boundary. An existing publisher's encoder writes both update bits
    /// unconditionally on every quote, on a live feed, so every subscriber
    /// reading the field is told both sides changed whenever either did. From
    /// here that byte is not reachable, and [`SideUpdate::Unchanged`] is how a
    /// venue says the thing that encoder cannot.
    Quote {
        instrument: InstrumentRef,
        /// The venue's own timestamp for this event, in nanoseconds.
        source_ts_ns: u64,
        bid: SideUpdate<'a>,
        ask: SideUpdate<'a>,
    },

    /// One execution.
    Trade {
        instrument: InstrumentRef,
        source_ts_ns: u64,
        px: Scalar<'a>,
        qty: Scalar<'a>,
        aggressor: Aggressor,
        /// The venue's own trade identifier, where it publishes one. Used for
        /// dedup across publishers, so it must be derived from what the venue
        /// assigned and never from a counter of our own.
        trade_id: Option<u64>,
        /// Session cumulative volume, where the venue publishes it.
        cumulative_volume: Option<Scalar<'a>>,
        flags: TradeFlags,
    },

    /// One price level's aggregate resting quantity, **after** the change.
    ///
    /// `qty` is absolute and never a delta, and a quantity of zero removes the
    /// level. A subscriber that added it to what it held would drift; one that
    /// missed a message is wrong at that price and correct everywhere else,
    /// which is what makes the loss bounded and detectable.
    Level {
        instrument: InstrumentRef,
        source_ts_ns: u64,
        side: Side,
        px: Scalar<'a>,
        /// Absolute aggregate quantity at `px`. Zero removes the level.
        qty: Scalar<'a>,
        /// Orders resting at this price, where the venue publishes it.
        order_count: Option<u16>,
        presence: Presence,
    },

    /// Bulk removal of levels.
    ///
    /// Not a resynchronisation signal: it says these levels are gone, not that
    /// the book is untrustworthy.
    Clear {
        instrument: InstrumentRef,
        source_ts_ns: u64,
        scope: ClearScope<'a>,
    },
}

/// What happened to one side of a two-sided quote.
///
/// The three cases are what `Update Flags` is derived from, and a venue always
/// knows which of them it has. There is no fourth, and no way to state a raw
/// flags byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideUpdate<'a> {
    /// This side did not move. The subscriber keeps what it holds.
    Unchanged,
    /// This side has no resting quantity: the book is one-sided here.
    Gone,
    /// This side is now the stated price and quantity.
    Updated {
        px: Scalar<'a>,
        qty: Scalar<'a>,
        /// How many distinct sources contribute to this side, for a venue that
        /// aggregates several. `None` where the venue does not say.
        source_count: Option<u16>,
    },
}

/// Which side of the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

/// Which side the aggressor of a trade took.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Aggressor {
    /// The venue does not publish it. Not a default to reach for: a venue that
    /// publishes the side and states `Unknown` here has lost information every
    /// consumer of the trade feed wanted.
    Unknown,
    Buy,
    Sell,
}

/// The trade qualifiers the specification defines.
///
/// Three booleans rather than the wire's flags byte, for the same reason
/// [`SideUpdate`] is three cases rather than two bits: a venue that can write a
/// byte can write a bit nobody defined, and a byte a venue composes is a byte no
/// test compares against anything.
///
/// `sweep` keeps its externally defined name: it is what the field is called on
/// the wire and what the term means in the market it describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TradeFlags {
    /// A block trade.
    pub block: bool,
    /// An order that swept several levels.
    pub sweep: bool,
    /// A cross.
    pub cross: bool,
}

impl TradeFlags {
    /// No qualifier set: the ordinary trade.
    pub const NONE: Self = Self {
        block: false,
        sweep: false,
        cross: false,
    };
}

/// What a venue knows about whether a level existed before this update.
///
/// The wire's `Action` is derived from this and from the quantity, above this
/// boundary and in one place: **a quantity of zero is a removal and nothing
/// else can be**, and a non-zero quantity takes the hint below. The two pairings
/// the specification forbids — a removal carrying any other action, an action of
/// removal carrying quantity — are therefore not merely refused but
/// unrepresentable.
///
/// This is the one shipped defect this crate was shaped around. A publisher
/// numbering the action table from `New` instead of `Unknown` emits every
/// removal as a change carrying zero: self-consistent, invisible to any test
/// that encodes and decodes, and quietly wrong for every consumer that reads the
/// field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Presence {
    /// The upstream does not distinguish an insertion from a change.
    ///
    /// Conformant, and the correct default: the specification defines this
    /// value for exactly this case, and it covers that ambiguity and nothing
    /// else. It is never a conformant answer for a removal, which is why a
    /// removal does not come through here at all.
    #[default]
    Unknown,
    /// No quantity rested at this price before.
    New,
    /// Some did.
    Change,
}

/// How much of a book a clear removes.
///
/// The specification forbids a clear bounded by one price from applying to both
/// sides — *"a bounded clear of both sides has no meaning two implementations
/// would agree on"* — and the codec refuses that pairing at the push. Here it
/// cannot be written: [`FromPrice`](Self::FromPrice) names one side, and there
/// is no variant that bounds both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearScope<'a> {
    /// Remove every level on one side.
    EntireSide(Side),
    /// Remove every level on both sides.
    BothSides,
    /// Remove every level on one side from `px` outward, inclusive.
    FromPrice { side: Side, px: Scalar<'a> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bounded_clear_of_both_sides_cannot_be_written() {
        // Not an assertion about a value — an assertion about the type. Every
        // scope that bounds by a price names exactly one side, so the pairing
        // the specification forbids has no representation to refuse.
        let bounded = ClearScope::FromPrice {
            side: Side::Bid,
            px: Scalar::text("100"),
        };
        match bounded {
            ClearScope::FromPrice { side, .. } => assert_eq!(side, Side::Bid),
            ClearScope::EntireSide(_) | ClearScope::BothSides => {
                panic!("constructed scope changed shape")
            }
        }
    }

    #[test]
    fn presence_defaults_to_the_conformant_value() {
        // A venue that does not think about this field gets the value the
        // specification defines for not knowing, not the first variant of a
        // table.
        assert_eq!(Presence::default(), Presence::Unknown);
    }

    #[test]
    fn trade_flags_default_to_none() {
        assert_eq!(TradeFlags::default(), TradeFlags::NONE);
    }

    #[test]
    fn a_quote_states_both_sides() {
        // The shape that makes `Update Flags` derivable: there is no way to
        // build a quote that leaves a side unsaid.
        let quote = Event::Quote {
            instrument: InstrumentRef::from_admission(0),
            source_ts_ns: 1,
            bid: SideUpdate::Updated {
                px: Scalar::text("1.00"),
                qty: Scalar::text("5"),
                source_count: None,
            },
            ask: SideUpdate::Gone,
        };
        match quote {
            Event::Quote { bid, ask, .. } => {
                assert!(matches!(bid, SideUpdate::Updated { .. }));
                assert_eq!(ask, SideUpdate::Gone);
            }
            _ => panic!("expected a quote"),
        }
    }
}
