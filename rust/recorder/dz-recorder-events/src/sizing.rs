//! The sizing measurement: **messages walked over datagrams walked, per feed.**
//!
//! This produces a number rather than a behaviour, and the number is what a
//! deployment decision is made against. The transport tier writes one row per
//! datagram; this tier writes one row per *message*, and the multiplier between
//! them is neither small nor constant — it is a property of the feed and its
//! publisher, not of the recorder, so it cannot be assumed from another feed.
//! The design says the multiplier is measured, per feed, from the archive
//! itself, before that feed's derivation is enabled.
//!
//! # Why the denominator is not the messages' own provenance
//!
//! Every message the walk yields carries the datagram it arrived in, so the
//! distinct datagram indices in [`WireCapture::messages`] look like a
//! denominator and are not one. A datagram carrying nothing but a `Heartbeat`
//! yields no message at all, and it is still a datagram: the transport tier
//! reads its header and writes a row for it. Counting only the datagrams that
//! carried something would divide by the busy ones and report a feed as denser
//! than it is — on precisely the quiet channels where the answer decides
//! whether the derivation is affordable. So the denominator comes from
//! [`WireCapture::datagrams_by_instance`], which counts a datagram when its
//! header is read.
//!
//! # Why a window has to state what it held
//!
//! A ratio taken over a quiet minute is a true number about a window nobody
//! cares about. The burst is the case the multiplier exists to warn about — a
//! publisher that packs an update burst into one datagram produces one
//! transport row and hundreds of rows here — and a snapshot cycle is
//! `total_levels` messages per instrument arriving on the runtime's own
//! cadence. A window holding neither cannot answer the question it was taken
//! for, so [`FeedSizing::incomplete`] says so rather than leaving a reader to
//! notice.
//!
//! # Keyed on the channel
//!
//! Per feed is per `(source address, Channel ID)` — the same key the reference
//! data and the book use, and for the same reason. A feed's messages are spread
//! across `mktdata`, `refdata` and `snapshot`, which are three channel
//! instances, and a ratio taken per instance would divide one feed's definition
//! cycle by its own datagrams and never see the prices it is published for.

use std::collections::BTreeMap;
use std::fmt;

use dz_recorder_core::{ChannelInstance, Source};
use dz_recorder_relower::{RelowerError, StateBody, WireCapture};

use crate::derive::instance_of;
use crate::instruments::Channel;

/// Which of the walk's three outputs a message came from.
///
/// The three are counted apart because they are paid for apart: the reference
/// messages are the definition cycle on the runtime's own clock and are the same
/// count on a dead market as on a busy one, and reading a ratio without seeing
/// that share would attribute a quiet feed's rows to its prices.
#[derive(Debug, Clone, Copy)]
enum Class {
    Market,
    State,
    Reference,
}

/// Why a window's ratio is not yet worth deciding against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Incomplete {
    /// No datagram in the window carried more than one message.
    ///
    /// The mean over such a window is 1 or below whatever the publisher does
    /// under load, so it states the publisher's idle behaviour and nothing
    /// about the burst the multiplier is asked about.
    NoBurst,
    /// No snapshot cycle completed in the window.
    ///
    /// A cycle is the largest run of messages a single datagram stream produces
    /// per instrument, and it arrives on the runtime's cadence rather than the
    /// market's — a window that missed one has not seen the feed's peak.
    NoSnapshotCycle,
}

impl Incomplete {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoBurst => "no datagram carried more than one message",
            Self::NoSnapshotCycle => "no snapshot cycle completed",
        }
    }
}

/// One feed's measurement over one window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeedSizing {
    /// Datagrams of this feed's `Magic` on this channel, across every port
    /// role, counted from the header the way the transport tier counts them.
    pub datagrams: u64,
    /// Datagrams that carried at least one message the walk yielded.
    ///
    /// The difference against [`datagrams`](Self::datagrams) is the control
    /// traffic — `Heartbeat` and `EndOfSession` — which becomes a transport row
    /// and no market data row at all.
    pub datagrams_with_messages: u64,
    /// `Quote`, `Trade`, `LevelUpdate` and `BookClear`.
    pub market_messages: u64,
    /// `InstrumentReset` and the snapshot triple.
    pub state_messages: u64,
    /// `InstrumentDefinition` and `ManifestSummary`.
    pub reference_messages: u64,
    /// The most messages any one datagram of this feed carried.
    ///
    /// The mean sizes the storage; this sizes the loader, which holds an
    /// object's rows before it sends them.
    pub peak_messages_per_datagram: u32,
    /// Snapshot cycles that both began and ended inside the window.
    pub snapshot_cycles: u64,
}

impl FeedSizing {
    /// Every message the walk yielded on this channel.
    ///
    /// The numerator. Control messages are deliberately absent: they are in the
    /// denominator, because a datagram carrying one is a transport row, and out
    /// of the numerator, because none of them becomes a market data row. A
    /// channel that is mostly heartbeats therefore measures below one, which is
    /// the true statement about it.
    #[must_use]
    pub const fn messages(&self) -> u64 {
        self.market_messages + self.state_messages + self.reference_messages
    }

    /// Messages walked over datagrams walked.
    ///
    /// `None` for a channel with no datagrams at all, which is a channel that
    /// was configured and silent rather than one whose ratio is zero.
    #[must_use]
    pub fn messages_per_datagram(&self) -> Option<f64> {
        if self.datagrams == 0 {
            return None;
        }
        // Both counts are archive-sized rather than u64-sized, and a ratio is
        // wanted rather than an exact integer, so the precision the cast can
        // lose is far below what the number is read to.
        #[allow(clippy::cast_precision_loss)]
        Some(self.messages() as f64 / self.datagrams as f64)
    }

    /// What this window did not hold, and so cannot answer for.
    #[must_use]
    pub fn incomplete(&self) -> Vec<Incomplete> {
        let mut out = Vec::new();
        if self.peak_messages_per_datagram <= 1 {
            out.push(Incomplete::NoBurst);
        }
        if self.snapshot_cycles == 0 {
            out.push(Incomplete::NoSnapshotCycle);
        }
        out
    }
}

/// The measurement over one window, per feed.
#[derive(Debug, Clone, Default)]
pub struct Sizing {
    by_channel: BTreeMap<Channel, FeedSizing>,
}

impl Sizing {
    /// Measure what a capture already holds.
    ///
    /// The window is whatever was absorbed into it: a `WireCapture` accumulates
    /// across calls, so several objects are one window by being absorbed into
    /// one capture, and a window of one object is the degenerate case rather
    /// than the shape.
    #[must_use]
    pub fn of(capture: &WireCapture) -> Self {
        let mut by_channel: BTreeMap<Channel, FeedSizing> = BTreeMap::new();
        for (instance, datagrams) in capture.datagrams_by_instance() {
            by_channel
                .entry(Channel::of(*instance))
                .or_default()
                .datagrams += datagrams;
        }

        // Messages per datagram, so the peak is the packing the publisher
        // actually chose rather than the `Message Count` its header claimed.
        let mut per_datagram: BTreeMap<(Channel, u64), u32> = BTreeMap::new();
        let walked = capture
            .messages()
            .iter()
            .map(|m| (m.provenance, Class::Market))
            .chain(
                capture
                    .state_messages()
                    .iter()
                    .map(|m| (m.provenance, Class::State)),
            )
            .chain(
                capture
                    .reference_messages()
                    .iter()
                    .map(|m| (m.provenance, Class::Reference)),
            );
        for (provenance, class) in walked {
            let channel = Channel::of(instance_of(&provenance));
            let feed = by_channel.entry(channel).or_default();
            match class {
                Class::Market => feed.market_messages += 1,
                Class::State => feed.state_messages += 1,
                Class::Reference => feed.reference_messages += 1,
            }
            *per_datagram
                .entry((channel, provenance.datagram_index))
                .or_default() += 1;
        }

        for ((channel, _), messages) in &per_datagram {
            let feed = by_channel.entry(*channel).or_default();
            feed.datagrams_with_messages += 1;
            feed.peak_messages_per_datagram = feed.peak_messages_per_datagram.max(*messages);
        }

        for (channel, cycles) in completed_cycles(capture) {
            by_channel.entry(channel).or_default().snapshot_cycles += cycles;
        }

        Self { by_channel }
    }

    /// Measure one archive.
    ///
    /// # Errors
    ///
    /// [`RelowerError::MulticastArchive`] if the source fails before it is
    /// exhausted. A window that tore is a window whose denominator stops before
    /// its numerator does, and a ratio taken over one is not a smaller true
    /// measurement — it is a wrong one.
    pub fn measure<S: Source + ?Sized>(source: &mut S, magic: u16) -> Result<Self, RelowerError> {
        let mut capture = WireCapture::new();
        capture.absorb(source, magic)?;
        Ok(Self::of(&capture))
    }

    #[must_use]
    pub const fn by_channel(&self) -> &BTreeMap<Channel, FeedSizing> {
        &self.by_channel
    }

    /// One channel's measurement.
    #[must_use]
    pub fn feed(&self, channel: Channel) -> Option<&FeedSizing> {
        self.by_channel.get(&channel)
    }

    /// Whether the window measured no channel of this feed at all.
    ///
    /// Every other absence in this report is a channel saying it cannot be
    /// decided against yet. This one is the window saying it never held the feed
    /// that was asked for — an archive of another feed, or a `Magic` the caller
    /// got wrong — and it is the difference between *no answer* and *an answer
    /// of nothing*. A caller that prints the table and stops treats the two
    /// alike, and a table with no rows reads as a feed that was quiet rather
    /// than as a question that was never asked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_channel.is_empty()
    }
}

/// Snapshot cycles that both began and ended inside the window, per channel.
///
/// A cycle is open on `(channel instance, instrument, snapshot_id)` and closed
/// by the `SnapshotEnd` that names it. An end with no begin does not count and
/// is not repaired: a window that starts mid-cycle saw part of one, and calling
/// that a cycle would make the window look like it had seen a peak it did not.
fn completed_cycles(capture: &WireCapture) -> BTreeMap<Channel, u64> {
    let mut open: BTreeMap<(ChannelInstance, u32, u32), ()> = BTreeMap::new();
    let mut out: BTreeMap<Channel, u64> = BTreeMap::new();
    for message in capture.state_messages() {
        let instance = instance_of(&message.provenance);
        match message.body {
            StateBody::SnapshotBegin(begin) => {
                open.insert((instance, begin.instrument_id, begin.snapshot_id), ());
            }
            StateBody::SnapshotEnd(end) => {
                if open
                    .remove(&(instance, end.instrument_id, end.snapshot_id))
                    .is_some()
                {
                    *out.entry(Channel::of(instance)).or_default() += 1;
                }
            }
            StateBody::Reset(_) | StateBody::SnapshotLevel(_) => {}
        }
    }
    out
}

/// The report, as a person reads it before enabling a feed.
impl fmt::Display for Sizing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "messages walked over datagrams walked, per feed")?;
        writeln!(
            f,
            "{:<22} {:>10} {:>10} {:>13} {:>6} {:>7}",
            "channel", "datagrams", "messages", "per datagram", "peak", "cycles"
        )?;
        for (channel, feed) in &self.by_channel {
            let name = format!("{}/{}", channel.source_addr, channel.channel_id);
            let ratio = feed
                .messages_per_datagram()
                .map_or_else(|| "-".to_owned(), |r| format!("{r:.2}"));
            writeln!(
                f,
                "{:<22} {:>10} {:>10} {:>13} {:>6} {:>7}",
                name,
                feed.datagrams,
                feed.messages(),
                ratio,
                feed.peak_messages_per_datagram,
                feed.snapshot_cycles
            )?;
            writeln!(
                f,
                "{:<22} market {}, state {}, reference {}; {} datagram(s) carried no message",
                "",
                feed.market_messages,
                feed.state_messages,
                feed.reference_messages,
                feed.datagrams - feed.datagrams_with_messages
            )?;
            for incomplete in feed.incomplete() {
                writeln!(f, "{:<22} not yet decidable: {}", "", incomplete.as_str())?;
            }
        }
        Ok(())
    }
}
