/// A feed in the DoubleZero Edge family.
///
/// `Magic` is what rejects a datagram misrouted from a sibling feed, so it
/// belongs to the feed rather than to a call site that has to remember it.
pub trait Feed {
    /// The datagram delimiter for this feed.
    const MAGIC: u16;

    /// The feed's name, for diagnostics.
    const NAME: &'static str;
}
