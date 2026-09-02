/// A feed in the DoubleZero Edge family.
///
/// `Magic` is what rejects a datagram misrouted from a sibling feed, so it
/// belongs to the feed rather than to a call site that has to remember it.
pub trait Feed {
    /// The datagram delimiter for this feed.
    const MAGIC: u16;

    /// The feed's name, for diagnostics.
    const NAME: &'static str;

    /// Every message Type ID this feed's specification carries.
    ///
    /// **The specification's own message table, as a constant**, and it exists
    /// because the magic alone does not stop the mistake it looks like it
    /// stops. A [`DatagramBuilder`](crate::DatagramBuilder) is generic over the
    /// feed, so the magic on the wire is always right — but `push` took any
    /// [`AppMessage`](crate::AppMessage) and validated only its port role.
    /// Nothing refused a `Quote` in a market-by-price datagram, and `0x03` is
    /// not in that feed's table: a subscriber would read a message the feed it
    /// subscribed to does not define.
    ///
    /// That is the same class of defect as an `Action` table numbered from the
    /// wrong value — a byte the codec permits and a specification forbids —
    /// and it is caught the same way: by holding the code to a table
    /// transcribed from the specification rather than to itself.
    ///
    /// A shared Type ID appears in every sibling that carries it. The wire's
    /// cross-specification policy requires such a message to mean the same
    /// thing in each, which is why one entry in several tables is correct
    /// rather than a duplication to factor out.
    const CARRIES: &'static [u8];

    /// Whether this feed's specification carries a Type ID.
    #[must_use]
    fn carries(type_id: u8) -> bool {
        // A linear scan over a handful of bytes, on a path that then writes a
        // whole message. `contains` on a slice is what a `match` would compile
        // to and it cannot fall out of step with the table above.
        Self::CARRIES.contains(&type_id)
    }
}
