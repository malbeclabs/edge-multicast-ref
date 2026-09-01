/// The three port roles a channel may use. The specification names exactly
/// these three and requires these spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortRole {
    Mktdata,
    Refdata,
    /// The depth feeds' third port, carrying book state rather than book
    /// changes. `dz-edge-mbp`'s `SnapshotBegin`, `SnapshotLevel` and
    /// `SnapshotEnd` are the message types that list it; a builder constructed
    /// with this role refuses every other message these crates define, which is
    /// what keeps a live update off the snapshot port and a snapshot off the
    /// live one.
    Snapshot,
}

impl PortRole {
    /// The token used on the wire, in configuration and in metric labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mktdata => "mktdata",
            Self::Refdata => "refdata",
            Self::Snapshot => "snapshot",
        }
    }
}
