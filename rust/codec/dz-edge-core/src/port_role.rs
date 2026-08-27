/// The three port roles a channel may use. The specification names exactly
/// these three and requires these spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortRole {
    Mktdata,
    Refdata,
    /// No message type in these crates lists this role yet, because the
    /// snapshot port role belongs to the depth feeds, which are not
    /// implemented here. A builder constructed with this role will refuse
    /// every message currently defined; the first depth feed's message types
    /// will list it.
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
