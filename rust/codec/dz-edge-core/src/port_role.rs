/// The three port roles a channel may use. The specification names exactly
/// these three and requires these spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortRole {
    Mktdata,
    Refdata,
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
