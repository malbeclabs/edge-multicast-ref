//! The two error types an adapter returns, and why one of them is a metric
//! label.

/// Why an upstream payload could not be parsed.
///
/// **These four variants are a metric label.** A governing playbook fixes the
/// ingress parse-error taxonomy at `schema`, `unknown_field`, `malformed` and
/// `truncated`, and `dz_publisher_ingress_parse_errors_total{reason}` is
/// recorded from whatever an adapter returns here. An adapter therefore cannot
/// fail to parse without the right series moving, and cannot invent a fifth
/// reason that a dashboard has no panel for.
///
/// The same taxonomy is declared a second time, as a label enum, in the metrics
/// crate. Two copies is the cost of this crate depending on nothing: a venue
/// must not inherit a Prometheus client to name a parse error. They are held to
/// each other by a test in the metrics crate that compares both the arity and
/// the tokens, which is the arrangement the codec and the metrics crate already
/// use for port roles, and for the same reason — *a metric label is not a wire
/// concern*.
///
/// `detail` is `&'static str` rather than `String` so that failing to parse
/// costs no allocation on a path whose whole purpose is to be cheap. It names
/// the field or the expectation; it is for a log line and never for a label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The payload is a shape this adapter does not implement: a version it
    /// does not speak, or a document type it does not handle.
    #[error("unsupported upstream schema: {detail}")]
    Schema { detail: &'static str },

    /// A field the adapter does not recognise, where recognising every field is
    /// part of the contract. Distinct from `Schema`: the shape is known and one
    /// member of it is not.
    #[error("unknown upstream field: {detail}")]
    UnknownField { detail: &'static str },

    /// Structurally present and unusable: a number that is not one, an
    /// enumeration outside its range, a required field absent.
    #[error("malformed upstream payload: {detail}")]
    Malformed { detail: &'static str },

    /// The payload ended before it was complete.
    #[error("truncated upstream payload: {detail}")]
    Truncated { detail: &'static str },
}

impl ParseError {
    /// Every variant, in the order the metrics crate declares them.
    ///
    /// Used by the test that holds the two taxonomies to each other, so that
    /// adding a variant here without adding the label there fails a build
    /// rather than producing a reason no dashboard groups by.
    pub const ALL: [Self; 4] = [
        Self::Schema { detail: "" },
        Self::UnknownField { detail: "" },
        Self::Malformed { detail: "" },
        Self::Truncated { detail: "" },
    ];

    /// The label value this reason is counted under.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema { .. } => "schema",
            Self::UnknownField { .. } => "unknown_field",
            Self::Malformed { .. } => "malformed",
            Self::Truncated { .. } => "truncated",
        }
    }

    /// An upstream shape this adapter does not implement.
    #[must_use]
    pub const fn schema(detail: &'static str) -> Self {
        Self::Schema { detail }
    }

    /// A field the adapter does not recognise.
    #[must_use]
    pub const fn unknown_field(detail: &'static str) -> Self {
        Self::UnknownField { detail }
    }

    /// Present and unusable.
    #[must_use]
    pub const fn malformed(detail: &'static str) -> Self {
        Self::Malformed { detail }
    }

    /// Ended before it was complete.
    #[must_use]
    pub const fn truncated(detail: &'static str) -> Self {
        Self::Truncated { detail }
    }
}

/// Why an adapter could not do something that was asked of it.
///
/// Deliberately not the same type as [`ParseError`]: that one is a property of
/// a payload and is counted per payload, this one is a property of a request the
/// runtime made and is acted on rather than counted. The three variants are the
/// three different actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdapterError {
    /// The adapter cannot answer yet, and expects to be able to later: a book
    /// that has not bootstrapped, a session that has not authenticated.
    ///
    /// The caller retries. **Not an error to log at error level**, and in
    /// particular not one to restart on: a snapshot rotation that finds one
    /// instrument not ready skips that slot and comes back, which is the
    /// difference between one dormant instrument and a restart loop.
    #[error("not ready: {detail}")]
    NotReady { detail: &'static str },

    /// The adapter was asked about an instrument it does not hold.
    ///
    /// This is a disagreement between the runtime's admitted set and the
    /// adapter's own, which is a defect on one side or the other and never
    /// something to retry.
    #[error("instrument not held by this adapter")]
    UnknownInstrument,

    /// The adapter failed at something it should have been able to do. A bug,
    /// reported as one.
    #[error("adapter failure: {detail}")]
    Internal { detail: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reason_has_a_distinct_token() {
        let mut tokens: Vec<&str> = ParseError::ALL.iter().map(|e| e.as_str()).collect();
        tokens.sort_unstable();
        let count = tokens.len();
        tokens.dedup();
        assert_eq!(tokens.len(), count, "two reasons share a label value");
    }

    #[test]
    fn a_constructor_exists_for_every_reason() {
        // Pins the constructors to the variants: a new variant without one
        // makes this fail to compile rather than leaving callers to write the
        // struct literal and get the detail field wrong.
        assert_eq!(ParseError::schema("x").as_str(), "schema");
        assert_eq!(ParseError::unknown_field("x").as_str(), "unknown_field");
        assert_eq!(ParseError::malformed("x").as_str(), "malformed");
        assert_eq!(ParseError::truncated("x").as_str(), "truncated");
    }

    #[test]
    fn detail_reaches_the_message_and_not_the_token() {
        let error = ParseError::malformed("bid_px");
        assert!(error.to_string().contains("bid_px"));
        assert_eq!(error.as_str(), "malformed");
    }
}
