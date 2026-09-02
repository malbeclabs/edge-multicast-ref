//! `[ingress] kind`: a closed set, resolved by a match, gated by features.
//!
//! The publisher crates design spells `[ingress] kind` and `[adapter] kind`
//! alike and says plainly that they are not the same mechanism. This is the
//! first of the two. An adapter is chosen from a registry the venue's own `main`
//! populates, because the set of adapters a binary contains is not knowable
//! here. A transport is the opposite: the family is fixed, it lives in this
//! repository, and the set is therefore a closed enum and a total match.

use crate::error::ConfigError;

/// One of the transports in this family.
///
/// **Not `#[non_exhaustive]`, deliberately.** Adding a transport should break
/// every match over this type, including the runtime's, because the thing that
/// makes a transport usable is something constructing it — and a variant a
/// configuration can name but nothing constructs is a value that resolves to
/// nothing at startup. Breaking the match is how that is caught at compile time
/// rather than by an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// A websocket client. `dz-ingress-websocket`.
    WebSocket,
    /// A session-oriented order-entry protocol. Not yet built.
    Fix,
    /// A multicast receiver, for a venue that publishes one. Not yet built.
    Multicast,
    /// Polled request/response. Not yet built.
    Rest,
    /// A local file or directory the venue's own process writes. Not yet built.
    FileTail,
    /// A Unix socket carrying a framed stream from another process, which is
    /// how a venue integration that is not Rust reaches this boundary. Not yet
    /// built.
    Uds,
}

impl Kind {
    /// Every transport in the family, whether or not this binary links it.
    ///
    /// The set an error message names. An operator who has misspelled a value
    /// needs to be told what the acceptable ones are, and being told only the
    /// ones this build happens to contain would hide the difference between
    /// *no such transport* and *not built with it*.
    pub const ALL: [Self; 6] = [
        Self::WebSocket,
        Self::Fix,
        Self::Multicast,
        Self::Rest,
        Self::FileTail,
        Self::Uds,
    ];

    /// The tokens of [`ALL`](Self::ALL), for an error message.
    ///
    /// Written out rather than built at runtime so that it is a `&'static str`
    /// usable in a `thiserror` format string, and so that a variant added
    /// without a token here is caught by
    /// `tests/config.rs::every_kind_has_a_token`.
    pub const TOKEN_LIST: &'static str = "websocket, fix, multicast, rest, filetail, uds";

    /// The configuration token for this transport.
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Fix => "fix",
            Self::Multicast => "multicast",
            Self::Rest => "rest",
            Self::FileTail => "filetail",
            Self::Uds => "uds",
        }
    }

    /// Whether this binary contains an implementation of this transport.
    ///
    /// # How the feature comes to be on
    ///
    /// Each feature here is turned on by the *transport crate*, not by whoever
    /// assembles the binary:
    /// `dz-ingress-websocket` depends on this crate with `features =
    /// ["websocket"]`. So the feature is on exactly when the crate that
    /// implements the transport is in the link, which is what makes this
    /// question answerable at all from a crate that — being the one every
    /// transport depends on — cannot depend on any of them.
    #[must_use]
    pub const fn is_linked(self) -> bool {
        match self {
            Self::WebSocket => cfg!(feature = "websocket"),
            Self::Fix => cfg!(feature = "fix"),
            Self::Multicast => cfg!(feature = "multicast"),
            Self::Rest => cfg!(feature = "rest"),
            Self::FileTail => cfg!(feature = "filetail"),
            Self::Uds => cfg!(feature = "uds"),
        }
    }

    /// The tokens of the transports this binary links, for an error message.
    #[must_use]
    pub fn linked_list() -> String {
        let linked: Vec<&str> = Self::ALL
            .iter()
            .filter(|kind| kind.is_linked())
            .map(|kind| kind.as_token())
            .collect();
        if linked.is_empty() {
            // Worth saying rather than printing an empty list: a binary linking
            // no transport crate at all is a build that forgot one, and the
            // message should read as that rather than as a spelling problem.
            "none - this binary links no transport crate from this family".to_string()
        } else {
            linked.join(", ")
        }
    }

    /// Resolve a configuration token, or say why it cannot be.
    ///
    /// The whole of `[ingress] kind`'s resolution: one total match over a
    /// closed set, two distinguishable failures, and no fallback. There is
    /// deliberately no default — a transport is the one thing about an upstream
    /// that cannot be guessed, and the audit's misspelled section became the
    /// wrong transport precisely because something defaulted.
    ///
    /// # Errors
    ///
    /// [`ConfigError::UnknownKind`] for a token no transport in the family
    /// answers to, naming the built-in set.
    /// [`ConfigError::KindNotLinked`] for one that does, when this binary was
    /// built without it, naming what it was built with.
    pub fn resolve(token: &str) -> Result<Self, ConfigError> {
        let kind = Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_token() == token)
            .ok_or_else(|| ConfigError::UnknownKind {
                token: token.to_string(),
            })?;
        if kind.is_linked() {
            Ok(kind)
        } else {
            Err(ConfigError::KindNotLinked {
                token: kind.as_token(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_no_transport_answers_to_names_the_built_in_set() {
        let error = Kind::resolve("websockets").expect_err("plural is not a transport");
        let message = error.to_string();
        // The value it rejected, and every value it would have accepted. Both,
        // because a message with only one of them is a message an operator has
        // to guess against.
        assert!(message.contains("websockets"), "{message}");
        for kind in Kind::ALL {
            assert!(message.contains(kind.as_token()), "{message}");
        }
    }

    #[test]
    fn every_token_resolves_to_the_variant_that_spells_it() {
        for kind in Kind::ALL {
            match Kind::resolve(kind.as_token()) {
                Ok(resolved) => assert_eq!(resolved, kind),
                // Not linked is the other correct answer, and which of the two
                // it is depends on the features of the build running this test.
                // What is asserted here is that no token is *unknown*.
                Err(ConfigError::KindNotLinked { token }) => assert_eq!(token, kind.as_token()),
                Err(other) => panic!("{} resolved to {other}", kind.as_token()),
            }
        }
    }

    #[test]
    fn no_two_transports_share_a_token() {
        let mut tokens: Vec<&str> = Kind::ALL.iter().map(|kind| kind.as_token()).collect();
        tokens.sort_unstable();
        let count = tokens.len();
        tokens.dedup();
        assert_eq!(tokens.len(), count, "two transports share a token");
    }

    #[test]
    fn the_token_list_in_the_error_message_is_the_token_set() {
        // The list is a literal so that it can be a `&'static str` in the error
        // format string. This is what keeps it from drifting from `ALL`.
        let built: Vec<&str> = Kind::ALL.iter().map(|kind| kind.as_token()).collect();
        assert_eq!(Kind::TOKEN_LIST, built.join(", "));
    }
}
