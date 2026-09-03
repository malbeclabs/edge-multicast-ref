//! The adapters this crate contains itself, and the one there is a reason for.
//!
//! # Why there is a built-in at all
//!
//! Every other adapter is a venue's own code, registered by the venue's own
//! `main`, which is the whole argument of [`crate::registry`]. The exception is
//! the one case where there is no venue code to register: **an integration that
//! is not Rust.** It cannot implement [`Adapter`](dz_adapter_core::Adapter), so
//! it runs as another process, writes normalized-event records, and
//! [`UdsAdapter`] reads them. Nothing about that adapter is venue-specific —
//! it resolves a symbol to a handle and hands the event on — so a venue
//! registering its own copy would be boilerplate with a chance of being wrong.
//!
//! It costs a serialization format and a copy per event, which is exactly why a
//! Rust venue should not use it.
//!
//! # A built-in is a registered kind, not a fallback
//!
//! It is resolved by name like any other, after the venue's own entries and
//! never instead of one, and a `kind` naming neither is still the startup error
//! that lists both sets. There is no default and nothing here is reached by a
//! misspelling. What a built-in changes is only that this binary knows one name
//! without being told it.
//!
//! # What is honestly missing, and what it does about it
//!
//! The transport half does not exist: `Kind::Uds` is a token in
//! [`dz_ingress_core::Kind`] with no crate behind it yet, so there is nothing to
//! read the socket a source process writes to. This built-in therefore composes
//! a [`Venue`] whose `Input` refuses to connect and **says why in the refusal**,
//! which leaves exactly one usable path today: `[adapter.replay]`, which
//! substitutes recorded bytes for the transport and is how the record encoding
//! is exercised end to end. That refusal is the honest state of this path; a
//! transport that silently connected to nothing would be worse than the error.

use std::time::Duration;

use dz_adapter_core::{AssetClass, ConnectionId, MarketModel, PriceBound, SettleType};
use dz_adapter_uds::{UdsAdapter, UdsListing};
use dz_ingress_core::{BoxFuture, IngressError, Input, Received, UpstreamMessage};
use serde::Deserialize;

use crate::error::AdapterInitError;
use crate::registry::{AdapterContext, Venue};

/// The `[adapter] kind` tokens this crate answers to itself.
///
/// Named in the error a bad `kind` produces, separately from the venue's own
/// entries, because *what is in this binary* has two sources and an operator
/// reading the message needs to know which one a name came from.
pub const BUILTIN_KINDS: &[&str] = &["uds"];

/// The connection name every record source reports under.
const CONNECTION: &str = "records";

/// Build the built-in adapter `cx.kind()` names, if it names one.
pub(crate) fn open(cx: &AdapterContext<'_>) -> Option<Result<Venue, AdapterInitError>> {
    match cx.kind() {
        "uds" => Some(uds(cx)),
        _ => None,
    }
}

/// `[adapter.upstream]` for the built-in record source.
///
/// # The listings are configuration, and they have to be
///
/// A record names a symbol, so this adapter can only resolve symbols it was
/// told to offer: there is no discovery in the encoding, and the source process
/// and the publisher agree on the set out of band. Stated in full here rather
/// than reduced to a list of symbols, because every field of it is something a
/// subscriber reads out of `InstrumentDefinition` — a symbol with a guessed
/// exponent is a definition that misdescribes every price on the feed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Upstream {
    /// One entry per instrument the source process will send records for.
    listing: Vec<Listing>,
}

/// One instrument, as `[[adapter.upstream.listing]]` states it.
///
/// A serde mirror of [`UdsListing`], and it has to be a mirror rather than a
/// derive on the original: `dz-adapter-core` depends on `thiserror` and nothing
/// else, and putting `serde` in it would put a serializer in every venue
/// repository that links the boundary. The copies are held to each other by
/// construction — [`Listing::into_uds`] names every field, so one added there
/// fails to compile here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Listing {
    symbol: String,
    #[serde(default)]
    leg1: Option<String>,
    #[serde(default)]
    leg2: Option<String>,
    asset_class: AssetClassToken,
    price_exponent: i8,
    qty_exponent: i8,
    market_model: MarketModelToken,
    tick_size: String,
    lot_size: String,
    #[serde(default)]
    contract_value: Option<String>,
    #[serde(default)]
    quoted_per_contract: Option<String>,
    #[serde(default)]
    expiry_ns: Option<u64>,
    settle_type: SettleTypeToken,
    price_bound: PriceBoundToken,
}

impl Listing {
    fn into_uds(self) -> UdsListing {
        UdsListing {
            symbol: self.symbol,
            leg1: self.leg1,
            leg2: self.leg2,
            asset_class: self.asset_class.into(),
            price_exponent: self.price_exponent,
            qty_exponent: self.qty_exponent,
            market_model: self.market_model.into(),
            tick_size: self.tick_size,
            lot_size: self.lot_size,
            contract_value: self.contract_value,
            quoted_per_contract: self.quoted_per_contract,
            expiry_ns: self.expiry_ns,
            settle_type: self.settle_type.into(),
            price_bound: self.price_bound.into(),
        }
    }
}

/// The four vocabularies below are the reference-data specification's own
/// tables, spelled as configuration tokens.
///
/// `deny_unknown_fields` has no equivalent for an enumeration, so each is a
/// closed set of `snake_case` tokens and a value outside it is a load error
/// naming what would have been accepted. A venue cannot state a number here,
/// which is the same protection [`AssetClass`] gives an adapter against
/// numbering a wire table wrongly.
macro_rules! token_enum {
    ($(#[$meta:meta])* $name:ident => $target:ty { $($token:ident => $variant:ident),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum $name {
            $($token),+
        }

        impl From<$name> for $target {
            fn from(token: $name) -> Self {
                match token {
                    $($name::$token => Self::$variant),+
                }
            }
        }
    };
}

token_enum! {
    /// `Asset Class`.
    AssetClassToken => AssetClass {
        Unknown => Unknown,
        CryptoSpot => CryptoSpot,
        PredictionBinary => PredictionBinary,
        PredictionScalar => PredictionScalar,
        PredictionCategorical => PredictionCategorical,
        PerpetualFuture => PerpetualFuture,
    }
}

token_enum! {
    /// How the venue matches.
    MarketModelToken => MarketModel {
        Unknown => Unknown,
        Clob => Clob,
        Amm => Amm,
    }
}

token_enum! {
    /// How the instrument settles.
    SettleTypeToken => SettleType {
        NotApplicable => NotApplicable,
        Cash => Cash,
        Physical => Physical,
    }
}

token_enum! {
    /// The range prices may take.
    PriceBoundToken => PriceBound {
        Unbounded => Unbounded,
        UnitInterval => UnitInterval,
        NonNegative => NonNegative,
    }
}

fn uds(cx: &AdapterContext<'_>) -> Result<Venue, AdapterInitError> {
    let upstream: Upstream = cx
        .upstream()
        .map_err(|source| -> AdapterInitError { Box::new(source) })?;
    let listings: Vec<UdsListing> = upstream
        .listing
        .into_iter()
        .map(Listing::into_uds)
        .collect();
    // Refused here rather than at the first poll: an adapter offering nothing
    // publishes nothing, and a source process writing records for symbols the
    // publisher never admitted is a feed that looks alive and carries no
    // instrument. A startup failure names the section; a silent empty set does
    // not.
    if listings.is_empty() {
        return Err(Box::new(EmptyListings));
    }
    // One source per declared `[[source]]`, or the one implicit connection a
    // document without them has. Every one of them refuses at connect, and each
    // is named as the document named it — a refusal counted under a label that
    // is not the operator's own name for the connection is a refusal they
    // cannot find.
    let sources: Vec<Box<dyn Input>> = if cx.sources().is_empty() {
        vec![Box::new(NoTransport(ConnectionId::new(CONNECTION)))]
    } else {
        cx.sources()
            .iter()
            .map(|source| Box::new(NoTransport(source.connection)) as Box<dyn Input>)
            .collect()
    };
    Ok(Venue {
        adapter: Box::new(UdsAdapter::new(listings)),
        sources,
    })
}

/// `[adapter.upstream]` named no instrument.
#[derive(Debug, thiserror::Error)]
#[error(
    "`[adapter.upstream]` names no `[[adapter.upstream.listing]]`: the record source and this \
     publisher have to agree on the instrument set out of band, and there is no discovery in the \
     record encoding"
)]
struct EmptyListings;

/// The transport the record source would be read on, which does not exist yet.
///
/// It refuses at connect and names both the reason and the path that does work.
/// Not a silent no-op and not an empty stream: a publisher that connected to
/// nothing and stayed up is the shape of a healthy feed with no data, which is
/// the one failure this whole family of crates is built to make impossible.
#[derive(Debug)]
struct NoTransport(ConnectionId);

/// The refusal, in one place so `connect` and `recv` cannot drift.
fn unbuilt() -> IngressError {
    IngressError::Fatal {
        detail: "the `uds` transport is not built yet: `dz-ingress-uds` does not exist, so the \
                 built-in record adapter can only be driven by `[adapter.replay]`"
            .to_owned(),
    }
}

impl Input for NoTransport {
    fn connection(&self) -> ConnectionId {
        self.0
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        // `Fatal` and not `Connect`: a transport that does not exist is not a
        // connection worth retrying under a backoff forever, and the driver
        // stops on it rather than looping.
        Box::pin(async { Err(unbuilt()) })
    }

    fn send<'a>(
        &'a mut self,
        _message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async { Err(unbuilt()) })
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async { Err(unbuilt()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
