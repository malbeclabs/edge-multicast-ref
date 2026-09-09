//! What the built-in adapter says about where an instrument goes.
//!
//! A record names a symbol and nothing else, so the partition is configuration's
//! — one shard per `[[adapter.upstream.listing]]`. This is the whole of what
//! this adapter decides about routing: it passes the name on, and a name it was
//! given no channel for costs that listing rather than the poll.
#![forbid(unsafe_code)]

use dz_adapter_core::{
    Adapter as _, AssetClass, InstrumentRef, InstrumentSpec, ListingSink, MarketModel, PriceBound,
    SettleType, DEFAULT_SHARD,
};
use dz_adapter_uds::{UdsAdapter, UdsListing};

/// A listing for `symbol`, on `shard`, with the rest of the fields fixed.
fn listing(symbol: &str, shard: Option<&str>) -> UdsListing {
    UdsListing {
        symbol: symbol.to_string(),
        shard: shard.map(str::to_string),
        leg1: None,
        leg2: None,
        asset_class: AssetClass::CryptoSpot,
        price_exponent: -4,
        qty_exponent: -2,
        market_model: MarketModel::Clob,
        tick_size: "0.0001".to_string(),
        lot_size: "0.01".to_string(),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::Cash,
        price_bound: PriceBound::NonNegative,
    }
}

/// Records the shard each offer named, and admits every one of them.
///
/// It implements `list_on` only, which is what the trait requires. The default
/// shard therefore arrives here as the token rather than as a second method,
/// which is what makes the third row below a statement about one spelling.
#[derive(Default)]
struct Heard {
    offers: Vec<(String, String)>,
}

impl ListingSink for Heard {
    fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        self.offers
            .push((shard.to_owned(), spec.symbol.to_owned()));
        Some(InstrumentRef::from_admission(self.offers.len() as u32 - 1))
    }
    fn delist(&mut self, _instrument: InstrumentRef) {}
}

/// Each listing is offered on the shard it names, and one that names none is
/// offered on the default.
///
/// Written as the whole sequence rather than as "alpha was offered": an adapter
/// that passed `DEFAULT_SHARD` for everything would satisfy any assertion about
/// the third row, and one that reached for the first listing's shard would
/// satisfy any assertion about the first. Three rows with three different
/// answers is what neither survives.
#[test]
fn each_listing_is_offered_on_the_shard_it_names() {
    let mut adapter = UdsAdapter::new(vec![
        listing("A-B", Some("alpha")),
        listing("B-D", Some("beta")),
        listing("C-F", None),
    ]);
    let mut heard = Heard::default();

    adapter.poll_listings(&mut heard);

    assert_eq!(
        heard.offers,
        vec![
            ("alpha".to_owned(), "A-B".to_owned()),
            ("beta".to_owned(), "B-D".to_owned()),
            (DEFAULT_SHARD.to_owned(), "C-F".to_owned()),
        ]
    );
    for symbol in ["A-B", "B-D", "C-F"] {
        assert!(adapter.handle(symbol).is_some(), "{symbol} kept no handle");
    }
}

/// A shard this publisher has no channel for costs its own listing and no other.
///
/// The runtime declines an unknown shard, and the record path then has no handle
/// for that symbol — which is the outcome the boundary documents: a `None` is
/// ordinary. What must not happen is the poll stopping, because the listings
/// after it are the ones on shards that do exist.
#[test]
fn a_declined_shard_costs_one_listing_and_the_poll_continues() {
    struct OnlyAlpha;
    impl ListingSink for OnlyAlpha {
        fn list_on(&mut self, shard: &str, _spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
            (shard == "alpha").then(|| InstrumentRef::from_admission(0))
        }
        fn delist(&mut self, _instrument: InstrumentRef) {}
    }

    let mut adapter = UdsAdapter::new(vec![
        listing("B-D", Some("beta")),
        listing("A-B", Some("alpha")),
    ]);

    adapter.poll_listings(&mut OnlyAlpha);

    assert!(
        adapter.handle("B-D").is_none(),
        "a symbol on a shard the publisher has no channel for holds a handle"
    );
    assert!(
        adapter.handle("A-B").is_some(),
        "the listing after the declined one was never offered"
    );
}
