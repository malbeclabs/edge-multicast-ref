//! `run()` end to end: the real entry point, offline.
//!
//! `examples/loopback_publisher.rs` composed a publisher by hand, which proved
//! the send path but left [`run`](dz_publisher_runtime::run) — the one function
//! a venue's `main` actually calls — with no live exercise at all. This closes
//! that: a real config document, the real registry, a real adapter reading real
//! recorded bytes, the real lowering, real sockets, and the real teardown.
//!
//! The adapter is the built-in one, so the record encoding is exercised too. A
//! venue's `main` differs from this file in one line: which constructor the
//! registry is given.
//!
//! ```text
//! cargo run -p dz-publisher-runtime --example replay_publisher -- <config.toml>
//! ```
//!
//! `examples/replay.sh` writes the config and the payloads, runs this, and
//! reads the other end with the repository's Go subscriber.

use std::process::ExitCode;

use dz_adapter_core::{
    AssetClass, ConnectionId, DisconnectReason, MarketModel, PriceBound, SettleType,
};
use dz_adapter_uds::{UdsAdapter, UdsListing};
use dz_ingress_core::{BoxFuture, IngressError, Input, Received, UpstreamMessage};
use dz_publisher_runtime::{AdapterInitError, AdapterRegistry, Venue};

/// The transport this venue would have built, which a replay run replaces.
///
/// It exists because `Venue` carries both halves and a venue always has a
/// transport — an offline run swaps it out, it does not remove it. Refusing to
/// connect is the honest behaviour: if a run reaches this, `[adapter.replay]`
/// was not enabled and the operator meant something else.
struct WouldConnect(ConnectionId);

impl Input for WouldConnect {
    fn connection(&self) -> ConnectionId {
        self.0
    }

    fn connect(
        &mut self,
        _timeout: std::time::Duration,
    ) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async {
            Err(IngressError::Fatal {
                detail: "this example has no live transport; enable [adapter.replay]".to_string(),
            })
        })
    }

    fn send<'a>(
        &'a mut self,
        _message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async {
            Err(IngressError::Ended {
                reason: DisconnectReason::RemoteClose,
                detail: "no transport".to_string(),
            })
        })
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<std::time::Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async {
            Err(IngressError::Ended {
                reason: DisconnectReason::RemoteClose,
                detail: "no transport".to_string(),
            })
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// The one instrument this run publishes, as `[adapter.upstream]` would state
/// it for a venue that discovers nothing.
fn listing(symbol: &str) -> UdsListing {
    UdsListing {
        symbol: symbol.to_string(),
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

/// What a venue's `main` looks like.
fn main() -> ExitCode {
    dz_publisher_runtime::run(AdapterRegistry::new().with("uds", |cx| {
        /// The venue's own keys, under `[adapter.upstream]`.
        #[derive(serde::Deserialize)]
        struct Upstream {
            /// The instruments this source will send records for. A source
            /// process and a publisher have to agree on these out of band;
            /// there is no discovery in the record encoding.
            symbols: Vec<String>,
        }

        let upstream: Upstream = cx
            .upstream()
            .map_err(|source| -> AdapterInitError { Box::new(source) })?;
        let listings = upstream.symbols.iter().map(|s| listing(s)).collect();

        Ok(Venue {
            adapter: Box::new(UdsAdapter::new(listings)),
            input: Box::new(WouldConnect(ConnectionId::new("records"))),
        })
    }))
}
