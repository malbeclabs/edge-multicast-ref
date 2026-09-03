//! A publisher that actually sends, over a real socket, to a real group.
//!
//! Everything else in this workspace is tested against fakes: sockets behind
//! traits, injected clocks, recording sinks. That is deliberate and it is what
//! makes the suite run unprivileged with no network — but it means thirteen
//! commits of tests are a hypothesis until a byte leaves a socket.
//!
//! This is the byte. It composes the real runtime over real
//! `MulticastTransmitter`s, publishes a known set of events, and exits. What
//! reads the other end is not this crate's business: `examples/loopback.sh`
//! points the repository's Go subscriber at it, which makes the check
//! cross-language rather than an agreement between our own encoder and decoder.
//!
//! ```text
//! cargo run -p dz-publisher-runtime --example loopback_publisher -- \
//!     --group 233.252.0.9 --mktdata-port 41003 --refdata-port 41004
//! ```

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use dz_adapter_core::{
    Adapter, Aggressor, AssetClass, EventSink, InstrumentRef, InstrumentSpec, ListingSink,
    MarketModel, ParseError, Payload, PriceBound, Scalar, SettleType, SideUpdate, TradeFlags,
};
use dz_edge_core::PortRole;
use dz_edge_tob::TopOfBook;
use dz_publisher_egress::{
    EgressPolicy, EraStore, FailureScope, KernelRoute, MulticastTransmitter, Tee,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_refdata::{CycleSchedule, FileStore, Registry, RegistryConfig, SelectionPolicy};
use dz_publisher_runtime::config::{Feed, FeedSpec};
use dz_publisher_runtime::pipeline::{FeedPipeline, Port, Ports};
use dz_publisher_runtime::publisher::{Feeds, Publisher};
use dz_publisher_runtime::{Exit, SystemClock};

/// The one instrument this publishes, and the values a subscriber will see.
const SYMBOL: &str = "LOOPBACK-1";
const INSTRUMENT_ID: u32 = 1;
const SOURCE_ID: u16 = 1;
const CHANNEL_ID: u8 = 0;
const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;

/// An adapter that offers one instrument and emits three events.
///
/// A real venue's adapter in place of this is the only difference between this
/// example and a publisher: everything below it is the shared runtime.
struct Fixture {
    offered: bool,
    handle: Option<InstrumentRef>,
    emitted: bool,
}

impl Adapter for Fixture {
    fn message_types(&self) -> &[&'static str] {
        &["fixture"]
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        if self.offered {
            return;
        }
        self.offered = true;
        self.handle = out.list(&InstrumentSpec {
            symbol: SYMBOL,
            leg1: None,
            leg2: None,
            asset_class: AssetClass::CryptoSpot,
            price_exponent: PRICE_EXPONENT,
            qty_exponent: QTY_EXPONENT,
            market_model: MarketModel::Clob,
            tick_size: Scalar::text("0.0001"),
            lot_size: Scalar::text("0.01"),
            contract_value: None,
            quoted_per_contract: None,
            expiry_ns: None,
            settle_type: SettleType::Cash,
            price_bound: PriceBound::NonNegative,
        });
    }

    fn on_payload(
        &mut self,
        _payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let Some(instrument) = self.handle else {
            return Ok(());
        };
        if self.emitted {
            return Ok(());
        }
        self.emitted = true;
        out.upstream_message("fixture");

        // A two-sided quote. 999.95 / 1000.05 at the price exponent, 125.00 /
        // 72.50 at the quantity exponent - the same values the cross-language
        // golden vector carries, so a subscriber's output can be read against
        // `testdata/golden/manifest.json`.
        out.event(dz_adapter_core::Event::Quote {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_000,
            bid: SideUpdate::Present {
                px: Scalar::text("999.95"),
                qty: Scalar::text("125.00"),
                source_count: Some(3),
            },
            ask: SideUpdate::Present {
                px: Scalar::text("1000.05"),
                qty: Scalar::text("72.50"),
                source_count: Some(4),
            },
        });

        // A one-sided quote, so the gone flag reaches a subscriber that has
        // never been sent one.
        out.event(dz_adapter_core::Event::Quote {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_002,
            bid: SideUpdate::Gone,
            ask: SideUpdate::Present {
                px: Scalar::text("1000.05"),
                qty: Scalar::text("72.50"),
                source_count: None,
            },
        });

        out.event(dz_adapter_core::Event::Trade {
            instrument,
            source_ts_ns: 1_700_000_000_000_000_001,
            px: Scalar::text("1000.00"),
            qty: Scalar::text("5.00"),
            aggressor: Aggressor::Buy,
            trade_id: Some(987_654_321),
            cumulative_volume: Some(Scalar::text("10000.00")),
            flags: TradeFlags {
                sweep: true,
                ..TradeFlags::NONE
            },
        });
        Ok(())
    }
}

fn arg(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == name {
            return args.next();
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let group: Ipv4Addr = arg("--group")
        .unwrap_or_else(|| "233.252.0.9".to_string())
        .parse()?;
    let mktdata_port: u16 = arg("--mktdata-port")
        .unwrap_or_else(|| "41003".to_string())
        .parse()?;
    let refdata_port: u16 = arg("--refdata-port")
        .unwrap_or_else(|| "41004".to_string())
        .parse()?;
    let state_dir = arg("--state-dir").unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("dz-loopback-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });

    let source_id = SourceId::new(SOURCE_ID).ok_or("the source registry reserves that id")?;
    let clock = SystemClock::default();
    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "loopback",
        source_id: SOURCE_ID,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &["fixture"],
        channel_ids: &[CHANNEL_ID],
        ingress_message_types: &["fixture"],
    }));

    // The transmitter resolves its own source address off the route to the
    // group, which is the discipline: an address a publisher was told rather
    // than one the send path would actually use is how a feed ends up
    // unjoinable.
    // **Pinned, and a real run is what showed why.** Left unpinned, the
    // transmitter resolves its source off the route to the group — which is the
    // discipline, and on this host that route leaves by the default interface.
    // A subscriber joined on loopback then hears nothing, and the publisher
    // reports a clean teardown, because nothing in the send path is wrong: the
    // two ends simply chose different interfaces. That is the failure mode the
    // pin exists for, and it is invisible to every test that holds a fake
    // socket.
    let pin: Ipv4Addr = arg("--pin")
        .unwrap_or_else(|| "127.0.0.1".to_string())
        .parse()?;
    let policy = EgressPolicy {
        pin: Some(pin),
        // No prefix declared: this is a loopback group, and the invariant a
        // prefix enforces is about a production route.
        expected_prefix: None,
        ttl: 1,
    };
    let route = KernelRoute;

    let open = |name: &'static str,
                role: PortRole,
                port: u16|
     -> Result<Port, Box<dyn std::error::Error>> {
        let transmitter = MulticastTransmitter::open(
            name,
            &policy,
            SocketAddrV4::new(group, port),
            role,
            FailureScope::Process,
            &route,
        )?;
        let endpoint = transmitter.endpoint();
        let mut tee = Tee::new(role, Arc::clone(&metrics));
        tee.add(Box::new(transmitter));
        Ok(Port {
            endpoint,
            sink: tee,
        })
    };

    let feed = Feed {
        spec: FeedSpec::TopOfBook,
        channel_id: CHANNEL_ID,
        source_id,
        group,
        mktdata_port,
        refdata_port,
        snapshot_port: None,
        // Top-of-book carries no snapshot port, so there is no rotation to
        // configure; a cycle here would be refused at load.
        snapshot_cycle: None,
        heartbeat_interval: Duration::from_secs(1),
        definition_cycle: Duration::from_secs(1),
        manifest_cadence: Duration::from_millis(200),
        idle_guard: Duration::from_secs(3600),
    };

    let era = EraStore::open(&state_dir)?.begin_era::<dz_edge_tob::TopOfBook>()?;
    eprintln!("era {} state {state_dir}", era.get());

    let ports = Ports {
        mktdata: open("mktdata", PortRole::Mktdata, mktdata_port)?,
        refdata: open("refdata", PortRole::Refdata, refdata_port)?,
        snapshot: None,
    };
    let feeds = Feeds {
        top_of_book: Some(FeedPipeline::<TopOfBook>::new(
            &feed,
            Arc::clone(&metrics),
            era,
            ports,
        )),
        market_by_price: None,
    };

    let registry = Registry::open(
        RegistryConfig {
            source_id,
            channel_id: CHANNEL_ID,
            selection: SelectionPolicy::new(8, 16, 8)?,
            schedule: CycleSchedule::new(feed.definition_cycle, 1232, 1),
        },
        FileStore::new(&state_dir),
        clock.clone(),
    )?;

    let mut publisher = Publisher::new(
        Arc::clone(&metrics),
        registry,
        clock.clone(),
        source_id,
        feeds,
        feed.idle_guard,
    );

    let mut adapter = Fixture {
        offered: false,
        handle: None,
        emitted: false,
    };

    // Admit the instrument, then let the definition cycle put its definition on
    // refdata before the quotes reference it — a subscriber that saw a quote for
    // an `Instrument ID` it holds no definition for has nothing to resolve.
    publisher.poll_listings(&mut adapter);
    for _ in 0..40 {
        if publisher.tick().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let payload = Payload {
        bytes: b"go",
        recv_ts_ns: 0,
        connection: dz_adapter_core::ConnectionId::new("fixture"),
    };
    // `Publisher` is itself the `EventSink`, so this is the whole ingress
    // path with the transport removed: bytes in, datagrams out.
    adapter.on_payload(&payload, &mut publisher)?;
    for _ in 0..8 {
        // A guard firing here is the run reporting itself, so it ends the loop
        // rather than being discarded.
        if publisher.tick().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let teardown = publisher.shut_down(Exit::Signal);
    eprintln!(
        "sent instrument_id {INSTRUMENT_ID}; teardown {:?}",
        teardown.steps()
    );
    Ok(())
}
