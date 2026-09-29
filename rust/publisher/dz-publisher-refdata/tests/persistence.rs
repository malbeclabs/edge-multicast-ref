//! What a mint writes, what a compaction keeps, and what forgetting a delisted
//! instrument costs.
//!
//! A venue that lists short-lived instruments mints forever. A mint must cost
//! one line whatever the history is, and the history must be allowed to end,
//! without an `Instrument ID` ever being re-issued.

use std::time::Duration;

use dz_adapter_core::{
    AssetClass, InstrumentRef, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar,
    SettleType,
};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::{
    encode_line, symbol_field, CycleSchedule, FileStore, ManualClock, MemoryStore, RecordError,
    RefdataError, Registry, RegistryConfig, SelectionPolicy, ShardConfig, StateError, StateRecord,
    StateStore, COMPACTION_FLOOR,
};

const SOURCE_ID: u16 = 7;
const HOUR_NS: u64 = 3_600 * 1_000_000_000;
/// A Unix reading well past zero, so a timestamp of 0 cannot pass for one.
const START_NS: u64 = 1_800_000_000 * 1_000_000_000;

fn spec(symbol: &str) -> InstrumentSpec<'_> {
    InstrumentSpec {
        symbol,
        leg1: None,
        leg2: None,
        asset_class: AssetClass::CryptoSpot,
        price_exponent: -4,
        qty_exponent: -2,
        market_model: MarketModel::Clob,
        tick_size: Scalar::text("0.0001"),
        lot_size: Scalar::text("0.01"),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::NotApplicable,
        price_bound: PriceBound::Unbounded,
    }
}

fn config(forget_delisted_after: Option<Duration>) -> RegistryConfig {
    RegistryConfig {
        source_id: SourceId::new(SOURCE_ID).expect("7 is an assigned production id"),
        shards: vec![ShardConfig::default_shard(3)],
        selection: SelectionPolicy::from_seed(8).expect("8 is a seed"),
        schedule: CycleSchedule::new(Duration::from_secs(30), 1232, 8),
        forget_delisted_after,
    }
}

/// One hour, which the tests below step the clock across.
const HORIZON: Option<Duration> = Some(Duration::from_secs(3_600));

fn clock_at(unix_ns: u64) -> ManualClock {
    let clock = ManualClock::new();
    clock.set_unix_ns(unix_ns);
    clock
}

fn opened<S: StateStore>(
    store: S,
    clock: &ManualClock,
    horizon: Option<Duration>,
) -> Registry<S, ManualClock> {
    let mut registry =
        Registry::open(config(horizon), store, clock.clone()).expect("the directory is usable");
    registry.seeding_complete();
    registry
}

fn id_of<S: StateStore>(registry: &Registry<S, ManualClock>, handle: InstrumentRef) -> u32 {
    registry
        .definition(handle)
        .expect("published")
        .instrument_id
}

/// List and delist `count` instruments no other test step names, one at a
/// time, which is what a venue of short-lived instruments does forever. The
/// selection policy's cap is never reached, because each is gone before the
/// next arrives.
fn churn<S: StateStore>(registry: &mut Registry<S, ManualClock>, prefix: &str, count: usize) {
    for n in 0..count {
        let symbol = format!("{prefix}-{n}");
        let handle = registry.list(&spec(&symbol)).expect("admitted");
        registry.delist(handle);
    }
}

fn lines(store: &MemoryStore) -> usize {
    store
        .record()
        .expect("persisted")
        .iter()
        .filter(|&&byte| byte == b'\n')
        .count()
}

// ---------------------------------------------------------------------------
// A mint is one line.
// ---------------------------------------------------------------------------

#[test]
fn a_mint_appends_one_line_and_rewrites_nothing() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, None);

    // The first write a directory ever sees is a base, because a line has
    // nothing to be appended to until one exists.
    registry.list(&spec("AAA")).expect("admitted");
    assert_eq!((store.stores(), store.appends()), (1, 0));

    // Every mint after it is one appended line, and the bytes appended are
    // exactly that line: nothing already in the record is written again.
    for (n, symbol) in ["BBB", "CCC", "DDD"].into_iter().enumerate() {
        let before = store.record().expect("persisted");
        let handle = registry.list(&spec(symbol)).expect("admitted");
        let id = id_of(&registry, handle);
        let after = store.record().expect("persisted");

        assert_eq!(store.stores(), 1, "nothing was rewritten for {symbol}");
        assert_eq!(store.appends(), n + 1);
        assert_eq!(&after[..before.len()], &before[..]);
        assert_eq!(
            &after[before.len()..],
            &encode_line(id, &symbol_field(symbol).0)[..]
        );
    }

    // A delisting and a re-offer write nothing at all.
    let handle = registry.list(&spec("AAA")).expect("the same instrument");
    registry.delist(handle);
    assert_eq!((store.stores(), store.appends()), (1, 3));
}

#[test]
fn a_venue_that_lists_forever_rewrites_the_record_a_bounded_number_of_times() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, None);

    let mints = 3 * COMPACTION_FLOOR;
    churn(&mut registry, "W", mints);

    // One base to start the record, and one each time the appended lines
    // reach the base: at the floor, and at twice the floor. A store per
    // mint would be 3,072.
    assert_eq!(store.stores(), 3);
    // Every mint but the first is appended, including the ones that set a
    // compaction off: the line is durable before the base is written.
    assert_eq!(store.appends(), mints - 1);

    // What was compacted is what was minted: every ID, once, and the next one
    // after them.
    let loaded = StateRecord::load(&store.record().expect("persisted")).expect("our own bytes");
    assert_eq!(loaded.record.entries.len(), mints);
    assert_eq!(
        loaded.record.next_id,
        u32::try_from(mints).expect("small") + 1
    );
    // The last base holds each delisting's time. The one exception is the
    // instrument whose mint set it off, which was published when it was
    // written; the lines appended since carry no time at all.
    let stamped = loaded
        .record
        .entries
        .iter()
        .filter(|entry| entry.delisted_at.is_some())
        .count();
    assert_eq!(stamped, loaded.base - 1);
}

// ---------------------------------------------------------------------------
// Forgetting.
// ---------------------------------------------------------------------------

#[test]
fn an_instrument_delisted_past_the_horizon_is_forgotten_and_its_id_is_not_reissued() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let old_id;
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("OLD")).expect("admitted");
        old_id = id_of(&registry, handle);
        registry.delist(handle);

        // Past the horizon, a compaction forgets it. The churn is what sets the
        // compaction off, as the venue's own listings would.
        clock.set_unix_ns(START_NS + 2 * HOUR_NS);
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);
        let record =
            StateRecord::decode(&store.record().expect("persisted")).expect("our own bytes");
        assert!(
            record
                .entries
                .iter()
                .all(|entry| entry.symbol != symbol_field("OLD").0),
            "the forgotten entry is gone from the record"
        );

        // Relisted, it is a new instrument: the old ID is not re-issued to it or
        // to anything else, because `next_id` never went back.
        let relisted = registry.list(&spec("OLD")).expect("admitted");
        let new_id = id_of(&registry, relisted);
        assert_ne!(new_id, old_id);
        assert_eq!(new_id, record.next_id);
    }

    // And the same across a restart: the record reads back with the relisted
    // symbol once, under its new ID.
    let registry = opened(store.clone(), &clock, HORIZON);
    drop(registry);
    let record = StateRecord::decode(&store.record().expect("persisted")).expect("our own bytes");
    let old: Vec<_> = record
        .entries
        .iter()
        .filter(|entry| entry.symbol == symbol_field("OLD").0)
        .collect();
    assert_eq!(old.len(), 1);
    assert_ne!(old[0].instrument_id, old_id);
}

#[test]
fn an_entry_forgotten_at_open_is_gone_from_the_record_before_its_symbol_is_minted_again() {
    // Forgotten across the restart, with no compaction while running. If the
    // record went on holding the old entry, the new mint of the same symbol
    // would be in it twice and the next start would refuse it as damaged.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("OLD")).expect("admitted");
        registry.delist(handle);
        // A delisting's time reaches the record only through a base.
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    }

    clock.set_unix_ns(START_NS + 2 * HOUR_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("OLD")).expect("admitted");
        assert_ne!(id_of(&registry, handle), 1);
    }
    let restarted = Registry::open(config(HORIZON), store, clock.clone());
    assert!(restarted.is_ok(), "{:?}", restarted.err());
}

#[test]
fn an_instrument_delisted_inside_the_horizon_keeps_its_id() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, HORIZON);
    let handle = registry.list(&spec("KEPT")).expect("admitted");
    let id = id_of(&registry, handle);
    registry.delist(handle);

    clock.set_unix_ns(START_NS + HOUR_NS / 2);
    churn(&mut registry, "W", COMPACTION_FLOOR + 1);

    let relisted = registry.list(&spec("KEPT")).expect("admitted");
    assert_eq!(id_of(&registry, relisted), id);
}

#[test]
fn a_published_instrument_is_never_forgotten_however_old_its_mint() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("LIVE")).expect("admitted");
        assert_eq!(id_of(&registry, handle), 1);

        clock.set_unix_ns(START_NS + 10 * HOUR_NS);
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);
        let again = registry.list(&spec("LIVE")).expect("still published");
        assert_eq!(id_of(&registry, again), 1);
    }

    // It was written as published, so a restart well past the horizon keeps it
    // until the venue has had its chance to offer it again.
    clock.set_unix_ns(START_NS + 20 * HOUR_NS);
    let mut restarted = opened(store, &clock, HORIZON);
    let handle = restarted.list(&spec("LIVE")).expect("admitted");
    assert_eq!(id_of(&restarted, handle), 1);
}

#[test]
fn an_entry_published_when_the_publisher_stopped_is_measured_from_the_restart() {
    // Recorded as published, and not offered after the restart: when it was
    // last published is not known, and the restart is the latest it can have
    // been. Measuring from anything earlier forgets it early.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        registry.list(&spec("GONE")).expect("admitted");
        // Stopped with it published.
    }

    // Two hours later: past the horizon from the mint, not from the restart.
    clock.set_unix_ns(START_NS + 2 * HOUR_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        // A compaction while it is not offered writes it with the restart's
        // second.
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    }

    // Half an hour after that restart it is still inside the horizon.
    clock.set_unix_ns(START_NS + 2 * HOUR_NS + HOUR_NS / 2);
    let mut registry = opened(store, &clock, HORIZON);
    let handle = registry.list(&spec("GONE")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 1);
}

#[test]
fn a_relisting_the_record_holds_as_delisted_is_written_down_before_it_is_admitted() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("BACK")).expect("admitted");
        registry.delist(handle);
        // A base now holds it with the delisting's second.
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);

        clock.set_unix_ns(START_NS + HOUR_NS / 2);
        let appends = store.appends();
        let before = lines(&store);
        registry.list(&spec("BACK")).expect("relisted");
        assert_eq!(store.appends(), appends + 1, "one restating line");
        assert_eq!(lines(&store), before + 1);

        // Relisted again after an in-process delisting, the record already
        // holds it as published, and nothing is written.
        let handle = registry.list(&spec("BACK")).expect("published");
        registry.delist(handle);
        registry.list(&spec("BACK")).expect("relisted");
        assert_eq!(store.appends(), appends + 1);
        // Stopped with it published.
    }

    // Ninety minutes after the delisting the base holds: past the horizon
    // from that second. The restated line is what keeps it.
    clock.set_unix_ns(START_NS + HOUR_NS + HOUR_NS / 2);
    let mut registry = opened(store, &clock, HORIZON);
    let handle = registry.list(&spec("BACK")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 1);
}

#[test]
fn a_relisting_that_cannot_be_written_down_admits_nothing() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, HORIZON);
    let handle = registry.list(&spec("BACK")).expect("admitted");
    registry.delist(handle);
    churn(&mut registry, "W", COMPACTION_FLOOR + 1);

    store.break_writes("no space left on device");
    assert!(registry.list(&spec("BACK")).is_none());
    assert!(registry.fault().is_some());
}

// ---------------------------------------------------------------------------
// The record.
// ---------------------------------------------------------------------------

fn header(next_id: u32, entries: usize) -> String {
    format!("dz-refdata-state 2 {SOURCE_ID} {next_id} {entries}\n")
}

fn line(id: u32, symbol: &str) -> String {
    String::from_utf8(encode_line(id, &symbol_field(symbol).0)).expect("ASCII")
}

#[test]
fn a_record_folds_its_appended_lines_into_its_base() {
    let base_aaa = format!("{} 1800000000\n", line(1, "AAA").trim_end());
    let text = format!(
        "{}{base_aaa}{}{}",
        header(2, 1),
        line(2, "BBB"),
        line(1, "AAA")
    );
    let loaded = StateRecord::load(text.as_bytes()).expect("our own bytes");

    assert_eq!(loaded.record.next_id, 3, "the minting line advanced it");
    assert_eq!((loaded.base, loaded.appended), (1, 2));
    assert!(!loaded.torn && !loaded.needs_rewrite());
    assert_eq!(loaded.record.entries.len(), 2);
    assert!(
        loaded
            .record
            .entries
            .iter()
            .all(|entry| entry.delisted_at.is_none()),
        "AAA was restated as published"
    );
}

#[test]
fn a_base_round_trips_with_its_timestamps() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, None);
    churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    let bytes = store.record().expect("persisted");
    let record = StateRecord::decode(&bytes).expect("our own bytes");
    assert_eq!(StateRecord::decode(&record.encode()), Ok(record));
}

#[test]
fn a_version_1_record_reads_as_published_and_is_rewritten_on_open() {
    let text = format!(
        "dz-refdata-state 1 {SOURCE_ID} 3\n{}{}",
        line(1, "AAA"),
        line(2, "BBB")
    );
    let store = MemoryStore::new();
    store.set_record(text.into_bytes());
    let clock = clock_at(START_NS);

    let registry = opened(store.clone(), &clock, HORIZON);
    drop(registry);
    let loaded = StateRecord::load(&store.record().expect("persisted")).expect("our own bytes");
    assert!(!loaded.needs_rewrite(), "rewritten as the current layout");
    assert_eq!(loaded.record.next_id, 3);
    assert!(loaded
        .record
        .entries
        .iter()
        .all(|entry| entry.delisted_at.is_none()));

    let mut registry = opened(store, &clock, HORIZON);
    let handle = registry.list(&spec("BBB")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 2);
}

#[test]
fn a_torn_final_line_is_dropped_and_the_next_mint_does_not_run_on_from_it() {
    // A mint whose append never completed: its admission never happened, so
    // nothing published depends on it.
    let partial = line(2, "BBB");
    let text = format!(
        "{}{}{}",
        header(1, 0),
        line(1, "AAA"),
        &partial[..partial.len() / 2]
    );
    let loaded = StateRecord::load(text.as_bytes()).expect("a torn tail is not damage");
    assert!(loaded.torn);
    assert_eq!(loaded.record.next_id, 2);

    let store = MemoryStore::new();
    store.set_record(text.into_bytes());
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, None);
        // Cut off, not rewritten: a rewrite needs room for the whole record,
        // and a torn line is what an append that ran the disk out of room
        // leaves.
        assert_eq!(
            store.record().expect("persisted"),
            format!("{}{}", header(1, 0), line(1, "AAA")).into_bytes()
        );
        assert_eq!(store.stores(), 0, "nothing was rewritten at open");
        let handle = registry.list(&spec("CCC")).expect("admitted");
        assert_eq!(
            id_of(&registry, handle),
            2,
            "the torn ID was never admitted"
        );
    }
    let loaded = StateRecord::load(&store.record().expect("persisted")).expect("our own bytes");
    assert!(!loaded.torn);
    assert_eq!(loaded.record.entries.len(), 2);
}

#[test]
fn a_torn_final_line_is_cut_off_a_real_record_in_place() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let whole = format!("{}{}", header(1, 0), line(1, "AAA"));
    let partial = line(2, "BBB");
    std::fs::write(
        dir.path().join("instruments.state"),
        format!("{whole}{}", &partial[..partial.len() / 2]),
    )
    .expect("writable");

    let clock = clock_at(START_NS);
    drop(opened(FileStore::new(dir.path()), &clock, None));
    assert_eq!(
        std::fs::read(dir.path().join("instruments.state")).expect("readable"),
        whole.into_bytes()
    );
}

#[test]
fn an_append_whose_flush_fails_costs_an_id_and_admits_nothing() {
    // A failed `sync_data` does not take back the write before it, so the whole
    // line can be in the record after the append returned an error.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(store.clone(), &clock, None);
        registry.list(&spec("AAA")).expect("admitted");
        store.break_flushes("input/output error");
        let appends = store.appends();
        assert!(registry.list(&spec("BBB")).is_none(), "nothing is admitted");
        assert_eq!(store.appends(), appends + 1, "the line is in the record");
        assert!(registry.fault().is_some());
        assert!(registry.list(&spec("CCC")).is_none(), "nothing further");
    }
    store.repair_writes();

    // The line reads back as the mint it is. The ID it holds was never
    // published, and it is not handed to anything else.
    let mut registry = opened(store, &clock, None);
    let handle = registry.list(&spec("CCC")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 3);
    let handle = registry.list(&spec("BBB")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 2);
}

#[test]
fn a_delisting_late_in_a_second_is_not_forgotten_early_in_the_next() {
    // A whole second rounded down would record 10.999 s as 10, and a
    // one-second horizon would forget it at 11.000 s, a millisecond later.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS + 999_999_999);
    let mut registry = opened(store.clone(), &clock, Some(Duration::from_secs(1)));
    let handle = registry.list(&spec("LATE")).expect("admitted");
    let id = id_of(&registry, handle);
    registry.delist(handle);

    clock.set_unix_ns(START_NS + 1_000_000_000);
    churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    let relisted = registry.list(&spec("LATE")).expect("admitted");
    assert_eq!(id_of(&registry, relisted), id);
}

#[test]
fn a_horizon_with_a_fraction_of_a_second_is_not_cut_short() {
    // `1500ms` rounded down is one second, and would forget at 1.2 s.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, Some(Duration::from_millis(1_500)));
    let handle = registry.list(&spec("FRAC")).expect("admitted");
    let id = id_of(&registry, handle);
    registry.delist(handle);

    clock.set_unix_ns(START_NS + 1_200_000_000);
    churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    let relisted = registry.list(&spec("FRAC")).expect("admitted");
    assert_eq!(id_of(&registry, relisted), id);
}

#[test]
fn a_record_that_is_complete_and_wrong_is_refused() {
    let refused = |text: String| StateRecord::load(text.as_bytes()).unwrap_err();

    // A complete final line that is malformed was written by something this
    // build did not write.
    assert!(matches!(
        refused(format!(
            "{}{}2 not-hexadecimal\n",
            header(1, 0),
            line(1, "AAA")
        )),
        RecordError::Malformed { .. }
    ));
    // A base is written whole, so a torn one is damage and not an append
    // that never completed.
    let aaa = line(1, "AAA");
    assert!(matches!(
        refused(format!("{}{}", header(2, 1), &aaa[..aaa.len() - 1])),
        RecordError::Malformed { .. }
    ));
    // A version 1 record is all base, so its final line torn is damage
    // too, and dropping it would lose an ID the upgrade must keep.
    assert!(matches!(
        refused(format!(
            "dz-refdata-state 1 {SOURCE_ID} 3\n{}{}",
            aaa,
            line(2, "BBB").trim_end()
        )),
        RecordError::Malformed { .. }
    ));
    // A final line with no newline that no append could have started: only
    // a prefix of a line this build appends is an append that never finished.
    let too_long = format!("1 {}", "0".repeat(129));
    for tail in ["not-an-append", "1 NOT-HEX", "1 2 3", " 00", &too_long] {
        assert!(
            matches!(
                refused(format!("{}{}{tail}", header(2, 1), line(1, "AAA"))),
                RecordError::Malformed { .. }
            ),
            "{tail:?}"
        );
    }
    // A base shorter than its header counts.
    assert!(matches!(
        refused(format!("{}{}", header(3, 2), line(1, "AAA"))),
        RecordError::Malformed { .. }
    ));
    // An appended mint that skips an ID.
    assert!(matches!(
        refused(format!("{}{}", header(1, 0), line(2, "AAA"))),
        RecordError::AppendedOutOfOrder {
            instrument_id: 2,
            next_id: 1,
            ..
        }
    ));
    // A restatement under another symbol.
    assert!(matches!(
        refused(format!(
            "{}{}{}",
            header(2, 1),
            line(1, "AAA"),
            line(1, "BBB")
        )),
        RecordError::RestatedUnderAnotherSymbol {
            instrument_id: 1,
            ..
        }
    ));
    // A timestamp on an appended line: a delisting writes nothing, so no
    // append ever carries one.
    assert!(matches!(
        refused(format!(
            "{}{} 1800000000\n",
            header(1, 0),
            line(1, "AAA").trim_end()
        )),
        RecordError::Malformed { .. }
    ));
    // A second mint of a symbol already in the record.
    assert!(matches!(
        refused(format!(
            "{}{}{}",
            header(2, 1),
            line(1, "AAA"),
            line(2, "AAA")
        )),
        RecordError::DuplicateSymbol {
            first: 1,
            second: 2
        }
    ));
}

#[test]
fn a_record_that_cannot_be_rewritten_on_open_stops_the_publisher_starting() {
    let text = format!("dz-refdata-state 1 {SOURCE_ID} 2\n{}", line(1, "AAA"));
    let store = MemoryStore::new();
    store.set_record(text.into_bytes());
    store.break_writes("read-only file system");

    let opened = Registry::open(config(None), store, clock_at(START_NS));
    assert!(matches!(opened, Err(RefdataError::State(_))));
}

// ---------------------------------------------------------------------------
// A compaction that cannot be written.
// ---------------------------------------------------------------------------

/// A record whose appended lines are due a compaction at open: an empty base
/// and one minting line per ID up to the floor.
fn due_a_compaction() -> String {
    let mut text = header(1, 0);
    for id in 1..=u32::try_from(COMPACTION_FLOOR).expect("small") {
        text.push_str(&line(id, &format!("S-{id}")));
    }
    text
}

#[test]
fn a_compaction_due_at_open_that_cannot_be_written_still_starts() {
    // A disk with room for a line and not for the whole record. What is there
    // reads back whole, so the publisher serves every ID it holds.
    let whole = due_a_compaction();
    let partial = line(9_999, "TORN");
    let store = MemoryStore::new();
    store.set_record(format!("{whole}{}", &partial[..partial.len() / 2]).into_bytes());
    store.break_stores("no space left on device");

    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, None);
    assert_eq!(store.stores(), 0);
    assert_eq!(
        store.record().expect("persisted"),
        whole.clone().into_bytes(),
        "the torn line is cut off, as it is when no compaction is due"
    );
    assert!(registry.fault().is_none());
    let handle = registry.list(&spec("S-5")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 5);
    let handle = registry.list(&spec("NEW")).expect("admitted");
    let floor = u32::try_from(COMPACTION_FLOOR).expect("small");
    assert_eq!(id_of(&registry, handle), floor + 1);
}

#[test]
fn a_base_that_may_have_replaced_the_record_stops_the_publisher_starting() {
    // A store that fails after the rename cannot say which record a later
    // load sees, so nothing can be appended to either with confidence.
    let store = MemoryStore::new();
    store.set_record(due_a_compaction().into_bytes());
    store.break_flushes("input/output error");

    let opened = Registry::open(config(None), store, clock_at(START_NS));
    assert!(matches!(opened, Err(RefdataError::State(_))));
}

#[test]
fn a_compaction_that_cannot_be_written_while_running_faults_nothing_and_waits_a_threshold() {
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let mut registry = opened(store.clone(), &clock, None);
    // The first mint is the base, and each one after it a line: one short of
    // the floor.
    churn(&mut registry, "W", COMPACTION_FLOOR);
    assert_eq!(store.stores(), 1);

    store.break_stores("no space left on device");
    churn(&mut registry, "X", 1);
    assert!(
        registry.fault().is_none(),
        "the line it set off from landed"
    );
    assert_eq!(store.stores(), 1);
    store.repair_writes();

    // Not tried again on the next mint: a full disk asked for the whole record
    // on every mint would make each one cost the history.
    churn(&mut registry, "Y", COMPACTION_FLOOR - 1);
    assert_eq!(store.stores(), 1);
    churn(&mut registry, "Z", 1);
    assert_eq!(store.stores(), 2);

    drop(registry);
    let loaded = StateRecord::load(&store.record().expect("persisted")).expect("our own bytes");
    assert_eq!(loaded.record.entries.len(), 2 * COMPACTION_FLOOR + 1);
}

#[test]
fn an_entry_is_forgotten_only_by_a_base_that_lands() {
    // An entry dropped from memory and left in the record would be minted a
    // second entry for its symbol on relisting, and the next start would
    // refuse the record as damaged.
    let store = MemoryStore::new();
    let clock = clock_at(START_NS);
    let old_id;
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("OLD")).expect("admitted");
        old_id = id_of(&registry, handle);
        registry.delist(handle);
        // A delisting's time reaches the record only through a base.
        churn(&mut registry, "W", COMPACTION_FLOOR + 1);
    }

    clock.set_unix_ns(START_NS + 2 * HOUR_NS);
    store.break_stores("no space left on device");
    {
        let mut registry = opened(store.clone(), &clock, HORIZON);
        let handle = registry.list(&spec("OLD")).expect("admitted");
        assert_eq!(id_of(&registry, handle), old_id, "still in the record");
    }
    store.repair_writes();

    let mut registry =
        Registry::open(config(HORIZON), store, clock.clone()).expect("the record holds OLD once");
    registry.seeding_complete();
    let handle = registry.list(&spec("OLD")).expect("admitted");
    assert_eq!(id_of(&registry, handle), old_id);
}

// ---------------------------------------------------------------------------
// The real directory.
// ---------------------------------------------------------------------------

#[test]
fn an_append_after_a_base_lands_in_the_record_that_replaced_the_old_one() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mut store = FileStore::new(dir.path());
    store.claim().expect("unclaimed");

    store.store(b"first\n").expect("writable");
    store.append(b"a\n").expect("writable");
    // The rename leaves the append handle on the old inode unless it is
    // dropped; a line written there is one no read will ever see.
    store.store(b"second\n").expect("writable");
    store.append(b"b\n").expect("writable");

    assert_eq!(
        store.load().expect("readable").expect("present"),
        b"second\nb\n"
    );
}

#[test]
fn a_base_that_cannot_be_renamed_leaves_the_record_and_no_pending_file() {
    // A directory in the record's place refuses the rename. The pending file
    // goes with the failure, or on a full disk it would hold the room the
    // appends after it need.
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::fs::create_dir(dir.path().join("instruments.state")).expect("writable");
    std::fs::write(dir.path().join("instruments.state").join("held"), b"").expect("writable");
    let mut store = FileStore::new(dir.path());
    store.claim().expect("unclaimed");

    let stored = store.store(b"record\n");
    assert!(
        matches!(stored, Err(StateError::NotReplaced(_))),
        "{stored:?}"
    );
    assert!(!dir.path().join("instruments.state.pending").exists());
    assert!(dir.path().join("instruments.state").is_dir());
}

#[test]
fn a_registry_over_a_real_directory_reads_back_what_it_appended() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let clock = clock_at(START_NS);
    {
        let mut registry = opened(FileStore::new(dir.path()), &clock, None);
        for symbol in ["AAA", "BBB", "CCC"] {
            registry.list(&spec(symbol)).expect("admitted");
        }
    }
    let mut registry = opened(FileStore::new(dir.path()), &clock, None);
    let handle = registry.list(&spec("CCC")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 3);
    let handle = registry.list(&spec("DDD")).expect("admitted");
    assert_eq!(id_of(&registry, handle), 4);
}
