//! `Reset Count` across restarts, and what happens when the record is missing
//! or unreadable.
//!
//! The era is keyed on a feed specification and a shard, so most of what is
//! asserted here is an *absence*: an era one shard cannot move, a stride no
//! other shard's presence can change, and a file the shard key does not rename.

mod common;

use std::fs;

use dz_edge_core::ResetCount;
use dz_publisher_egress::{EraError, EraStore, Shard};

use common::{EscapingFeed, OtherFeed, TempStateDir, TestFeed};

/// The era a channel instance with no persisted history advertises. Transcribed
/// from the design: "`Reset Count` persists per channel instance, so a newly
/// enabled one advertises 1 rather than inheriting another's history."
const FIRST: u8 = 1;

/// A shard as a document would name it. Two of them, because every property
/// worth asserting here is about one shard not moving another.
const ALPHA: Shard<'static> = Shard::named("alpha");
const BETA: Shard<'static> = Shard::named("beta");

/// The file names of everything under a state directory, sorted, so that a test
/// asserting a path asserts the whole directory rather than the one entry it
/// went looking for.
fn file_names(dir: &TempStateDir) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir.path())
        .expect("read dir")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into())
        .collect();
    names.sort();
    names
}

#[test]
fn a_feed_with_no_persisted_history_begins_at_one() {
    // Not zero: `ResetCount(0)` is what a channel that has never reset
    // advertises, and this publisher's first datagram has already invalidated
    // whatever a subscriber cached from a previous incarnation.
    let dir = TempStateDir::new("fresh");
    let store = EraStore::open(dir.path()).expect("open");

    assert_eq!(
        store
            .begin_era::<TestFeed>(Shard::DEFAULT)
            .expect("first era"),
        ResetCount(FIRST)
    );
}

#[test]
fn the_era_survives_a_restart_and_advances_on_each_one() {
    // The whole point of persisting it. A publisher whose sequence series
    // restarts at 0 while `Reset Count` stays put has told its subscribers
    // nothing: they keep the stale book and apply fresh deltas onto it.
    let dir = TempStateDir::new("restart");

    let first = {
        let store = EraStore::open(dir.path()).expect("open");
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era")
    };
    // The store is dropped: the process is gone, and only the file remains.
    let second = {
        let store = EraStore::open(dir.path()).expect("reopen");
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era")
    };
    let third = {
        let store = EraStore::open(dir.path()).expect("reopen");
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era")
    };

    assert_eq!(
        [first, second, third],
        [ResetCount(1), ResetCount(2), ResetCount(3)]
    );
}

#[test]
fn a_newly_enabled_feed_does_not_inherit_another_feeds_era() {
    // The reason the store is keyed per feed rather than per publisher. A feed
    // enabled today must advertise 1, not the era a feed that has published for
    // months has reached: its first datagram would otherwise claim a history it
    // does not have, and a subscriber that had been listening to the *other*
    // feed's era sees no change at all.
    let dir = TempStateDir::new("per-feed");
    let store = EraStore::open(dir.path()).expect("open");

    for _ in 0..3 {
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era");
    }

    assert_eq!(
        store
            .persisted_era::<TestFeed>(Shard::DEFAULT)
            .expect("read"),
        Some(ResetCount(3))
    );
    assert_eq!(
        store.begin_era::<OtherFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(FIRST)
    );
}

#[test]
fn a_newly_named_shard_does_not_inherit_another_shards_era() {
    // The same argument with the third noun, and the reason the key is the
    // whole channel instance. A shard configured today must advertise 1, not
    // the era the shard beside it has reached over months of publishing: the
    // two are separate channel instances and no subscriber holds state under
    // both.
    let dir = TempStateDir::new("per-shard");
    let store = EraStore::open(dir.path()).expect("open");

    for _ in 0..3 {
        store.begin_era::<TestFeed>(ALPHA).expect("era");
    }

    assert_eq!(
        store.persisted_era::<TestFeed>(ALPHA).expect("read"),
        Some(ResetCount(3))
    );
    assert_eq!(
        store.begin_era::<TestFeed>(BETA).expect("era"),
        ResetCount(FIRST)
    );
}

#[test]
fn an_existing_deployments_era_file_is_read_rather_than_restarted() {
    // The one property this key change exists to preserve. The file is written
    // here rather than through the store, because a store that made its own
    // file is not an existing installation: every deployment running today has
    // `<spec>.era` on disk, written before a document could name a shard.
    //
    // Were the default shard's path to become `<spec>.default.era`, the file
    // below would read as *no file*, which resolves to `FIRST_ERA`. A publisher
    // on era 7 would restart on era 1 and announce nothing — a subscriber
    // detects a reset by inequality against what it last saw, and 1 is a value
    // it may well hold. That is the silent corruption the corrupt-file refusal
    // exists to prevent, delivered by the upgrade meant to be safe.
    let dir = TempStateDir::new("upgrade");
    fs::write(dir.path().join("test-feed.era"), b"era-v1 7\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");

    assert_eq!(
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(8),
        "the default shard reads the file the upgrade inherited"
    );
    assert_eq!(
        store.begin_era::<TestFeed>(ALPHA).expect("era"),
        ResetCount(FIRST),
        "a shard the document has just named has no history to inherit"
    );
}

#[test]
fn the_default_shards_own_name_resolves_to_the_shard_that_keeps_its_file() {
    // The one call that could still deliver the rename. A document naming no
    // shard resolves to the default shard's token, so a caller that hands that
    // token straight to `Shard::named` writes `<spec>.default.era` and every
    // deployment on disk restarts at 1. `Shard::resolve` is the mapping, and
    // what it has to produce is a shard indistinguishable from `Shard::DEFAULT`
    // — the same file, the same history.
    const DEFAULT_TOKEN: &str = "default";

    let dir = TempStateDir::new("resolve");
    fs::write(dir.path().join("test-feed.era"), b"era-v1 7\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");

    assert_eq!(
        store
            .begin_era::<TestFeed>(Shard::resolve(DEFAULT_TOKEN, DEFAULT_TOKEN))
            .expect("era"),
        ResetCount(8),
        "the default shard's own token must continue the file it already has"
    );
    assert_eq!(
        store
            .begin_era::<TestFeed>(Shard::resolve("alpha", DEFAULT_TOKEN))
            .expect("era"),
        ResetCount(FIRST),
        "and every other name is a shard of its own"
    );
    assert_eq!(
        file_names(&dir),
        vec!["test-feed.alpha.era".to_owned(), "test-feed.era".to_owned()]
    );
}

#[test]
fn two_shards_of_one_specification_advance_independently() {
    // Keyed on the specification alone, these two would draw from one counter:
    // the second block to be composed would get 2 on its first start, and every
    // start would advance the counter by two. Neither shard may move the other,
    // so the assertion is on both the value and the other shard's file.
    let dir = TempStateDir::new("independent");
    let store = EraStore::open(dir.path()).expect("open");

    assert_eq!(
        store.begin_era::<TestFeed>(ALPHA).expect("era"),
        ResetCount(FIRST)
    );
    let beta_record = dir.path().join("test-feed.beta.era");
    assert_eq!(
        store.begin_era::<TestFeed>(BETA).expect("era"),
        ResetCount(FIRST),
        "the second shard begins its own history rather than continuing alpha's"
    );

    let beta_bytes = fs::read(&beta_record).expect("read");
    for _ in 0..4 {
        store.begin_era::<TestFeed>(ALPHA).expect("era");
    }

    assert_eq!(
        store.persisted_era::<TestFeed>(BETA).expect("read"),
        Some(ResetCount(FIRST)),
        "four starts of alpha must leave beta where it was"
    );
    assert_eq!(
        fs::read(&beta_record).expect("read"),
        beta_bytes,
        "and must not have rewritten beta's record at all"
    );
}

#[test]
fn persisted_era_answers_for_one_shard_rather_than_for_the_specification() {
    // The diagnostic's question is "what era is this channel instance in", and
    // with several shards of one specification in one process a
    // per-specification answer describes none of them.
    let dir = TempStateDir::new("diagnostic");
    let store = EraStore::open(dir.path()).expect("open");

    for _ in 0..2 {
        store.begin_era::<TestFeed>(ALPHA).expect("era");
    }
    store.begin_era::<TestFeed>(BETA).expect("era");

    assert_eq!(
        [
            store.persisted_era::<TestFeed>(ALPHA).expect("read"),
            store.persisted_era::<TestFeed>(BETA).expect("read"),
            store
                .persisted_era::<TestFeed>(Shard::DEFAULT)
                .expect("read"),
        ],
        [Some(ResetCount(2)), Some(ResetCount(1)), None],
        "each shard answers for itself, and the one that never published for none"
    );
}

#[test]
fn each_shards_era_advances_by_one_a_start_in_whatever_order_the_shards_begin() {
    // The property the per-specification key loses, and it takes two starts to
    // see: one start cannot tell a stride of one from a stride of two. The
    // second start begins the shards in the opposite order, because document
    // order deciding which era a channel instance gets is a text edit that
    // hands one channel a value the other published under.
    let dir = TempStateDir::new("order");

    let first_start = {
        let store = EraStore::open(dir.path()).expect("open");
        [
            store.begin_era::<TestFeed>(ALPHA).expect("era"),
            store.begin_era::<TestFeed>(BETA).expect("era"),
        ]
    };
    let second_start = {
        let store = EraStore::open(dir.path()).expect("reopen");
        let beta = store.begin_era::<TestFeed>(BETA).expect("era");
        let alpha = store.begin_era::<TestFeed>(ALPHA).expect("era");
        [alpha, beta]
    };

    assert_eq!(first_start, [ResetCount(1), ResetCount(1)]);
    assert_eq!(second_start, [ResetCount(2), ResetCount(2)]);
}

#[test]
fn adding_or_removing_a_shard_does_not_change_the_era_the_others_see_next() {
    // Drawn from one counter, the stride is the number of shards configured, so
    // adding one moves every other shard's next era and eventually hands one an
    // era it has already published under. Editing the document must change
    // nothing but the shard edited.
    let dir = TempStateDir::new("resize");

    {
        let store = EraStore::open(dir.path()).expect("open");
        store.begin_era::<TestFeed>(ALPHA).expect("era");
        store.begin_era::<TestFeed>(BETA).expect("era");
    }
    // A third shard joins.
    let gamma = Shard::named("gamma");
    let with_gamma = {
        let store = EraStore::open(dir.path()).expect("reopen");
        [
            store.begin_era::<TestFeed>(ALPHA).expect("era"),
            store.begin_era::<TestFeed>(gamma).expect("era"),
            store.begin_era::<TestFeed>(BETA).expect("era"),
        ]
    };
    // And beta is taken out of the document again.
    let without_beta = {
        let store = EraStore::open(dir.path()).expect("reopen");
        [
            store.begin_era::<TestFeed>(ALPHA).expect("era"),
            store.begin_era::<TestFeed>(gamma).expect("era"),
        ]
    };

    assert_eq!(
        with_gamma,
        [ResetCount(2), ResetCount(FIRST), ResetCount(2)],
        "the new shard starts its own history and the other two advance by one"
    );
    assert_eq!(
        without_beta,
        [ResetCount(3), ResetCount(2)],
        "and a shard leaving the document is not a start for the ones that stay"
    );
}

#[test]
fn a_corrupt_record_refuses_to_start_and_is_left_for_an_operator() {
    // The stated decision, and the case where guessing is worst. A file exists,
    // so an era *was* in use and we cannot know which; picking one risks
    // re-advertising the era subscribers already hold state under, and their
    // barrier fires on a *change* — so no subscriber ever drops its stale book.
    // Refusing is loud and repairable. The file is not rewritten, because an
    // operator cannot repair what the publisher has already overwritten.
    let dir = TempStateDir::new("corrupt");
    let path = dir.path().join("test-feed.era");
    fs::write(&path, b"\x00\x01garbage").expect("write");

    let store = EraStore::open(dir.path()).expect("open");
    let error = store
        .begin_era::<TestFeed>(Shard::DEFAULT)
        .expect_err("a corrupt record must not be guessed at");

    assert!(matches!(error, EraError::Corrupt { .. }), "got {error:?}");
    assert_eq!(
        fs::read(&path).expect("read"),
        b"\x00\x01garbage",
        "the file an operator has to inspect must still be there"
    );
}

#[test]
fn a_named_shards_corrupt_record_refuses_to_start_and_says_which_shard() {
    // The refusal follows the key. A corrupt record is per channel instance, so
    // the path in the error is the one an operator has to repair — and it must
    // name the shard, because `<spec>.era` and `<spec>.alpha.era` are different
    // channel instances and repairing the wrong one changes nothing.
    let dir = TempStateDir::new("corrupt-shard");
    let path = dir.path().join("test-feed.alpha.era");
    fs::write(&path, b"era-v1 not-a-number\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");
    let error = store
        .begin_era::<TestFeed>(ALPHA)
        .expect_err("a corrupt record must not be guessed at");

    assert!(
        matches!(&error, EraError::Corrupt { path: reported, .. } if reported == &path),
        "got {error:?}"
    );
    assert_eq!(
        store.begin_era::<TestFeed>(BETA).expect("era"),
        ResetCount(FIRST),
        "one shard's unreadable record is not the other shards' refusal"
    );
}

#[test]
fn a_record_holding_era_zero_is_ordinary_and_the_next_one_is_one() {
    // A persisted 0 is what a channel recorded on its 256th era. Refusing it -
    // which this store did at first - would turn the wrap into a refusal to
    // start, once every 256 restarts, on a publisher that had done nothing
    // wrong.
    let dir = TempStateDir::new("zero");
    fs::write(dir.path().join("test-feed.era"), b"era-v1 0\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");
    assert_eq!(
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(1)
    );
}

#[test]
fn a_record_from_an_unknown_format_is_corrupt_rather_than_reinterpreted() {
    // The format tag exists so that a later format is distinguishable from a
    // corrupt file of this one, rather than being parsed as whichever fields
    // happen to line up.
    let dir = TempStateDir::new("tagged");
    fs::write(dir.path().join("test-feed.era"), b"era-v9 4\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");
    assert!(matches!(
        store.begin_era::<TestFeed>(Shard::DEFAULT),
        Err(EraError::Corrupt { .. })
    ));
}

#[test]
fn the_era_after_the_last_one_a_byte_can_hold_wraps_to_zero() {
    // `Reset Count` is a `u8` and the specification anticipates this wrap
    // rather than leaving it to be reasoned about: a subscriber detects a reset
    // by testing its last-seen value for *inequality*, and "any change,
    // including the 255 to 0 wrap, is a reset; never compare for ordering". So
    // 0 is never read as a claim about history - only against what that
    // subscriber last saw on that channel instance.
    //
    // Skipping 0 was written here first, on the reasoning that 0 is what a
    // channel advertises before it has ever reset. That reads the field as
    // ordered, which is the one thing the specification forbids, and it would
    // have put this store's sequence at odds with the codec's own
    // `ChannelSequence::begin_era`, which wraps.
    let dir = TempStateDir::new("wrap");
    fs::write(dir.path().join("test-feed.era"), b"era-v1 255\n").expect("write");

    let store = EraStore::open(dir.path()).expect("open");
    assert_eq!(
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(0)
    );
    // And the one after it is 1, so the series keeps moving through the wrap.
    assert_eq!(
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(1)
    );
}

#[test]
fn a_feed_name_that_is_not_one_path_component_is_refused() {
    // The feed name becomes a path. `Feed::NAME` is a constant in the codec
    // crates, which is exactly why nobody would think of it as a path
    // component, and the check costs nothing at startup.
    let dir = TempStateDir::new("unsafe-name");
    let store = EraStore::open(dir.path()).expect("open");

    assert!(matches!(
        store.begin_era::<EscapingFeed>(Shard::DEFAULT),
        Err(EraError::UnsafeFeedName { name: "../escape" })
    ));
}

#[test]
fn a_shard_name_that_is_not_one_path_component_is_refused() {
    // The shard name is the second half of the same path, and it arrives from a
    // document rather than from a constant, so it is the half an operator can
    // actually write. The document checks it at load; this is the check in the
    // function that builds the path, and it must hold on its own.
    let dir = TempStateDir::new("unsafe-shard");
    let store = EraStore::open(dir.path()).expect("open");

    for name in ["../escape", "Alpha", "two words", "alpha/beta", ""] {
        let error = store
            .begin_era::<TestFeed>(Shard::named(name))
            .expect_err("a shard name that is not one path component");
        assert!(
            matches!(&error, EraError::UnsafeShardName { name: refused } if refused == name),
            "{name:?} got {error:?}"
        );
    }
    assert!(
        file_names(&dir).is_empty(),
        "a refused name must not have reached the filesystem at all"
    );
}

#[test]
fn the_state_directory_is_created_rather_than_required() {
    // A first deployment has no state directory, and a publisher that refuses
    // to start until somebody mkdirs one has made its own first run a manual
    // step.
    let dir = TempStateDir::new("nested");
    let nested = dir.path().join("state").join("egress");
    let store = EraStore::open(&nested).expect("open creates the directory");

    assert_eq!(
        store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era"),
        ResetCount(FIRST)
    );
    assert!(nested.join("test-feed.era").is_file());
}

#[test]
fn a_record_is_not_left_beside_a_temporary_file() {
    // The write goes through a temporary file and a rename, so that a crash
    // mid-write leaves either the previous era or the new one. What must not
    // survive is the temporary itself: a stray `.tmp` beside the record is how
    // the next reader is left guessing which is authoritative.
    //
    // The two names are the whole file layout, asserted here rather than in a
    // test of its own: the default shard keeps `<spec>.era` and a named shard
    // adds its component to it.
    let dir = TempStateDir::new("atomic");
    let store = EraStore::open(dir.path()).expect("open");
    store.begin_era::<TestFeed>(Shard::DEFAULT).expect("era");
    store.begin_era::<TestFeed>(ALPHA).expect("era");

    assert_eq!(
        file_names(&dir),
        vec!["test-feed.alpha.era".to_owned(), "test-feed.era".to_owned()]
    );
}
