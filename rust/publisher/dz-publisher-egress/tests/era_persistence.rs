//! `Reset Count` across restarts, and what happens when the record is missing
//! or unreadable.

mod common;

use std::fs;

use dz_edge_core::ResetCount;
use dz_publisher_egress::{EraError, EraStore};

use common::{EscapingFeed, OtherFeed, TempStateDir, TestFeed};

/// The era a feed with no persisted history advertises. Transcribed from the
/// design: "`Reset Count` persists per feed, so a newly enabled feed advertises
/// 1 rather than inheriting another feed's history."
const FIRST: u8 = 1;

#[test]
fn a_feed_with_no_persisted_history_begins_at_one() {
    // Not zero: `ResetCount(0)` is what a channel that has never reset
    // advertises, and this publisher's first datagram has already invalidated
    // whatever a subscriber cached from a previous incarnation.
    let dir = TempStateDir::new("fresh");
    let store = EraStore::open(dir.path()).expect("open");

    assert_eq!(
        store.begin_era::<TestFeed>().expect("first era"),
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
        store.begin_era::<TestFeed>().expect("era")
    };
    // The store is dropped: the process is gone, and only the file remains.
    let second = {
        let store = EraStore::open(dir.path()).expect("reopen");
        store.begin_era::<TestFeed>().expect("era")
    };
    let third = {
        let store = EraStore::open(dir.path()).expect("reopen");
        store.begin_era::<TestFeed>().expect("era")
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
        store.begin_era::<TestFeed>().expect("era");
    }

    assert_eq!(
        store.persisted_era::<TestFeed>().expect("read"),
        Some(ResetCount(3))
    );
    assert_eq!(
        store.begin_era::<OtherFeed>().expect("era"),
        ResetCount(FIRST)
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
        .begin_era::<TestFeed>()
        .expect_err("a corrupt record must not be guessed at");

    assert!(matches!(error, EraError::Corrupt { .. }), "got {error:?}");
    assert_eq!(
        fs::read(&path).expect("read"),
        b"\x00\x01garbage",
        "the file an operator has to inspect must still be there"
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
    assert_eq!(store.begin_era::<TestFeed>().expect("era"), ResetCount(1));
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
        store.begin_era::<TestFeed>(),
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
    assert_eq!(store.begin_era::<TestFeed>().expect("era"), ResetCount(0));
    // And the one after it is 1, so the series keeps moving through the wrap.
    assert_eq!(store.begin_era::<TestFeed>().expect("era"), ResetCount(1));
}

#[test]
fn a_feed_name_that_is_not_one_path_component_is_refused() {
    // The feed name becomes a path. `Feed::NAME` is a constant in the codec
    // crates, which is exactly why nobody would think of it as a path
    // component, and the check costs nothing at startup.
    let dir = TempStateDir::new("unsafe-name");
    let store = EraStore::open(dir.path()).expect("open");

    assert!(matches!(
        store.begin_era::<EscapingFeed>(),
        Err(EraError::UnsafeFeedName { name: "../escape" })
    ));
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
        store.begin_era::<TestFeed>().expect("era"),
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
    let dir = TempStateDir::new("atomic");
    let store = EraStore::open(dir.path()).expect("open");
    store.begin_era::<TestFeed>().expect("era");

    let names: Vec<String> = fs::read_dir(dir.path())
        .expect("read dir")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into())
        .collect();
    assert_eq!(names, vec!["test-feed.era".to_owned()]);
}
