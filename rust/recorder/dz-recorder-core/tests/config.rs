//! The configuration is the one place an operator can make the recorder do
//! something other than what they believe it does, so every property asserted
//! here is one where a permissive parser would be silently wrong.

use dz_recorder_core::{CaptureMode, Compression, RecorderConfig};
use std::time::Duration;

fn example() -> String {
    std::fs::read_to_string("tests/fixtures/recorder_example.toml").unwrap()
}

#[test]
fn the_documented_example_parses() {
    let cfg = RecorderConfig::parse(&example()).unwrap();
    assert_eq!(cfg.site, "site-a");
    assert_eq!(cfg.feed.len(), 1);
    assert_eq!(cfg.feed[0].mktdata_port, 40000);
    assert_eq!(cfg.feed[0].refdata_port, 40001);
    assert_eq!(cfg.feed[0].snapshot_port, None);
    assert_eq!(cfg.capture.mode, CaptureMode::Afpacket);
    assert_eq!(cfg.archive.compression, Compression::Zstd);
    assert!(!cfg.health.walk_messages);
}

#[test]
fn an_unknown_key_is_a_load_failure() {
    // A misspelled section that parses cleanly and falls back to a default is
    // how a host runs the wrong transport while the operator believes otherwise.
    let err =
        RecorderConfig::parse("site='a'\nrecorder='b'\nenv='c'\nmodee='socket'\n").unwrap_err();
    assert!(err.to_string().contains("modee"), "{err}");
}

#[test]
fn an_unknown_key_inside_every_section_is_a_load_failure() {
    for (section, key) in [
        (
            "[[feed]]\nspec='s'\nmulticast_group='233.252.0.10'\nmktdata_port=1\nrefdata_port=2\n",
            "snapshotport='3'",
        ),
        ("[capture]\n", "moode='socket'"),
        ("[archive]\n", "rotate_byte='1MiB'"),
        ("[health]\n", "walk_message=true"),
        ("[metrics]\n", "listen='127.0.0.1:9100'"),
    ] {
        let text = format!("site='a'\nrecorder='b'\nenv='c'\n{section}{key}\n");
        let err = RecorderConfig::parse(&text).unwrap_err();
        let misspelling = key.split_once(['=']).unwrap().0;
        assert!(err.to_string().contains(misspelling), "{key}: {err}");
    }
}

#[test]
fn an_unknown_capture_mode_is_a_load_failure() {
    // The transport is the one key where a fallback would be invisible: socket
    // mode records what one socket survived, not what the network delivered.
    let text = "site='a'\nrecorder='b'\nenv='c'\n[capture]\nmode='afpackets'\n";
    assert!(RecorderConfig::parse(text).is_err());
}

#[test]
fn no_key_can_raise_the_datagram_size_cap() {
    // The cap is mandated by every feed spec. Configuration cannot reach it.
    let toml = example();
    assert!(
        !toml.contains("max_datagram"),
        "the cap is a constant, not a key"
    );
    assert!(
        !toml.contains("snaplen"),
        "the capture length is computed from it"
    );
    let cfg = RecorderConfig::parse(&toml).unwrap();
    // The longest headers, not the synthesised ones: 14 + 60 + 8. A capture
    // length of cap + 42 cuts the tail off a compliant datagram at the cap
    // whose IPv4 header carries options, and the recorder then counts that
    // datagram as a publisher over the cap — a finding it manufactured. This is
    // also the snaplen the archive declares in every interface block, and the
    // two disagreeing is a segment whose blocks are longer than it admits.
    assert_eq!(cfg.capture.snaplen(), dz_edge_core::MAX_DATAGRAM_SIZE + 82);
}

#[test]
fn an_unexpected_source_is_recorded_not_dropped() {
    // expected_sources gates counting and alerting, never the archive. A wrongly
    // recorded datagram is filterable afterwards; a wrongly dropped one is gone.
    let cfg = RecorderConfig::parse(&example()).unwrap();
    let feed = &cfg.feed[0];
    assert!(
        !feed.expected_sources.is_empty(),
        "or the assertion below is vacuous"
    );
    assert!(feed.expected_sources.is_empty() || feed.admits_every_source());
}

#[test]
fn the_config_hash_ignores_comments_and_key_order() {
    // The hash goes in the archive as provenance, so a finding is attributable
    // to a configuration. Reformatting the file must not invalidate that.
    let a = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n").unwrap();
    let b = RecorderConfig::parse("# note\nrecorder='r'\nenv='e'\nsite='s'\n").unwrap();
    assert_eq!(a.config_hash(), b.config_hash());
}

#[test]
fn the_config_hash_is_lowercase_hex_sha256() {
    let hash = RecorderConfig::parse(&example()).unwrap().config_hash();
    assert_eq!(hash.len(), 64);
    assert!(
        hash.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "{hash}"
    );
}

#[test]
fn the_config_hash_changes_when_behaviour_changes() {
    // The other half of the property: a hash that ignored a value would make
    // every archive attributable to a configuration that never ran.
    let a = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n").unwrap();
    let b = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[capture]\nmode='socket'\n")
        .unwrap();
    assert_ne!(a.config_hash(), b.config_hash());
}

#[test]
fn two_spellings_of_one_size_hash_the_same() {
    let a = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[capture]\nbuffer='64MiB'\n")
        .unwrap();
    let b =
        RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[capture]\nbuffer='65536KiB'\n")
            .unwrap();
    assert_eq!(a.config_hash(), b.config_hash());
}

#[test]
fn the_canonical_form_re_parses_to_the_same_configuration() {
    // What makes the hash meaningful: the bytes hashed are a configuration, not
    // a lossy rendering of one.
    let cfg = RecorderConfig::parse(&example()).unwrap();
    let round_tripped = RecorderConfig::parse(&cfg.canonical_toml()).unwrap();
    assert_eq!(cfg, round_tripped);
    assert_eq!(cfg.config_hash(), round_tripped.config_hash());
}

#[test]
fn sizes_and_intervals_parse_to_bytes_and_a_duration() {
    let cfg = RecorderConfig::parse(&example()).unwrap();
    assert_eq!(cfg.capture.buffer, 64 * 1024 * 1024);
    assert_eq!(cfg.archive.rotate_bytes, 256 * 1024 * 1024);
    assert_eq!(cfg.archive.staging_max, 64 * 1024 * 1024 * 1024);
    assert_eq!(cfg.archive.rotate_interval, Duration::from_secs(60));
}

#[test]
fn a_size_without_a_unit_is_a_load_failure() {
    // Both plausible guesses for a disk budget are wrong by orders of
    // magnitude, so the unit is required rather than assumed.
    let err = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[capture]\nbuffer='64'\n")
        .unwrap_err();
    assert!(err.to_string().contains("no unit"), "{err}");
}

#[test]
fn a_size_with_an_unknown_unit_is_a_load_failure() {
    for bad in ["64mb", "64 MiB", "64MB", "MiB", "64PiB"] {
        let text = format!("site='s'\nrecorder='r'\nenv='e'\n[archive]\nstaging_max='{bad}'\n");
        assert!(RecorderConfig::parse(&text).is_err(), "{bad} was accepted");
    }
}

#[test]
fn a_size_that_overflows_a_byte_count_is_a_load_failure() {
    let text = format!(
        "site='s'\nrecorder='r'\nenv='e'\n[archive]\nstaging_max='{}TiB'\n",
        u64::MAX
    );
    assert!(RecorderConfig::parse(&text).is_err());
}

#[test]
fn a_duration_without_a_unit_is_a_load_failure() {
    let err =
        RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[archive]\nrotate_interval='60'\n")
            .unwrap_err();
    assert!(err.to_string().contains("no unit"), "{err}");
}

#[test]
fn a_duration_with_an_unknown_unit_is_a_load_failure() {
    for bad in ["60sec", "60S", "1.5s", "s", "60 s"] {
        let text = format!("site='s'\nrecorder='r'\nenv='e'\n[archive]\nrotate_interval='{bad}'\n");
        assert!(RecorderConfig::parse(&text).is_err(), "{bad} was accepted");
    }
}

#[test]
fn a_number_where_a_size_belongs_is_a_load_failure() {
    // TOML would happily give an integer here, and an integer has no unit.
    let text = "site='s'\nrecorder='r'\nenv='e'\n[capture]\nbuffer=67108864\n";
    assert!(RecorderConfig::parse(text).is_err());
}

#[test]
fn the_port_role_tokens_are_the_spec_spellings() {
    // A port role with two spellings is a join that silently returns nothing.
    let toml = example();
    assert!(toml.contains("mktdata_port"));
    assert!(
        !toml.contains("marketdata"),
        "mktdata is the only spelling this code knows"
    );
}

#[test]
fn there_are_no_bucket_credential_or_endpoint_keys() {
    // The recorder does not upload. completed_dir is the whole interface to
    // whatever ships from it, and inventing a key here would invent a second.
    let toml = example();
    for absent in ["bucket", "endpoint", "credential", "access_key", "region"] {
        assert!(!toml.contains(absent), "{absent} is not a recorder key");
    }
    assert!(
        RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n[archive]\nbucket='b'\n").is_err()
    );
}

#[test]
fn a_configuration_without_an_archive_section_keeps_the_documented_defaults() {
    // The defaults are the spec's, so a host that states only its identity
    // behaves the way the design describes rather than the way serde does.
    let cfg = RecorderConfig::parse("site='s'\nrecorder='r'\nenv='e'\n").unwrap();
    assert_eq!(cfg.capture.mode, CaptureMode::Afpacket);
    assert_eq!(cfg.capture.buffer, 64 * 1024 * 1024);
    assert_eq!(cfg.archive.rotate_interval, Duration::from_secs(60));
    assert_eq!(cfg.archive.compression, Compression::Zstd);
    assert_eq!(cfg.metrics.listen_addr.to_string(), "127.0.0.1:9100");
    assert!(cfg.feed.is_empty());
}

#[test]
fn two_port_roles_cannot_share_a_port() {
    // Whatever maps a port back to a role has to pick one, and then every
    // datagram on that port is attributed to a role it may not belong to —
    // silently, and for the life of the archive.
    let err = RecorderConfig::parse(
        "site='s'\nrecorder='r'\nenv='e'\n\
         [[feed]]\nspec='top-of-book'\nmulticast_group='233.252.0.10'\n\
         mktdata_port=40000\nrefdata_port=40000\n",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("40000"),
        "the error names the port"
    );
}

#[test]
fn a_snapshot_port_colliding_with_another_role_is_a_load_failure() {
    assert!(RecorderConfig::parse(
        "site='s'\nrecorder='r'\nenv='e'\n\
         [[feed]]\nspec='depth'\nmulticast_group='233.252.0.10'\n\
         mktdata_port=40000\nrefdata_port=40001\nsnapshot_port=40001\n",
    )
    .is_err());
}

#[test]
fn every_address_in_the_example_is_a_documentation_range() {
    // A public example is something people paste. 239.0.0.0/8 is
    // administratively-scoped multicast that operators really use, and RFC 1918
    // is not documentation space; a collision in someone else's network is not
    // a mistake an example should be able to cause.
    let toml = std::fs::read_to_string("tests/fixtures/recorder_example.toml").unwrap();
    // Values only: the file's own comments name the ranges it avoids, and
    // saying why a range is wrong is not the same as carrying one.
    let values: String = toml
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in ["239.", "10.0.", "172.16.", "192.168."] {
        assert!(
            !values.contains(forbidden),
            "{forbidden} is not a documentation range"
        );
    }
    assert!(toml.contains("233.252.0."), "MCAST-TEST-NET");
    assert!(toml.contains("192.0.2."), "RFC 5737");
}

#[test]
fn two_feeds_cannot_claim_one_port_on_one_group() {
    // The same collision one feed already forbids, arriving by a longer route —
    // and worse here, because the feeds have different specs, so a datagram
    // would be attributed to whichever was listed first.
    let err = RecorderConfig::parse(
        "site='s'\nrecorder='r'\nenv='e'\n\
         [[feed]]\nspec='top-of-book'\nmulticast_group='233.252.0.10'\n\
         mktdata_port=40000\nrefdata_port=40001\n\
         [[feed]]\nspec='depth'\nmulticast_group='233.252.0.10'\n\
         mktdata_port=40002\nrefdata_port=40001\n",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("40001"),
        "the error names the port"
    );
    assert!(err.to_string().contains("depth"), "and both feeds");
}

#[test]
fn one_port_number_on_two_different_groups_is_ordinary() {
    // Two different groups are two different channel instances. Rejecting this
    // would forbid the most natural way to lay out a second feed.
    assert!(RecorderConfig::parse(
        "site='s'\nrecorder='r'\nenv='e'\n\
         [[feed]]\nspec='top-of-book'\nmulticast_group='233.252.0.10'\n\
         mktdata_port=40000\nrefdata_port=40001\n\
         [[feed]]\nspec='depth'\nmulticast_group='233.252.0.11'\n\
         mktdata_port=40000\nrefdata_port=40001\n",
    )
    .is_ok());
}
