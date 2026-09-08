//! What derivation refuses, and why each refusal beats the row set it replaces.
//!
//! Every case here is one where a plausible implementation carries on and
//! produces rows that look like a finding about a feed. They are not: they are
//! findings about a file we damaged, a window we truncated, or a subtraction the
//! archive never said was valid.
#![forbid(unsafe_code)]

mod common;

use std::io::Write as _;

use common::record;
use dz_recorder_replay::synthetic::SyntheticPublisher;
use dz_recorder_rows::{derive_object, DeriveError};

/// A finding drawn from an object whose sha256 was never checked is a finding
/// about a file, not about a feed. Verification is part of loading.
#[test]
fn an_object_whose_digest_does_not_match_its_manifest_is_refused_by_name() {
    let recorded = record(&SyntheticPublisher::clean(50));
    let mut manifest = recorded.manifest.clone();
    let stated = manifest.sha256.clone();
    manifest.sha256 = "f".repeat(64);

    let error = derive_object(&recorded.object, &manifest, None)
        .expect_err("an object that is not the one described must not be loaded");
    let DeriveError::DigestMismatch {
        object_key,
        stated: claimed,
        found,
    } = &error
    else {
        panic!("expected a digest mismatch, got {error}");
    };
    assert_eq!(object_key, &manifest.object_key, "the object is named");
    assert_eq!(claimed, &"f".repeat(64));
    assert_eq!(found, &stated, "and what the bytes actually hash to");
    // The message carries both, because an operator's next question is which
    // of the two moved.
    let text = error.to_string();
    assert!(text.contains(&manifest.object_key), "{text}");
    assert!(text.contains(&stated), "{text}");
}

/// The digest is checked *before* any row is derived, not after a finding has
/// been drawn from one.
#[test]
fn a_damaged_object_produces_no_partial_row_set() {
    let recorded = record(&SyntheticPublisher::clean(50));
    // Appended to rather than truncated, so the archive still replays whole and
    // the only thing wrong with it is that it is not the object described.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&recorded.object)
        .expect("the object is writable");
    file.write_all(b"not the bytes the manifest describes")
        .expect("append");
    drop(file);

    let error = derive_object(&recorded.object, &recorded.manifest, None)
        .expect_err("the digest no longer matches");
    assert!(
        matches!(error, DeriveError::DigestMismatch { .. }),
        "{error}"
    );
}

/// A truncated object fails the digest, which is the check that covers a tear
/// as well as corruption.
///
/// zstd carries no content checksum unless the compressor asked for one, so most
/// single-byte damage decodes to *different bytes* with no error at all — and
/// nothing in a replay can see that. The manifest hash is what answers it, which
/// is why it is checked before an object is replayed rather than after.
#[test]
fn a_torn_object_is_refused_before_it_is_replayed() {
    let recorded = record(&SyntheticPublisher::clean(200));
    let bytes = std::fs::read(&recorded.object).expect("the object is readable");
    let torn = recorded.object.with_extension("torn");
    std::fs::write(&torn, &bytes[..bytes.len() / 2]).expect("the torn copy is writable");

    let error = derive_object(&torn, &recorded.manifest, None)
        .expect_err("half an object is not the object");
    assert!(
        matches!(error, DeriveError::DigestMismatch { .. }),
        "{error}"
    );
}

/// An object that is not there names the path, because a loader walking a
/// directory an eviction is emptying under it will meet this.
#[test]
fn an_object_that_is_gone_names_the_path_it_looked_for() {
    let recorded = record(&SyntheticPublisher::clean(10));
    let missing = recorded.object.with_extension("gone");
    let error =
        derive_object(&missing, &recorded.manifest, None).expect_err("nothing is at that path");
    let DeriveError::Io { path, .. } = &error else {
        panic!("expected an i/o failure, got {error}");
    };
    assert_eq!(path, &missing);
}

/// The scope the archive declares is the scope every subtraction is made at, and
/// a manifest disagreeing with the object means one of them describes different
/// bytes.
#[test]
fn a_manifest_disagreeing_with_the_object_about_the_drop_scope_is_refused() {
    let recorded = record(&SyntheticPublisher::clean(50));
    let mut manifest = recorded.manifest.clone();
    assert_eq!(manifest.capture_drop_scope, "port-role");
    manifest.capture_drop_scope = "capture-handle".to_owned();

    let error = derive_object(&recorded.object, &manifest, None)
        .expect_err("a subtraction under the wrong scope is a false publisher finding");
    let DeriveError::ScopeDisagreement {
        section, manifest, ..
    } = &error
    else {
        panic!("expected a scope disagreement, got {error}");
    };
    assert_eq!(section, "port-role", "what the object's own section says");
    assert_eq!(manifest, "capture-handle");
}

/// An archive whose section states the scope needs no manifest to state it, and
/// the section is what is believed: it travels inside the bytes.
#[test]
fn the_objects_own_section_settles_the_scope_when_the_manifest_is_silent() {
    let recorded = record(&SyntheticPublisher::clean(50));
    let mut manifest = recorded.manifest.clone();
    manifest.capture_drop_scope = String::new();

    let derived =
        derive_object(&recorded.object, &manifest, None).expect("the object states its own scope");
    assert_eq!(
        derived.rows.datagram[0].drop_scope,
        dz_recorder_rows::DropScope::PortRole
    );
}
