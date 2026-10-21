//! Seeding a record from another publisher's `Instrument ID`s, without
//! depending on the layout it is written in.

use std::collections::HashMap;

use dz_edge_refdata::SYMBOL_LEN;

use crate::state::{
    is_venue_key, symbol_is_whole, unpadded, venue_key_field, Entry, RecordError, StateRecord,
    VenueKeys, FIRST_INSTRUMENT_ID,
};
use crate::store::{StateError, StateStore};

/// What [`seed`] writes into a state directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seed {
    /// The `Source ID` the IDs were published under. A record already in the
    /// directory must have been minted under the same one.
    pub source_id: u16,
    /// The `next_id` the other publisher would have minted next. Every ID in
    /// `entries` is below it, and the record keeps the larger of it and its
    /// own.
    pub next_id: u32,
    /// `(venue key, Instrument ID)` pairs: each key's ID as the other
    /// publisher published it. Each entry's `Symbol` is the key's wire field.
    pub entries: Vec<(String, u32)>,
    /// Forget every entry of an unkeyed record that `entries` does not
    /// resolve, rather than refusing the seed. Its ID stays below `next_id`
    /// and is never issued again, and the instrument, if listed, gets a new
    /// one.
    pub retire_unmapped: bool,
    /// The `[refdata] venue_keys` declaration the record will be opened
    /// under. It decides which entries of an unkeyed record have a known key,
    /// and how a pair matches one that does not.
    pub venue_keys: VenueKeys,
}

/// Why [`seed`] wrote nothing.
///
/// Every refusal but [`Store`](Self::Store) leaves the directory as it was.
#[derive(Debug, thiserror::Error)]
pub enum SeedError {
    /// The record in the directory was minted under another `Source ID`.
    #[error("the persisted state was minted under Source ID {persisted}, not {seeded}")]
    SourceIdMismatch { persisted: u16, seeded: u16 },

    /// A key the record holds under another ID, or an ID it holds under
    /// another key. Both sides are named: the pair the seed states and what
    /// the record, or an earlier pair of the same seed, holds.
    #[error(
        "the seed names venue key {seeded_key:?} as Instrument ID {seeded_id}, and the record \
         holds {held_key:?} as Instrument ID {held_id}"
    )]
    Conflict {
        seeded_key: String,
        seeded_id: u32,
        held_key: String,
        held_id: u32,
    },

    /// An ID of `0`, or at or above the seed's own `next_id`.
    #[error("Instrument ID {instrument_id} is not between 1 and the seed's next_id {next_id}")]
    IdNotBelowNext { instrument_id: u32, next_id: u32 },

    /// An ID the record does not hold, below the record's `next_id`.
    ///
    /// The record may have issued it and forgotten it, and `next_id` is the
    /// only thing that keeps an ID from being issued twice. Below it, "not
    /// held" does not mean "never issued".
    #[error(
        "Instrument ID {instrument_id} is not in the record and is below its next_id \
         {next_id}, so it may have been issued and forgotten"
    )]
    IdMayBeForgotten { instrument_id: u32, next_id: u32 },

    /// Entries of an unkeyed record whose key the seed neither maps nor
    /// retires, named by ID and `Symbol`.
    #[error("{} entries of the record have no known venue key, and the seed neither maps nor retires them", entries.len())]
    Unresolved {
        entries: Vec<(u32, [u8; SYMBOL_LEN])>,
    },

    /// A key that is empty, over 256 bytes, or holds a NUL.
    #[error("{venue_key:?} is not a venue key: it is empty, over 256 bytes, or holds a NUL")]
    BadVenueKey { venue_key: String },

    /// The record allocates derived IDs, and the seed is not resolve-only.
    ///
    /// A derived record takes a seed that maps unresolved entries it already
    /// holds and does nothing else. Any other seed's IDs belong below a floor,
    /// so seeding comes before the switch to a derived allocation, not after.
    /// Retiring is refused too: the derivation would give a retired entry's
    /// ID back to its instrument the next time it is listed.
    #[error("the record allocates derived Instrument IDs, and only a seed that resolves entries it holds can be written into it")]
    RecordIsDerived,

    /// The record is keyed under the other `venue_keys` declaration. Neither
    /// can be read as the other; see
    /// [`RefdataError::VenueKeysChanged`](crate::RefdataError::VenueKeysChanged).
    #[error(
        "the record is keyed under venue_keys = {persisted:?}, and the seed states {seeded:?}"
    )]
    VenueKeysChanged {
        persisted: VenueKeys,
        seeded: VenueKeys,
    },

    /// The record in the directory is not one this build can read.
    #[error("the persisted state is damaged")]
    Record(#[from] RecordError),

    /// The directory could not be claimed, read or written, unchanged.
    ///
    /// **After [`StateError::Write`], reload before acting.** The store's
    /// contract calls that outcome unknown: a directory sync that fails after
    /// the rename leaves either the old record or the merged one. After
    /// [`StateError::NotReplaced`] nothing changed. Since the merge is a union
    /// that refuses every disagreement, the same seed run again over either
    /// record gives the same result, so retrying is safe.
    #[error("the state directory could not be used")]
    Store(#[source] StateError),
}

/// Write another publisher's `(venue key, Instrument ID)` pairs into a state
/// directory, as a keyed record a registry under `identity = "venue_key"`
/// opens.
///
/// For a publisher moving onto this runtime from its own registry, which
/// would otherwise write a record in a format it does not own. Kalshi's
/// shipped publisher's `instruments.json`, `{ "next_id": N, "tickers": {
/// ticker: id } }`, is exactly a [`Seed`].
///
/// **It merges into a record that is there**, which is what a handover in
/// both directions needs: import before the first start, export on a
/// rollback, import again. The export needs no API: in a keyed record every
/// entry carries its key, and [`StateRecord::load`] returns them. The merge
/// is the union of the two, and refuses every disagreement rather than
/// choosing a side; see [`SeedError`]. It also resolves the entries of an
/// unkeyed record whose key is not known, which is the only way such a record
/// opens under the venue key.
///
/// A record allocating derived IDs takes only a seed that resolves entries it
/// already holds: no new entries, no retiring, and a `next_id` equal to its
/// floor, which is not moved.
///
/// The store is claimed for the call and released when it returns, so a
/// registry can open the same directory afterwards. The result is one base,
/// written through [`StateStore::store`].
///
/// # Errors
///
/// Every [`SeedError`]. See [`SeedError::Store`] for the one whose outcome is
/// not known.
pub fn seed<S: StateStore>(mut store: S, seed: &Seed) -> Result<(), SeedError> {
    store.claim().map_err(SeedError::Store)?;
    let record = match store.load().map_err(SeedError::Store)? {
        Some(bytes) => StateRecord::decode(&bytes)?,
        None => StateRecord::empty(seed.source_id),
    };
    let merged = merge(record, seed)?;
    let bytes = merged.encode();
    // The merge writes nothing a load would refuse; this is the check that it
    // did not, before the record is replaced by it.
    StateRecord::decode(&bytes)?;
    store.store(&bytes).map_err(SeedError::Store)
}

/// The record `seed` makes of `record`, or why it makes none.
fn merge(record: StateRecord, seed: &Seed) -> Result<StateRecord, SeedError> {
    if record.source_id != seed.source_id {
        return Err(SeedError::SourceIdMismatch {
            persisted: record.source_id,
            seeded: seed.source_id,
        });
    }
    if let Some(persisted) = record.keyed {
        if persisted != seed.venue_keys {
            return Err(SeedError::VenueKeysChanged {
                persisted,
                seeded: seed.venue_keys,
            });
        }
    }
    let derived = record.allocation.is_derived();
    if derived && (seed.retire_unmapped || seed.next_id != record.next_id) {
        return Err(SeedError::RecordIsDerived);
    }

    // Each entry's key, where it is known. In a keyed record every one is; in
    // an unkeyed one only a whole Symbol under the symbol declaration.
    let mut keys: Vec<Option<Box<[u8]>>> = record
        .entries
        .iter()
        .map(|entry| {
            let known = record.keyed.is_some()
                || (seed.venue_keys == VenueKeys::Symbol && symbol_is_whole(&entry.symbol));
            known.then(|| entry.key().into())
        })
        .collect();
    let mut entries = record.entries;
    let mut by_key: HashMap<Box<[u8]>, usize> = keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| Some((key.clone()?, index)))
        .collect();
    let mut by_id: HashMap<u32, usize> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.instrument_id, index))
        .collect();

    for (venue_key, instrument_id) in &seed.entries {
        let (venue_key, instrument_id) = (venue_key.as_str(), *instrument_id);
        let key = venue_key.as_bytes();
        if !is_venue_key(key) {
            return Err(SeedError::BadVenueKey {
                venue_key: venue_key.to_owned(),
            });
        }
        if !derived && (instrument_id == 0 || instrument_id >= seed.next_id) {
            return Err(SeedError::IdNotBelowNext {
                instrument_id,
                next_id: seed.next_id,
            });
        }
        let conflict = |entry: &Entry, held: &[u8]| SeedError::Conflict {
            seeded_key: venue_key.to_owned(),
            seeded_id: instrument_id,
            held_key: String::from_utf8_lossy(held).into_owned(),
            held_id: entry.instrument_id,
        };
        if let Some(&index) = by_key.get(key) {
            if entries[index].instrument_id != instrument_id {
                return Err(conflict(&entries[index], key));
            }
            continue;
        }
        match by_id.get(&instrument_id) {
            Some(&index) => {
                let entry = &entries[index];
                if let Some(held) = &keys[index] {
                    return Err(conflict(entry, held));
                }
                // An unresolved entry. Under the symbol declaration the key
                // is the one whose wire field is the entry's Symbol; under
                // stated keys it need not be, and the ID alone matches.
                let symbol = dz_edge_core::pad_ascii::<SYMBOL_LEN>(venue_key).0;
                if seed.venue_keys == VenueKeys::Symbol && symbol != entry.symbol {
                    return Err(conflict(entry, unpadded(&entry.symbol)));
                }
                entries[index].venue_key = venue_key_field(key, &entries[index].symbol);
                keys[index] = Some(key.into());
                by_key.insert(key.into(), index);
            }
            None => {
                if derived {
                    return Err(SeedError::RecordIsDerived);
                }
                if instrument_id < record.next_id {
                    return Err(SeedError::IdMayBeForgotten {
                        instrument_id,
                        next_id: record.next_id,
                    });
                }
                by_id.insert(instrument_id, entries.len());
                by_key.insert(key.into(), entries.len());
                keys.push(Some(key.into()));
                entries.push(Entry::keyed(instrument_id, venue_key, None));
            }
        }
    }

    let unresolved: Vec<(u32, [u8; SYMBOL_LEN])> = entries
        .iter()
        .zip(&keys)
        .filter(|(_, key)| key.is_none())
        .map(|(entry, _)| (entry.instrument_id, entry.symbol))
        .collect();
    if !unresolved.is_empty() && !seed.retire_unmapped {
        return Err(SeedError::Unresolved {
            entries: unresolved,
        });
    }
    let entries: Vec<Entry> = entries
        .into_iter()
        .zip(keys)
        .filter_map(|(entry, key)| {
            let key = key?;
            Some(Entry {
                venue_key: venue_key_field(&key, &entry.symbol),
                ..entry
            })
        })
        .collect();

    Ok(StateRecord {
        source_id: record.source_id,
        // A derived record's floor does not move; the check above made the
        // seed's equal to it.
        next_id: record.next_id.max(seed.next_id).max(FIRST_INSTRUMENT_ID),
        entries,
        allocation: record.allocation,
        keyed: Some(seed.venue_keys),
    })
}
