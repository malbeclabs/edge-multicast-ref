//! Why a publisher's reference data will not start.

use crate::state::{IdAllocation, RecordError, VenueKeys};
use crate::store::StateError;

/// The startup failures, none of which is recoverable by continuing.
///
/// Every variant here ends with the process not starting, and that is the
/// point. The failure this crate exists to prevent is a published `Instrument
/// ID` that resolves to nothing, and each of these is a way of reaching it by
/// carrying on: a second writer overwrites the first's flush, an unreadable
/// record mints from the start of the ID space again, and a record belonging to
/// another publisher publishes its IDs under our `Source ID`. A publisher that
/// refuses to start is visible in one place. A publisher that started on a
/// wrong ID map is visible in every subscriber, later, as a book keyed on an
/// instrument that has become something else.
///
/// The two shard variants are not about identity, and they are here for the
/// same reason: a shard set that cannot be resolved is a publisher whose
/// channels stay empty with nothing on the wire to say why.
#[derive(Debug, thiserror::Error)]
pub enum RefdataError {
    /// The state directory is already held by a live writer.
    ///
    /// **The incumbent wins and this process does not start.** It is the
    /// asymmetry that matters: the running publisher has already put IDs on the
    /// wire that subscribers hold, so refusing the newcomer costs one failed
    /// start, and admitting it costs the identity of every instrument on the
    /// feed. Two writers each mint from their own copy of `next_id` and each
    /// flush overwrites the other's, so after the next restart the IDs that
    /// lost the last flush resolve to nothing.
    #[error("another live writer holds the reference-data state directory; this publisher will not start alongside it")]
    StateHeldByAnotherWriter,

    /// The state directory could not be claimed, read, or written.
    #[error("the reference-data state directory is unusable")]
    State(#[source] StateError),

    /// A record is present and is not one this build can read.
    #[error("the persisted reference-data state is damaged")]
    CorruptState(#[from] RecordError),

    /// No shard was configured, so there is nothing to admit to.
    ///
    /// Every offer would be refused with
    /// [`Refusal::UnknownShard`](crate::Refusal::UnknownShard) and the
    /// publisher would run with an empty published set on every channel — a
    /// feed that is silent for a reason no datagram carries. A document always
    /// resolves at least one shard, so reaching this means a caller composed
    /// the configuration itself, which is exactly the caller that has no
    /// document to check.
    #[error("no shard was configured, so no instrument could ever be admitted")]
    NoShardConfigured,

    /// Two shards were configured under one name.
    ///
    /// A name resolves to one published set, so the second is unreachable: its
    /// `Channel ID` would carry a manifest that stays at `Manifest Seq` 0 and
    /// `Instrument Count` 0 for the life of the process while the venue went on
    /// admitting instruments it believed were on that shard. The document check
    /// refuses this first; this is the second line, in the code that does the
    /// resolving.
    #[error("the shard `{shard}` was configured twice, and a name resolves to one published set")]
    ShardConfiguredTwice { shard: String },

    /// The record was minted under a different `Source ID`.
    ///
    /// What this catches is two feeds configured to share one `state_dir` — the
    /// live-writer guard cannot see that, because they need never run at the
    /// same time. Continuing would publish the other publisher's `Instrument
    /// ID`s under this one's `Source ID`, and a subscriber resolving the pair
    /// would find two publishers claiming the same instrument identity.
    #[error("the persisted state was minted under Source ID {persisted}, not {configured}")]
    StateBelongsToAnotherSource { persisted: u16, configured: u16 },

    /// Derived allocation was configured together with a horizon for
    /// forgetting delisted instruments.
    ///
    /// Two publishers of one channel forget about one poll apart, so a symbol
    /// relisted near the horizon would come back under the record's ID on one
    /// and under a fresh derivation on the other. The document check refuses
    /// this first; this is the second line, for a caller composing the
    /// configuration itself.
    #[error("derived Instrument ID allocation, from the Symbol or the venue key, forgets nothing, so it cannot be combined with forget_delisted_after")]
    ForgettingUnderDerivedIds,

    /// The record was written under derived allocation and the configuration
    /// asks for sequential.
    ///
    /// Sequential minting would continue from the record's floor, straight into
    /// IDs the derived mints above it already hold. There is no way back that
    /// keeps every published ID, so there is none here.
    #[error("the persisted state was written under derived Instrument ID allocation, and sequential allocation cannot continue it")]
    StateIsDerived,

    /// `venue_keys = "stated"` was configured under `identity = "symbol"`.
    ///
    /// The symbol identity ignores every venue key, so a declaration that the
    /// adapter states its own would be read by nothing. The document check
    /// refuses this first; this is the second line.
    #[error("venue_keys = \"stated\" declares keys that identity = \"symbol\" never reads")]
    VenueKeysWithoutVenueKey,

    /// `id_allocation = "derived_key"` was configured under
    /// `identity = "symbol"`, where there is no key to derive from.
    #[error("id_allocation = \"derived_key\" derives from a venue key, and identity = \"symbol\" has none")]
    DerivedKeyWithoutVenueKey,

    /// The record was written under one derived allocation and the
    /// configuration asks for the other.
    ///
    /// `derived` hashes the `Symbol` and `derived_key` the venue key, so every
    /// derived ID above the floor would change, and a subscriber's books with
    /// them.
    #[error("the persisted state was written under id_allocation {persisted:?}, and {configured:?} derives different IDs")]
    DerivationChanged {
        persisted: IdAllocation,
        configured: IdAllocation,
    },

    /// The record is keyed, version 4, and the configuration asks for the
    /// symbol identity.
    ///
    /// Its entries may share a `Symbol`, and a registry that recalls by the
    /// `Symbol` cannot hold two instruments under one.
    #[error("the persisted state is keyed by venue key, and identity = \"symbol\" cannot read it")]
    StateIsKeyed,

    /// The record was written under one `venue_keys` declaration and the
    /// configuration states the other.
    ///
    /// Under `symbol` every `-` entry is "the key is the Symbol", which an
    /// adapter stating its own keys would never offer; under `stated` the
    /// reverse. Either way every offer would miss and be given a new ID. There
    /// is no way back that keeps the IDs: a fresh record is the only path.
    #[error(
        "the persisted state was written under venue_keys = {persisted:?}, not {configured:?}"
    )]
    VenueKeysChanged {
        persisted: VenueKeys,
        configured: VenueKeys,
    },

    /// An unkeyed record holds entries whose venue key is not known, and the
    /// configuration recalls by venue key.
    ///
    /// Each could stand for its exact `Symbol` or for a longer name that was
    /// cut, or under `venue_keys = "stated"` for any key at all. Giving the ID
    /// to whichever key is offered first depends on what one host saw first,
    /// so the record opens only after a mapping resolves them through
    /// [`seed`](crate::seed). Each entry is named by its ID and `Symbol`.
    #[error("{} entries of the persisted state have no known venue key; resolve them with a seed mapping", entries.len())]
    UnresolvedVenueKeys {
        entries: Vec<(u32, [u8; dz_edge_refdata::SYMBOL_LEN])>,
    },
}
