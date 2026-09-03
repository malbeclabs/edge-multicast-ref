//! Which of a venue's instruments get published, and which are declined.

/// The playbook's selection policy: seed the top N, cap at 2N, evict only on a
/// natural end of life, warn above N, and never withdraw an admitted
/// instrument to make room.
///
/// Every part of that has a job, and the pair of limits is the part worth
/// explaining. A venue's universe is routinely far larger than a feed
/// publishes, so something has to choose — and the two numbers exist because
/// *when* an instrument is offered matters as much as how many there are:
///
/// - The **seed** bounds the first poll. An adapter offers its whole set, so
///   without a separate seed limit the first poll would fill the cap, and the
///   instruments that filled it would be whatever the venue happened to list
///   first.
/// - The **cap** is twice the seed, so the headroom left after the seed is as
///   large as the seed itself. That headroom is what a genuinely new listing is
///   admitted into, hours later, without anything having to be evicted for it.
/// - The **warning threshold** sits at the seed, so an operator hears about the
///   headroom being consumed while there is still headroom, rather than at the
///   cap when new listings have already started being declined.
///
/// # Admission is sticky, and eviction is the venue's
///
/// An admitted instrument stays admitted until the venue withdraws it. Nothing
/// here evicts one to make room for a more attractive offer, and that is the
/// point rather than a simplification: a subscriber holding a book keyed on an
/// `Instrument ID` reads a withdrawal as the instrument having ended, so
/// evicting a live instrument to publish a busier one tells every subscriber
/// something untrue about the one it dropped.
///
/// So at the cap, an offer is declined. `None` from
/// [`ListingSink::list`](dz_adapter_core::ListingSink::list) is documented as
/// ordinary at that boundary for exactly this case.
///
/// # What "top N" means here
///
/// The order the adapter offers instruments in. Nothing in
/// [`InstrumentSpec`](dz_adapter_core::InstrumentSpec) states a rank, and that
/// is deliberate: the venue is the only party that knows which of its
/// instruments carry the liquidity, and a rank field on the boundary would be a
/// number a venue could get wrong in a way nothing above it could see. So the
/// offer order *is* the ranking, and it is documented at the boundary as the
/// order an adapter should poll in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectionPolicy {
    bootstrap_top_n: usize,
    max_published: usize,
    warn_published_above: usize,
}

/// Why a selection policy is not one.
///
/// A startup failure for the caller to report against its own configuration
/// keys, which is why these are named after the keys in `[refdata.selection]`
/// rather than after the fields here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    /// A publisher that admits nothing has no feed. Zero is what a
    /// half-written configuration file hands you, and it would present as a
    /// publisher that starts, stays up, reports a valid manifest of nothing and
    /// declines every instrument the venue offers.
    #[error("bootstrap_top_n is 0: a policy that admits nothing publishes nothing")]
    SeedIsZero,

    #[error("max_published {max_published} is below bootstrap_top_n {bootstrap_top_n}")]
    CapBelowSeed {
        bootstrap_top_n: usize,
        max_published: usize,
    },

    /// A threshold at or above the cap never fires before the cap does, so the
    /// warning it exists to give arrives at the same time as the declines it
    /// exists to give warning of.
    #[error(
        "warn_published_above {warn_published_above} is not below max_published {max_published}"
    )]
    WarnNotBelowCap {
        warn_published_above: usize,
        max_published: usize,
    },
}

/// Which limit applies, which depends on whether the published set has been
/// established yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The first poll has not finished. The seed limit applies and the
    /// manifest is not `Valid`.
    Seeding,
    /// The published set is established. The cap applies and the manifest is
    /// `Valid`.
    Established,
    /// The publisher is going down. The manifest is no longer `Valid` and
    /// nothing further is admitted: an `Instrument ID` minted during shutdown
    /// would be persisted and never published.
    ShuttingDown,
}

impl SelectionPolicy {
    /// The playbook's shape, from the one number an operator sets: seed `top_n`,
    /// cap at `2 * top_n`, warn above `top_n`.
    ///
    /// # Errors
    ///
    /// [`PolicyError::SeedIsZero`].
    pub const fn from_seed(top_n: usize) -> Result<Self, PolicyError> {
        if top_n == 0 {
            return Err(PolicyError::SeedIsZero);
        }
        Ok(Self {
            bootstrap_top_n: top_n,
            max_published: top_n.saturating_mul(2),
            warn_published_above: top_n,
        })
    }

    /// The three configured values, checked against each other.
    ///
    /// # Errors
    ///
    /// Every [`PolicyError`]. All are startup failures: a publisher whose
    /// selection policy is incoherent must not start, rather than discover it
    /// one declined instrument at a time.
    pub const fn new(
        bootstrap_top_n: usize,
        max_published: usize,
        warn_published_above: usize,
    ) -> Result<Self, PolicyError> {
        if bootstrap_top_n == 0 {
            return Err(PolicyError::SeedIsZero);
        }
        if max_published < bootstrap_top_n {
            return Err(PolicyError::CapBelowSeed {
                bootstrap_top_n,
                max_published,
            });
        }
        if warn_published_above >= max_published {
            return Err(PolicyError::WarnNotBelowCap {
                warn_published_above,
                max_published,
            });
        }
        Ok(Self {
            bootstrap_top_n,
            max_published,
            warn_published_above,
        })
    }

    /// How many instruments may be published in this phase.
    #[must_use]
    pub const fn limit(self, phase: Phase) -> usize {
        match phase {
            Phase::Seeding => self.bootstrap_top_n,
            Phase::Established => self.max_published,
            // Not zero because the published set is not withdrawn on the way
            // down - it is that nothing new joins it.
            Phase::ShuttingDown => 0,
        }
    }

    /// Whether a published count of `published` is one an operator should hear
    /// about.
    #[must_use]
    pub const fn warns_at(self, published: usize) -> bool {
        published > self.warn_published_above
    }

    #[must_use]
    pub const fn bootstrap_top_n(self) -> usize {
        self.bootstrap_top_n
    }

    #[must_use]
    pub const fn max_published(self) -> usize {
        self.max_published
    }

    #[must_use]
    pub const fn warn_published_above(self) -> usize {
        self.warn_published_above
    }
}
