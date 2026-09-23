//! How long to wait before the next connect attempt.

use std::time::Duration;

use crate::error::ConfigError;

/// The two keys `[ingress]` gives, checked once.
///
/// Validated at construction rather than at use, because both mistakes
/// available here are silent at use: an inverted pair clamps on the first
/// delay, and a zero initial delay doubles to zero forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    initial: Duration,
    max: Duration,
}

impl BackoffPolicy {
    /// A policy from the two configured durations.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ZeroDuration`] for a zero initial delay, and
    /// [`ConfigError::BackoffInverted`] for a maximum below it. See
    /// [`ConfigError`] for why neither is quietly repaired.
    pub const fn new(initial: Duration, max: Duration) -> Result<Self, ConfigError> {
        if initial.is_zero() {
            return Err(ConfigError::ZeroDuration {
                key: "reconnect_backoff_initial",
            });
        }
        if max.as_nanos() < initial.as_nanos() {
            return Err(ConfigError::BackoffInverted { initial, max });
        }
        Ok(Self { initial, max })
    }

    /// The shortest any delay is, and the base the ceiling doubles from. A
    /// proven connection resets that ceiling to one doubling up from this
    /// rather than to this, so the delay it waits next is drawn from the
    /// opening window: this to twice this, or this to [`max`](Self::max)
    /// wherever the maximum is the lower of the two.
    #[must_use]
    pub const fn initial(self) -> Duration {
        self.initial
    }

    /// The ceiling every delay is capped at.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }

    /// The line a policy with no window in it earns an operator, and `None` for
    /// a policy that has one to draw in.
    ///
    /// A maximum equal to the initial delay is a fixed delay: the window is a
    /// single point, every draw is that one value, and the connections a
    /// publisher holds against one address retry at the same instants — the
    /// lockstep [`Backoff`] exists to end. It loads, because a fixed retry
    /// cadence is a configuration somebody may mean; the operator who did not
    /// mean it is who this line is for, since nothing else in a running
    /// publisher names the cause. The delays themselves are correct either
    /// way, so there is nothing here to repair and nothing to count: what was
    /// missing was a sentence.
    ///
    /// # Why a line and not a [`ConfigError`]
    ///
    /// Two reasons, and the second is the one that decides it:
    ///
    /// - A refusal would stop a document that loads today and states exactly
    ///   what it says, at the moment a publisher is restarted onto a newer
    ///   build. Nothing on the wire depends on the jitter, and the refusals
    ///   this enum does make are for pairs that *cannot* be obeyed as written —
    ///   a transposed pair, a zero that doubles to zero forever, a rate finer
    ///   than the clock pacing it. A fixed delay is obeyed exactly as written.
    /// - **A refusal would bound the spelling and not the lockstep.** The
    ///   window's width is continuous in the pair: a maximum one nanosecond
    ///   above the initial delay draws over a window one nanosecond wide, which
    ///   is the same lockstep and loads under any refusal that keys on
    ///   equality. There is no width at which retries stop being in step, so
    ///   the value cannot be the thing this guards — only whether an operator
    ///   is told.
    ///
    /// Composed here, where the two keys are spelled and where every other
    /// sentence about them lives, and stated by whoever runs a policy:
    /// `dz-publisher-runtime` writes it at startup beside the other
    /// configurations that are legitimate and are not defaults. Separate from
    /// that call site so a test can assert it, which is the only part of the
    /// path a test can reach — nothing in this workspace captures stderr.
    #[must_use]
    pub fn lockstep_line(self) -> Option<String> {
        if self.max != self.initial {
            return None;
        }
        Some(format!(
            "reconnect jitter is off: `[ingress] reconnect_backoff_max` is the same as \
             `reconnect_backoff_initial` ({:?}), so every retry waits that one value and the \
             connections this publisher holds against one address retry in lockstep. State a \
             maximum above the initial delay to have them drawn apart.",
            self.initial
        ))
    }
}

/// The delay sequence: a window that doubles towards the configured maximum,
/// and each delay drawn inside the window its attempt is due under.
///
/// # Equal jitter, over a window that is not itself random
///
/// Several venues publish a connection-attempt budget — attempts per window per
/// address — and enforce it by banning the address for a period rather than by
/// refusing the connection. Two quantities have to stay inside such a budget,
/// and a delay sequence that is only a doubling gets the second one wrong:
///
/// - **The burst.** Connections that fail together are the ordinary case — one
///   event drops them, one policy drives them — so a sequence that returns its
///   ceiling retries every one of them at the same instant, for as long as the
///   outage lasts. A venue counting attempts over a short window sees one burst
///   of however many connections a publisher holds against the address, which
///   is the shape a per-address limit is written to catch.
/// - **The sustained rate.** `reconnect_backoff_max` is what an operator sets
///   to say how often a publisher may attempt at worst, so the *average* delay
///   is what the budget is spent at. Drawing from `[initial, ceiling]` —
///   textbook full jitter — averages half the ceiling, which doubles the
///   sustained attempt rate against the very budget being protected: four
///   connections through a ten-minute outage at a 30-second maximum would make
///   about 157 attempts against the address where the configured maximum says
///   80.
///
/// So the draw is **equal jitter**: uniform over `[ceiling / 2, ceiling]`,
/// clamped so that no delay is below the configured initial delay or above the
/// configured maximum. Half the ceiling is the *previous* ceiling only while
/// the ceiling is still doubling; from the first capped ceiling on, the floor
/// stays at half the configured maximum whatever the ceiling before it was. For
/// the documented pair — 500ms and 30s — the windows are 500ms to 1s, 1s to 2s,
/// and so on to 15s to 30s, which is the window every further attempt of an
/// outage is drawn from: the ceiling before it is 16s once and 30s thereafter,
/// and the floor is 15s in both cases. The burst is flattened across half the
/// ceiling, and the average delay is three quarters of it: a sustained rate of
/// about `4/3` of one attempt per ceiling rather than `2`.
///
/// Three properties follow, and the last is why this is not decorrelated
/// jitter, where the *state* is what the draw replaces (`ceiling =
/// random(initial, ceiling * 3)`):
///
/// - **Every delay is jittered, the first one included.** The window a sequence
///   opens with is `initial` to `2 × initial` — or to `max`, wherever the
///   maximum is lower than that — rather than a point, so connections dropped
///   by one event are separated on their first retry and not only once they
///   have failed twice. It matters most where it would be easiest to miss:
///   [`reset`](Self::reset) returns to that opening window, and a venue that
///   accepts a connection, delivers, and closes — a session boundary, or a
///   throttle that lets one payload through — is a venue every connection
///   resets against, repeatedly.
/// - **No delay is below `initial` or above `max`.** The floor keeps
///   [`ConfigError::ZeroDuration`] meaning what it says, since drawing from
///   zero would hand a venue a retry with no pause at all; the ceiling keeps
///   `reconnect_backoff_max` meaning a maximum. A maximum equal to the initial
///   delay is therefore a policy with no window anywhere in it: every draw is
///   that one value, and the connections this exists to separate stay in step.
///   [`BackoffPolicy::new`] refuses only a maximum *below* the initial delay,
///   so a fixed-delay configuration opts out of the jitter rather than failing
///   to load — and [`BackoffPolicy::lockstep_line`] is what says so at startup,
///   for the operator who did not mean to opt out and would otherwise have
///   nothing anywhere naming the cause.
/// - **The ceiling sequence is `2 × initial`, `4 × initial`, … capped at
///   `max`, exactly.** It is a value [`ceiling`](Self::ceiling) reports and a
///   test asserts as a list, so a cap applied one step late is caught by a
///   comparison rather than by a range. Under decorrelated jitter the ceiling
///   is reached at a random step, and how many attempts a venue's window admits
///   could then only be bounded in expectation — the wrong shape of answer
///   about a limit enforced with a ban.
///
/// The randomness is seeded and nothing in here reaches for a thread-local
/// generator: [`new`](Self::new) takes the seed, so a sequence is a value a
/// test states rather than a range it hopes for, and [`seed`](Self::seed) is
/// what a driver derives one from.
///
/// Neither `Clone` nor `Copy`, deliberately. A copy would carry the generator's
/// state and draw the same numbers as the original from that point on, which is
/// the lockstep this type exists to remove, reintroduced by an assignment.
#[derive(Debug)]
pub struct Backoff {
    policy: BackoffPolicy,
    /// The ceiling the next delay is drawn under. Doubles, capped at
    /// `policy.max`.
    ceiling: Duration,
    /// The generator's state. See [`Backoff::draw`].
    entropy: u64,
}

impl Backoff {
    /// A sequence at its start, drawing from `seed`.
    ///
    /// Two sequences given the same seed produce the same delays, which is what
    /// makes this testable; two connections that must not retry together
    /// therefore have to be given different seeds. [`seed`](Self::seed) is the
    /// derivation the driver uses for that, and it is the one a transport
    /// driving its own sequence should use too.
    #[must_use]
    pub fn new(policy: BackoffPolicy, seed: u64) -> Self {
        Self {
            ceiling: Self::opening_ceiling(policy),
            policy,
            entropy: seed,
        }
    }

    /// The seed for one connection's sequence, from a wall-clock reading and
    /// the connection's name.
    ///
    /// **The name is what carries the property that matters.** A venue's
    /// attempt budget is per address, so the connections that can spend one
    /// another's budget are the ones in a single publisher — and those are
    /// built in a single loop, off a single clock reading, so a reading alone
    /// would seed them identically. The name cannot: it is distinct per
    /// connection by construction, being the `connection` label every
    /// `dz_publisher_ingress_*` series is broken down by.
    ///
    /// The reading adds separation between two processes that hold the same
    /// connection names — a restart, or a second host — and it is the part that
    /// can degrade. A host with no real-time clock reads zero (see
    /// [`Clock::wall_ns`](crate::Clock::wall_ns)), so at a fleet-wide restart
    /// every such host seeds a given name the same way. That is accepted
    /// rather than worked around: those hosts hold different addresses, and a
    /// budget enforced per address is not spent any faster by two of them
    /// agreeing.
    ///
    /// FNV-1a over the name, mixed into the reading. Not a cryptographic hash
    /// and it does not need to be: what it has to produce is two *different*
    /// numbers, not unpredictable ones.
    #[must_use]
    pub fn seed(wall_ns: u64, connection: &str) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ wall_ns;
        for byte in connection.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    /// The delay to wait now, advancing the ceiling.
    ///
    /// Never below [`BackoffPolicy::initial`] and never above the ceiling this
    /// call drew under, which is the pair of bounds the whole type is for.
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = self.ceiling;
        self.ceiling = Self::doubled(ceiling, self.policy);
        // Half the ceiling is the bottom of the window - and the configured
        // initial delay whenever that is longer, which is the case of a
        // maximum less than twice the initial delay. Half the ceiling is the
        // previous ceiling only while the ceiling is still doubling: a capped
        // ceiling keeps its floor at half the configured maximum, so the
        // window a settled sequence draws from stays `[max / 2, max]` - 15s to
        // 30s for the documented pair, under a previous ceiling of 16s once
        // and 30s thereafter - rather than closing to a point.
        let floor = (ceiling - ceiling / 2).max(self.policy.initial);
        // The window, in nanoseconds a draw can address. One wider than
        // `u64::MAX` nanoseconds is 584 years of window, so clamping there
        // costs nothing any policy can express and keeps the draw a single
        // multiplication: the delay stays under the ceiling, because what the
        // clamp discards is the top of a window no configuration reaches.
        let span_ns = u64::try_from((ceiling - floor).as_nanos()).unwrap_or(u64::MAX);
        floor.saturating_add(Duration::from_nanos(self.draw_at_most(span_ns)))
    }

    /// The ceiling a sequence opens with, and the one a reset returns to: one
    /// doubling up from the initial delay, so that the opening window is
    /// `initial` to `2 x initial` rather than a point. Capped at the
    /// configured maximum like every other ceiling, which narrows that window
    /// to `initial` to `max` under a maximum below twice the initial delay,
    /// and closes it to a point again at a maximum equal to the initial delay.
    fn opening_ceiling(policy: BackoffPolicy) -> Duration {
        Self::doubled(policy.initial, policy)
    }

    /// The next ceiling up.
    ///
    /// Saturating, then capped: a configured maximum near the end of the range
    /// must not double past it and wrap to nothing, which would turn the
    /// ceiling into a hot loop.
    fn doubled(ceiling: Duration, policy: BackoffPolicy) -> Duration {
        ceiling.checked_mul(2).unwrap_or(policy.max).min(policy.max)
    }

    /// Start the sequence again from the window it opened with.
    ///
    /// Called by the driver for a connection that proved itself, and not merely
    /// for one that was accepted. See [`Driver`](crate::Driver) for what proof
    /// is and why accepting is not it.
    ///
    /// The ceiling resets and the generator's state does not. Re-seeding here
    /// would make one connection's draws repeat every time it proved itself,
    /// and would put two connections that reset on the same event back into the
    /// lockstep the draw exists to break.
    pub fn reset(&mut self) {
        self.ceiling = Self::opening_ceiling(self.policy);
    }

    /// The ceiling the next call to [`next_delay`](Self::next_delay) will draw
    /// under. Half of it, or the configured initial delay if that is longer, is
    /// the bottom of the same window.
    ///
    /// The delay itself is not knowable in advance — that is the point of it —
    /// so what this reports is the bound. For a test or a log line; the driver
    /// does not need it.
    #[must_use]
    pub const fn ceiling(&self) -> Duration {
        self.ceiling
    }

    /// A draw uniform over `0..=most`.
    ///
    /// Multiply-and-shift rather than a remainder: `most + 1` is not a power of
    /// two in general, and `%` over a 64-bit draw biases the low buckets. This
    /// takes the high half of the product, so the buckets differ in width by at
    /// most one draw out of `2^64`.
    fn draw_at_most(&mut self, most: u64) -> u64 {
        let scaled = (u128::from(self.draw()) * (u128::from(most) + 1)) >> 64;
        // The product is below `2^128` and the shift leaves at most 64 bits, so
        // the conversion holds; `most` is the correct answer if it ever did not.
        u64::try_from(scaled).unwrap_or(most)
    }

    /// One 64-bit draw, advancing the generator.
    ///
    /// SplitMix64 — a constant increment and two mixing rounds — whose period
    /// is `2^64` for every seed, zero included. That is why it is this
    /// generator and not an xorshift, where a zero seed is a sequence of zeroes
    /// and a seed is a thing derived from a clock.
    ///
    /// Nine lines and no dependency, which is the rest of the reason. This is
    /// the crate every venue in the family links, so its dependency list is
    /// held to the boundary crate's standard, and a uniform draw for a
    /// reconnect delay does not justify a random-number crate in every venue's
    /// tree.
    fn draw(&mut self) -> u64 {
        self.entropy = self.entropy.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.entropy;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seed with no structure to it, for the tests whose subject is the
    /// bounds rather than a particular sequence.
    const SEED: u64 = 0x2b99_2ddf_a232_4ca3;

    fn policy(initial_ms: u64, max_ms: u64) -> BackoffPolicy {
        BackoffPolicy::new(
            Duration::from_millis(initial_ms),
            Duration::from_millis(max_ms),
        )
        .expect("a valid policy")
    }

    /// The bottom of the window a given ceiling draws over.
    fn floor_under(ceiling: Duration, initial: Duration) -> Duration {
        (ceiling - ceiling / 2).max(initial)
    }

    #[test]
    fn the_ceiling_doubles_from_the_initial_delay_and_stops_at_the_maximum() {
        let mut backoff = Backoff::new(policy(500, 30_000), SEED);
        let ceilings: Vec<u128> = (0..9)
            .map(|_| {
                let ceiling = backoff.ceiling().as_millis();
                backoff.next_delay();
                ceiling
            })
            .collect();
        // The configured example values, spelled out. The sequence opens one
        // doubling up from the initial delay, because the opening window is the
        // initial delay to twice it rather than a point - and a cap applied one
        // step late, or a doubling that starts from the second delay, is a
        // different list. The jitter is drawn *inside* this list, so it does
        // not cost the cap its assertion.
        assert_eq!(
            ceilings,
            vec![1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000, 30_000]
        );
    }

    #[test]
    fn the_first_delay_is_drawn_like_every_other_one() {
        // The gap this closes: a sequence whose opening window is a point
        // retries every connection of a publisher at the same instant on the
        // first attempt after any drop - and `reset` returns to that window, so
        // a venue that accepts, delivers and closes would keep them there for
        // as long as it kept doing it.
        let mut seen = std::collections::BTreeSet::new();
        for seed in 0..64 {
            let mut backoff = Backoff::new(policy(500, 30_000), seed);
            let first = backoff.next_delay();
            assert!(
                first >= Duration::from_millis(500) && first <= Duration::from_secs(1),
                "seed {seed} opened with {first:?}"
            );
            seen.insert(first);
        }
        assert!(
            seen.len() > 32,
            "64 connections opened with {} distinct delays between them",
            seen.len()
        );
    }

    #[test]
    fn every_delay_is_between_the_floor_and_the_ceiling_it_was_drawn_under() {
        let initial = Duration::from_millis(500);
        // Enough seeds that a draw escaping the window in either direction has
        // nowhere to hide, and enough steps to pass the cap.
        for seed in 0..256 {
            let mut backoff = Backoff::new(policy(500, 30_000), seed);
            for step in 0..10 {
                let ceiling = backoff.ceiling();
                let floor = floor_under(ceiling, initial);
                let delay = backoff.next_delay();
                assert!(
                    delay >= initial,
                    "seed {seed} step {step} drew {delay:?}, below the configured initial delay"
                );
                assert!(
                    delay >= floor,
                    "seed {seed} step {step} drew {delay:?}, below its window's floor {floor:?}"
                );
                assert!(
                    delay <= ceiling,
                    "seed {seed} step {step} drew {delay:?}, above the ceiling {ceiling:?}"
                );
            }
        }
    }

    #[test]
    fn the_draw_covers_the_window_rather_than_sitting_at_one_end_of_it() {
        // A jitter that always returned the floor, or always the ceiling, would
        // satisfy the bounds above and remove nothing: the connections it is
        // meant to separate would still retry together. So the distribution is
        // asserted too - over the fourth delay, whose window is 4s to 8s.
        let midpoint = Duration::from_secs(6);
        let mut low = 0;
        let mut high = 0;
        for seed in 0..256 {
            let mut backoff = Backoff::new(policy(500, 30_000), seed);
            for _ in 0..3 {
                backoff.next_delay();
            }
            if backoff.next_delay() < midpoint {
                low += 1;
            } else {
                high += 1;
            }
        }
        // A uniform draw puts about half in each half. The bound is loose
        // because the assertion is about a generator that draws across the
        // window at all, not about the quality of its uniformity.
        assert!(low > 64, "only {low} of 256 draws fell in the lower half");
        assert!(high > 64, "only {high} of 256 draws fell in the upper half");
    }

    #[test]
    fn the_sustained_delay_averages_three_quarters_of_the_ceiling_and_not_half() {
        // The reason this is equal jitter and not full jitter, as a number. A
        // publisher at the ceiling spends a venue's attempt budget at the
        // *average* rate, so a window of `[initial, max]` - which averages half
        // the maximum - would double the sustained attempt rate against the
        // budget this exists to stay inside. The window is `[max / 2, max]`, so
        // the average is three quarters of it.
        let draws = 512;
        let mut total = Duration::ZERO;
        for seed in 0..draws {
            let mut backoff = Backoff::new(policy(500, 30_000), seed);
            // Past the cap, which is where an outage settles.
            for _ in 0..6 {
                backoff.next_delay();
            }
            total += backoff.next_delay();
        }
        let mean = total / u32::try_from(draws).expect("a test-sized count");
        assert!(
            mean > Duration::from_millis(21_500) && mean < Duration::from_millis(23_500),
            "the mean delay at a 30s ceiling was {mean:?}, not about 22.5s"
        );
    }

    #[test]
    fn the_floor_at_the_cap_is_half_the_maximum_and_not_the_ceiling_before_it() {
        // Half the ceiling is the previous ceiling only while the ceiling is
        // still doubling. At the cap the two part company: for 500ms/30s the
        // ceiling before the first capped one is 16s and the ceiling before
        // every later one is 30s, while the floor is 15s throughout. A floor
        // that tracked the previous ceiling would draw the first capped delay
        // over 16s to 30s and every later one over the single point 30s -
        // which is the lockstep this type exists to remove, arriving exactly
        // where an outage settles.
        let floor = Duration::from_secs(15);
        let max = Duration::from_secs(30);
        let ceiling_before_the_cap = Duration::from_secs(16);
        let mut below_the_ceiling_before_the_cap = 0;
        let mut settled_below_the_maximum = 0;
        for seed in 0..256 {
            let mut backoff = Backoff::new(policy(500, 30_000), seed);
            // Five draws take the ceiling 1s, 2s, 4s, 8s, 16s; the sixth is
            // the first one drawn under the cap.
            for _ in 0..5 {
                backoff.next_delay();
            }
            assert_eq!(backoff.ceiling(), max, "seed {seed} is not at the cap");
            let first_capped = backoff.next_delay();
            assert!(
                first_capped >= floor && first_capped <= max,
                "seed {seed} drew {first_capped:?} outside 15s to 30s"
            );
            if first_capped < ceiling_before_the_cap {
                below_the_ceiling_before_the_cap += 1;
            }
            // The next one, whose previous ceiling is the maximum itself.
            let settled = backoff.next_delay();
            assert!(
                settled >= floor && settled <= max,
                "seed {seed} drew {settled:?} outside 15s to 30s"
            );
            if settled < max {
                settled_below_the_maximum += 1;
            }
        }
        // A fifteenth of a 15s-to-30s window is below 16s, so a floor at the
        // previous ceiling would take this to zero.
        assert!(
            below_the_ceiling_before_the_cap > 4,
            "only {below_the_ceiling_before_the_cap} of 256 first capped draws \
             fell below the 16s ceiling that preceded them"
        );
        // And a floor at the previous ceiling would pin every one of these to
        // 30s exactly.
        assert_eq!(
            settled_below_the_maximum,
            256,
            "a settled sequence drew the maximum exactly in {} of 256 cases",
            256 - settled_below_the_maximum
        );
    }

    #[test]
    fn a_maximum_below_twice_the_initial_delay_still_draws_above_the_initial_delay() {
        // The clamp: half of this ceiling is below the delay the file states as
        // the shortest a retry waits, and the file wins.
        let mut backoff = Backoff::new(policy(500, 600), SEED);
        for _ in 0..16 {
            let delay = backoff.next_delay();
            assert!(
                delay >= Duration::from_millis(500) && delay <= Duration::from_millis(600),
                "{delay:?} is outside 500ms to 600ms"
            );
        }
    }

    #[test]
    fn two_connections_dropped_by_one_event_do_not_retry_together() {
        // The defect in one test. Both sequences start at the same instant off
        // the same clock reading, which is what a venue closing every
        // connection at once produces, and they are separated by the only thing
        // that distinguishes them: their names.
        let wall_ns = 1_760_000_000_123_456_789;
        let mut first = Backoff::new(
            policy(500, 30_000),
            Backoff::seed(wall_ns, "mktdata-primary"),
        );
        let mut second = Backoff::new(
            policy(500, 30_000),
            Backoff::seed(wall_ns, "mktdata-comparison"),
        );
        let firsts: Vec<Duration> = (0..8).map(|_| first.next_delay()).collect();
        let seconds: Vec<Duration> = (0..8).map(|_| second.next_delay()).collect();
        // Every delay, the opening one included: a pair of sequences that
        // agreed on one of them is a pair that retried together that time.
        for (step, (one, other)) in firsts.iter().zip(&seconds).enumerate() {
            assert_ne!(one, other, "both connections waited {one:?} at step {step}");
        }
    }

    #[test]
    fn a_seed_separates_two_connections_and_two_readings() {
        assert_ne!(
            Backoff::seed(1_760_000_000_123_456_789, "mktdata"),
            Backoff::seed(1_760_000_000_123_456_789, "mktdata-comparison"),
            "two connections in one publisher read the same nanosecond"
        );
        assert_ne!(
            Backoff::seed(1_760_000_000_123_456_789, "mktdata"),
            Backoff::seed(1_760_000_000_123_456_790, "mktdata"),
            "one connection across a restart keeps its name"
        );
    }

    #[test]
    fn a_fixed_seed_draws_a_fixed_sequence() {
        // What makes the delays testable at all, and the regression test on the
        // arithmetic: these are this seed's draws, in milliseconds, each one
        // from the window its step opens - 500 to 1,000, then 1,000 to 2,000,
        // 2,000 to 4,000, and so on to the 15,000 to 30,000 the ceiling stops
        // at. A generator whose mixing changed, or a window computed from the
        // wrong end, is a different list.
        let mut backoff = Backoff::new(policy(500, 30_000), SEED);
        let delays: Vec<u128> = (0..8).map(|_| backoff.next_delay().as_millis()).collect();
        assert_eq!(
            delays,
            vec![689, 1_251, 2_801, 5_094, 15_817, 28_718, 16_838, 29_422]
        );
    }

    #[test]
    fn a_reset_returns_to_the_opening_window_rather_than_to_zero() {
        let mut backoff = Backoff::new(policy(500, 30_000), SEED);
        for _ in 0..4 {
            backoff.next_delay();
        }
        backoff.reset();
        let delay = backoff.next_delay();
        // Not zero, and not further along: a venue that closes a healthy
        // connection on purpose - a daily session boundary, a maintenance
        // window - must not be reconnected against instantly, and must not be
        // reconnected against at the ceiling either.
        assert!(
            delay >= Duration::from_millis(500) && delay <= Duration::from_secs(1),
            "a reset drew {delay:?}, outside the opening window"
        );
    }

    #[test]
    fn a_reset_does_not_draw_the_same_numbers_again() {
        // Re-seeding on reset would hand every connection that proves itself
        // the same delays it had last time, and would put two connections that
        // reset on one event - a venue's session boundary - back in step.
        let mut backoff = Backoff::new(policy(500, 30_000), SEED);
        let before: Vec<Duration> = (0..4).map(|_| backoff.next_delay()).collect();
        backoff.reset();
        let after: Vec<Duration> = (0..4).map(|_| backoff.next_delay()).collect();
        assert_ne!(before, after, "the draws repeated: {after:?}");
    }

    #[test]
    fn a_ceiling_near_the_end_of_the_range_does_not_wrap_to_no_delay() {
        let initial = Duration::from_secs(u64::MAX / 3);
        let mut backoff = Backoff::new(
            BackoffPolicy::new(initial, Duration::MAX).expect("a valid policy"),
            SEED,
        );
        let ceiling = backoff.ceiling();
        let first = backoff.next_delay();
        let next_ceiling = backoff.ceiling();
        let second = backoff.next_delay();
        assert!(first >= initial, "{first:?} is below the initial delay");
        assert!(
            first <= ceiling,
            "{first:?} is above the ceiling {ceiling:?}"
        );
        assert!(!second.is_zero());
        assert!(
            second <= next_ceiling,
            "{second:?} is above the ceiling {next_ceiling:?}"
        );
    }

    #[test]
    fn a_zero_initial_delay_is_refused_rather_than_doubling_to_zero_forever() {
        let error = BackoffPolicy::new(Duration::ZERO, Duration::from_secs(30))
            .expect_err("zero must not be accepted");
        assert!(matches!(error, ConfigError::ZeroDuration { .. }), "{error}");
    }

    #[test]
    fn a_maximum_below_the_initial_delay_is_refused_rather_than_clamped() {
        let error = BackoffPolicy::new(Duration::from_secs(30), Duration::from_millis(500))
            .expect_err("a transposed pair must not be accepted");
        assert!(
            matches!(error, ConfigError::BackoffInverted { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_maximum_equal_to_the_initial_delay_states_the_lockstep_it_leaves() {
        // The one configuration the draw cannot help: the window is a point, so
        // every connection of a publisher waits the same second and retries
        // together, which is what this whole type exists to end. It loads,
        // because a fixed retry cadence is something to mean - so the line is
        // what the operator who did not mean it gets, and until it existed a
        // publisher in exact lockstep looked from the outside like one that had
        // been given a window.
        let line = policy(1_000, 1_000)
            .lockstep_line()
            .expect("a policy with no window in it must say so");
        // Both keys, the value, and the consequence: an operator handed the
        // consequence alone has to go and find which pair produced it, and one
        // handed the pair alone has no reason to act.
        for named in [
            "reconnect_backoff_initial",
            "reconnect_backoff_max",
            "1s",
            "lockstep",
        ] {
            assert!(
                line.contains(named),
                "the line does not name {named}: {line}"
            );
        }
    }

    #[test]
    fn a_policy_with_a_window_to_draw_in_says_nothing() {
        // The other direction, because a line stated for every policy is a line
        // nobody reads. The documented pair, then a maximum below twice the
        // initial delay - whose window is narrow and real - and then the
        // narrowest a configuration can express short of none, which is also
        // the case that says why this is not a refusal: one nanosecond of
        // window is the same lockstep and no refusal keyed on equality would
        // have caught it either.
        assert!(policy(500, 30_000).lockstep_line().is_none());
        assert!(policy(500, 600).lockstep_line().is_none());
        let narrowest = BackoffPolicy::new(
            Duration::from_millis(500),
            Duration::from_millis(500) + Duration::from_nanos(1),
        )
        .expect("a valid policy");
        assert!(
            narrowest.lockstep_line().is_none(),
            "a window one nanosecond wide is a window"
        );
    }
}
