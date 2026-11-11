//! The placed report as the rule set itself writes it.
//!
//! `fixtures/report-upstream-mbp.json` is the rule set's own golden for its
//! `-rule-report` output, copied unaltered from edge-feed-spec at
//! b9ff080b4019b48c48e021728b9d842d18bb1439. The fixtures beside it are the shape
//! this repository asked for; this one is the shape that arrived, and the test
//! is that the two are the same interface: every entry parses, the fields the
//! rule set adds beyond the ask are tolerated rather than refused, and an entry
//! the rule set places on no instance arrives as one with none.

use dz_recorder_conformance::report::{Outcome, RuleSetReport};

#[test]
fn the_rule_sets_own_placed_report_parses_as_the_declared_shape() {
    let bytes = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/report-upstream-mbp.json"),
    )
    .expect("the fixture is checked in");
    let report = RuleSetReport::from_json(&bytes).expect("the rule set's own report parses");
    assert_eq!(report.feed, "mbp");
    assert_eq!(report.rules.len(), 15);

    // Violations are split into ranges, one entry each, so one rule on one
    // instance arrives as several entries: the runner reduces them.
    let violations = report
        .rules
        .iter()
        .filter(|r| r.outcome == Outcome::Violation)
        .count();
    assert_eq!(violations, 10);
    assert!(report
        .rules
        .iter()
        .filter(|r| r.outcome == Outcome::Violation)
        .all(|r| r.instance.is_some() && r.evidence.is_some()));

    // Rules decided over state every path of a channel fills are placed on no
    // instance, and say so by omission rather than by a guess.
    let unplaced: Vec<_> = report
        .rules
        .iter()
        .filter(|r| r.instance.is_none())
        .collect();
    assert_eq!(unplaced.len(), 5);
    // They name the channel and the port role instead, which is what places
    // them at channel grain.
    assert!(unplaced
        .iter()
        .all(|r| r.channel_id.is_some() && r.port_role.is_some()));

    // No entry states the roles it reads beyond its own instance yet.
    assert!(report.rules.iter().all(|r| r.needs.is_empty()));

    // Every cited range states its era by an ordinal that does not wrap, beside
    // the Reset Count that does; nothing here overflowed the address cap.
    assert!(report
        .rules
        .iter()
        .filter_map(|r| r.evidence)
        .all(|e| e.era_index.is_some()));
    assert!(report.rules.iter().all(|r| !r.overflow));
    assert_eq!(report.unplaced_findings, 0);
}
