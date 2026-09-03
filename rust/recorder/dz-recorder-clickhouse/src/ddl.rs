//! The checked-in schema, embedded so that a `--check` and a test apply exactly
//! the files a deploy applies.
//!
//! Numbered, one file per migration, as `demo/clickhouse/migrations` already is.
//! There is no migration framework: a schema a process applies to itself at
//! startup changes when a binary is rolled, and these tables are read by
//! dashboards that outlive any one loader build. So the files are applied by
//! hand or by the deploy, and this module exists so that nothing has to retype
//! them to test them.

/// One numbered file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    pub name: &'static str,
    pub sql: &'static str,
}

const ROWS: &str = include_str!("../db/clickhouse/001_recorder_rows.sql");
const RETENTION: &str = include_str!("../db/clickhouse/002_recorder_retention.sql");
const ERA_RANK: &str = include_str!("../db/clickhouse/003_recorder_era_rank.sql");

/// Every migration, in the order they are applied.
#[must_use]
pub const fn migrations() -> [Migration; 3] {
    [
        Migration {
            name: "001_recorder_rows.sql",
            sql: ROWS,
        },
        Migration {
            name: "002_recorder_retention.sql",
            sql: RETENTION,
        },
        Migration {
            name: "003_recorder_era_rank.sql",
            sql: ERA_RANK,
        },
    ]
}

impl Migration {
    /// The statements in this file, split on `;` at the end of a line.
    ///
    /// A column store's HTTP interface takes one statement per request, so a
    /// file has to be split to be applied. The split is on a semicolon that ends
    /// a line rather than on every semicolon, because a comment in these files
    /// contains prose and prose contains punctuation — and a splitter that cut
    /// mid-comment would send half a statement.
    #[must_use]
    pub fn statements(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut current = String::new();
        for line in self.sql.lines() {
            let trimmed = line.trim_end();
            // A comment line is kept inside the statement it precedes rather
            // than stripped: the server ignores it, and an operator reading
            // `SHOW CREATE TABLE` output against these files wants to find the
            // same words.
            current.push_str(line);
            current.push('\n');
            if trimmed.ends_with(';') && !trimmed.trim_start().starts_with("--") {
                let statement = current.trim().to_owned();
                if !statement.is_empty() {
                    out.push(statement);
                }
                current.clear();
            }
        }
        let tail = current.trim();
        if !tail.is_empty() && !tail.lines().all(|l| l.trim_start().starts_with("--")) {
            out.push(tail.to_owned());
        }
        out
    }
}
