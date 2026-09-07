//! The sizing measurement, pointed at archives that were actually recorded.
//!
//! The tests measure a fixture, which proves the count is what it claims. This
//! is the other half: the number itself, for a real feed, which is what the
//! design says must exist before that feed's derivation is enabled. It is an
//! example rather than a service on purpose — it reads objects, prints a table
//! and exits, and nothing downstream consumes it.
//!
//! ```text
//! cargo run -p dz-recorder-events --example sizing -- \
//!     --feed market-by-price archive-*.pcapng.zst
//! ```
//!
//! **Every archive named becomes one window.** The multiplier is a property of
//! the feed and its publisher over a stretch of time, and a stretch worth
//! deciding against has to include a burst and a snapshot cycle — one object
//! usually does not. The report says which of the two a window is missing rather
//! than leaving a reader to trust a ratio taken over a quiet minute.
//!
//! A torn archive is reported and its ratio is not: replay yields what survived
//! a tear, so a window that ends inside a block has a denominator that stopped
//! before its numerator did.

use std::path::PathBuf;
use std::process::ExitCode;

use dz_edge_core::Feed;
use dz_edge_mbp::MarketByPrice;
use dz_edge_tob::TopOfBook;
use dz_recorder_events::Sizing;
use dz_recorder_relower::WireCapture;
use dz_recorder_replay::{ArchiveSource, Termination};

const USAGE: &str = "usage: sizing --feed <market-by-price|top-of-book|0xNNNN> <archive>...";

fn main() -> ExitCode {
    let mut magic: Option<u16> = None;
    let mut archives: Vec<PathBuf> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--feed" => {
                let Some(name) = args.next() else {
                    eprintln!("--feed takes a value\n{USAGE}");
                    return ExitCode::FAILURE;
                };
                match feed_magic(&name) {
                    Some(m) => magic = Some(m),
                    None => {
                        eprintln!("unknown feed {name:?}\n{USAGE}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            path => archives.push(PathBuf::from(path)),
        }
    }

    // Both are required and neither has a default. The `Magic` is the only thing
    // that stops a datagram misrouted from a sibling feed being parsed at the
    // wrong layout, and only the caller knows which feed it believes it is
    // holding; a default window of "whatever was lying around" would produce a
    // number with nothing to say what it is about.
    let Some(magic) = magic else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    if archives.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    }

    let mut capture = WireCapture::new();
    let mut torn = Vec::new();
    for path in &archives {
        let mut source = match ArchiveSource::open(path) {
            Ok(source) => source,
            Err(error) => {
                eprintln!("{}: {error}", path.display());
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = capture.absorb(&mut source, magic) {
            eprintln!("{}: {error}", path.display());
            return ExitCode::FAILURE;
        }
        if source.terminated_by() != Termination::Eof {
            torn.push(path.clone());
        }
    }

    // A window that never held this feed is a question that was not asked, and
    // it exits as one. Printing the empty table and succeeding is the worst of
    // the available behaviours: `--feed top-of-book` against an archive of the
    // other feed produces a header, no rows, and a zero exit — which reads as
    // *this feed is quiet* to the operator and to anything scripting this.
    let sizing = Sizing::of(&capture);
    if sizing.is_empty() {
        let skipped = capture.skipped();
        eprintln!(
            "no datagram in {} archive(s) carried this feed's Magic: {} of {} carried another \
             feed's. Nothing here is a ratio.",
            archives.len(),
            skipped.foreign_magic,
            capture.datagrams()
        );
        return ExitCode::FAILURE;
    }

    print!("{sizing}");
    println!(
        "\n{} archive(s), {} datagram(s) read in all, {:?} skipped",
        archives.len(),
        capture.datagrams(),
        capture.skipped()
    );
    for path in &torn {
        println!(
            "torn: {} — this window's ratio is not usable",
            path.display()
        );
    }
    if torn.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// The feed's `Magic`, by the name the specification gives the feed.
///
/// A raw `0xNNNN` is accepted beside the two names because an archive of a feed
/// this build has no crate for still has a ratio, and refusing to measure it
/// would make the measurement available only where it is least needed.
fn feed_magic(name: &str) -> Option<u16> {
    match name {
        "market-by-price" | "mbp" => Some(MarketByPrice::MAGIC),
        "top-of-book" | "tob" => Some(TopOfBook::MAGIC),
        hex if hex.starts_with("0x") || hex.starts_with("0X") => {
            u16::from_str_radix(&hex[2..], 16).ok()
        }
        decimal => decimal.parse().ok(),
    }
}
