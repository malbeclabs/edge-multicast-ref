//! An `Input` that reads payloads from a directory instead of a connection.
//!
//! `[adapter.replay]` was a configuration key with nothing behind it: it
//! parsed, and [`run`](crate::run) did not read it. This is what it now
//! selects, and it exists so that the one path a venue's `main` actually calls
//! can be exercised end to end without a network — the real config, the real
//! registry, the real adapter, the real lowering, the real sockets, and
//! recorded upstream bytes in place of a live venue.
//!
//! # Why this is an `Input` and not something new
//!
//! Because a replay *is* a transport: it produces payloads and knows nothing
//! about what they mean. Making it anything else would have put a second way to
//! reach an adapter beside the one the boundary defines, and then two paths to
//! keep in step. The adapter cannot tell the difference, which is the property
//! that makes the exercise worth anything — and it is the same property that
//! lets the same adapter be re-run offline over an archive.
//!
//! # What a venue has to link for it
//!
//! `[ingress] kind = "uds"` resolves only in a binary that links the transport
//! marker for it, because [`Kind::is_linked`](dz_ingress_core::Kind) is what
//! keeps a configuration file from naming a transport the binary does not
//! contain. Those markers are turned on by whoever assembles the binary, not
//! by this crate, so a venue whose `main` wants a replay run depends on
//! `dz-ingress-core` with `features = ["uds"]` — a dev dependency is enough
//! when, as here, the replay is only ever an example or a test. Without it
//! startup refuses and names what the binary does link, which is the honest
//! failure and not an obvious one to read the first time.
//!
//! # What it does not pretend to be
//!
//! It is not the archive format the offline re-lowering will read. That format
//! has to guarantee receive order and carry the connection each payload arrived
//! on; this reads files in name order and calls that the order. It is a
//! development and validation vehicle, and the doc on
//! [`ReplayInput::open`] says what it assumes.

use std::path::{Path, PathBuf};
use std::time::Duration;

use dz_adapter_core::{ConnectionId, DisconnectReason};
use dz_ingress_core::{BoxFuture, IngressError, Input, Received, UpstreamMessage};

/// One recorded upstream payload.
struct Recorded {
    /// The file's own name, so a run can say what it replayed rather than how
    /// many bytes it read.
    name: String,
    bytes: Vec<u8>,
}

/// How long a replay waits before its first payload. See
/// [`ReplayInput::connect`].
const SETTLE: Duration = Duration::from_millis(1_500);

/// A transport that hands over recorded payloads, in name order, once.
pub struct ReplayInput {
    connection: ConnectionId,
    payloads: Vec<Recorded>,
    /// How many have been handed over. A replay does not loop: a second pass
    /// over the same payloads would fold the same deltas into the venue's book
    /// twice, and a book that applied one delta twice is not a book any
    /// subscriber would have.
    at: usize,
    /// What the adapter wrote to the upstream sink. Kept rather than discarded
    /// so a replay can report that the subscriptions were composed, which is
    /// the one thing about `on_connected` an offline run can still check.
    sent: Vec<String>,
}

impl ReplayInput {
    /// Read every file directly under `dir`, in name order.
    ///
    /// **Name order is the assumption**, and it is the one to know about: a
    /// recorder that wrote `9.json` and `10.json` replays them in the order a
    /// string comparison gives, not the order they arrived. Zero-pad, or
    /// number them so that the two orders agree.
    ///
    /// Subdirectories are ignored rather than walked, so a fixture directory
    /// can hold a `README` beside its payloads without the reader having to
    /// know which is which — and a directory holding no files at all is an
    /// error rather than a run that publishes nothing and exits cleanly.
    ///
    /// # Errors
    ///
    /// [`IngressError::Fatal`] for a directory that cannot be read or holds no
    /// payload. Fatal rather than retryable: no amount of reconnecting will put
    /// a file there, and the driver stops instead of spinning.
    pub fn open(connection: ConnectionId, dir: &Path) -> Result<Self, IngressError> {
        let fatal = |detail: String| IngressError::Fatal { detail };

        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(|e| fatal(format!("{}: {e}", dir.display())))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        entries.sort();

        let payloads: Vec<Recorded> = entries
            .iter()
            .map(|path| {
                std::fs::read(path)
                    .map(|bytes| Recorded {
                        name: path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                        bytes,
                    })
                    .map_err(|e| fatal(format!("{}: {e}", path.display())))
            })
            .collect::<Result<_, _>>()?;

        if payloads.is_empty() {
            return Err(fatal(format!(
                "{} holds no payload to replay",
                dir.display()
            )));
        }

        Ok(Self {
            connection,
            payloads,
            at: 0,
            sent: Vec::new(),
        })
    }

    /// How many payloads are waiting.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.payloads.len().saturating_sub(self.at)
    }

    /// What the adapter wrote upstream, in order.
    #[must_use]
    pub fn sent(&self) -> &[String] {
        &self.sent
    }

    /// The payloads this will replay, in the order it will replay them.
    ///
    /// For the line a run prints: a replay that read the wrong directory, or
    /// read it in the wrong order, is a thing to see rather than infer from the
    /// events that followed.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.payloads.iter().map(|p| p.name.as_str()).collect()
    }
}

impl Input for ReplayInput {
    fn connection(&self) -> ConnectionId {
        self.connection
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(async move {
            // **A recording has no handshake, and that is a problem.** A live
            // connection spends time — a socket, a TLS negotiation, an
            // authentication, a subscription answered — and the publisher
            // spends it publishing reference data. A replay yields its first
            // payload in microseconds, so the first market-data messages go out
            // before any `InstrumentDefinition` has, and a subscriber holds a
            // quote for an `Instrument ID` it cannot resolve. That is not a
            // defect in the replay: it is the ordering a live feed gets for
            // free, and an offline run has to buy.
            //
            // So the connection settles first. Long enough for the definition
            // cycle to have published at least once at any cadence an operator
            // would configure, and paid once per run rather than per payload.
            tokio::time::sleep(SETTLE).await;
            // A replay connects once and does not reconnect: the payloads it
            // holds are already spent when they run out, and re-establishing
            // would mean replaying them, which is the one thing that must not
            // happen. So a second connect is refused rather than silently
            // rewinding.
            if self.at > 0 {
                return Err(IngressError::Fatal {
                    detail: "a replay does not reconnect; its payloads are spent".to_string(),
                });
            }
            Ok(())
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(async move {
            // There is nowhere to send it, and that is not a failure: what the
            // adapter writes on connect is a subscription, and a recording
            // already contains whatever the venue sent in answer to one. It is
            // kept so a run can report that it was composed.
            self.sent.push(match message {
                UpstreamMessage::Text(text) => text.to_string(),
                UpstreamMessage::Binary(bytes) => format!("{} binary bytes", bytes.len()),
            });
            Ok(())
        })
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            let Some(payload) = self.payloads.get(self.at) else {
                // The recording is finished, and that ends the run rather than
                // idling: an offline run that sat idle after its last payload
                // would report the idle guard, which says nothing about the
                // replay and everything about there being no more of it.
                return Err(IngressError::Ended {
                    reason: DisconnectReason::RemoteClose,
                    detail: format!("the recording's {} payloads are spent", self.payloads.len()),
                });
            };
            self.at += 1;
            // A line per payload, because an offline run's whole value is
            // knowing which recorded bytes produced which messages — and a
            // replay that stopped early is otherwise indistinguishable from a
            // publisher that dropped everything after the first.
            eprintln!(
                "  replayed {} ({} bytes), {} left",
                payload.name,
                payload.bytes.len(),
                self.payloads.len() - self.at
            );
            // No timestamp of our own: a recording carries the venue's clock in
            // its own bytes, and a receive stamp invented here would be this
            // process's wall clock pretending to be a subscriber's.
            Ok(Received::Payload {
                bytes: &payload.bytes,
                ts_ns: None,
            })
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
