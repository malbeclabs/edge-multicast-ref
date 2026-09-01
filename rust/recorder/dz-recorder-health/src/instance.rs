//! Per-channel-instance state: what this tier holds beside the shared sequence
//! tracker, and the label that keys its series.
//!
//! Everything here is keyed on the channel instance — `(source address,
//! Channel ID, destination port)` — and nothing coarser. An operator may run
//! two publishers serving one channel to one group and port, each advancing its
//! own sequence space and its own `Reset Count`; a tracker keyed any less finely
//! reads every alternation as backward motion in one direction, and lets one
//! publisher's heartbeats cover the other's total outage in the other.
//!
//! Continuity, reordering, duplication and the era ordinal are
//! [`SequenceTracker`]'s, in `dz-recorder-core`: they are the same rules the
//! offline analysis tier decides on, and an offline loader must be able to reach
//! them without linking a metrics registry and a Prometheus exposition.

use std::net::Ipv4Addr;

use dz_edge_core::PortRole;
use dz_recorder_core::SequenceTracker;

use crate::metrics::InstanceChildren;

/// An `Ipv4Addr` rendered as a label value without touching the heap.
///
/// `source` labels the channel-instance series, and the address has to become a
/// `&str` to reach a label vector. Formatting it through `to_string` would put
/// an allocation on the path that opens an instance, which on an any-source
/// join is a path an unknown sender controls the rate of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLabel {
    text: [u8; Self::MAX_LEN],
    len: u8,
}

impl SourceLabel {
    /// `255.255.255.255`.
    const MAX_LEN: usize = 15;

    #[must_use]
    pub fn new(address: Ipv4Addr) -> Self {
        let mut text = [0; Self::MAX_LEN];
        let mut len = 0;
        for (index, octet) in address.octets().iter().enumerate() {
            if index > 0 {
                text[len] = b'.';
                len += 1;
            }
            len += write_decimal(&mut text[len..], *octet);
        }
        Self {
            text,
            len: len as u8,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.text[..self.len as usize])
            .expect("only ASCII digits and dots are ever written")
    }
}

/// Writes `value` in decimal at the front of `dst` and returns its width.
fn write_decimal(dst: &mut [u8], mut value: u8) -> usize {
    let mut digits = [0; 3];
    let mut width = 0;
    loop {
        digits[width] = b'0' + value % 10;
        value /= 10;
        width += 1;
        if value == 0 {
            break;
        }
    }
    for (offset, digit) in digits[..width].iter().rev().enumerate() {
        dst[offset] = *digit;
    }
    width
}

/// One channel instance's entry in the bounded map.
pub(crate) struct InstanceState {
    pub(crate) sequence: SequenceTracker,
    pub(crate) source: SourceLabel,
    /// Held because `ChannelInstance` keys on the destination port, and dropping
    /// an evicted instance's series needs the `role` label the port maps to.
    pub(crate) role: PortRole,
    /// Receive timestamp of the most recent datagram, in nanoseconds. This is
    /// the least-recently-seen key eviction orders on, and it is the receive
    /// timestamp rather than a clock read here so that eviction order over a
    /// replayed archive is the order the traffic actually had.
    pub(crate) last_seen_ns: u64,
    /// `None` until the first heartbeat-shaped datagram, so the first one
    /// establishes the baseline rather than reporting an interval measured from
    /// the epoch.
    pub(crate) last_heartbeat_ns: Option<u64>,
    /// Whether the operator declared this source. A declared source's series
    /// were pre-created at startup and survive eviction: an operator's own
    /// publisher must not vanish from a dashboard because unknown senders
    /// filled a bounded map.
    pub(crate) declared_source: bool,
    pub(crate) children: InstanceChildren,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_label_renders_every_octet_width() {
        assert_eq!(
            SourceLabel::new(Ipv4Addr::new(192, 0, 2, 1)).as_str(),
            "192.0.2.1"
        );
        assert_eq!(
            SourceLabel::new(Ipv4Addr::new(255, 255, 255, 255)).as_str(),
            "255.255.255.255"
        );
        assert_eq!(
            SourceLabel::new(Ipv4Addr::new(0, 0, 0, 0)).as_str(),
            "0.0.0.0"
        );
    }
}
