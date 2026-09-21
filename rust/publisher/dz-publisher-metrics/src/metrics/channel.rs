use std::collections::HashMap;

use prometheus::{GaugeVec, Registry};

use crate::labels::channel_id_label;
use crate::opts::opts;

/// Per-channel liveness: when each Channel ID last carried a message the
/// upstream's activity put there, rather than one this publisher paced.
///
/// # A proposed addition to the normative set
///
/// The governing playbook carries `dz_publisher_idle_guard_last_update_timestamp_seconds`,
/// which is one gauge for the whole process: any message reaching any wire
/// refreshes it. That is the right shape for the guard it is named after — the
/// idle guard ends the process, and a guard that ended it over one channel
/// would restart every other channel this publisher carries. It is the wrong
/// shape for a question an operator asks about one channel, and a publisher
/// emitting several feeds has no other series that answers it: a channel whose
/// upstream has died keeps heartbeating, keeps republishing a `Valid` manifest,
/// and keeps its instruments in the published set, while its sibling's traffic
/// holds the process-wide gauge at *now*.
///
/// No existing family could hold it.
/// `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds` is per Channel
/// ID and measures the opposite thing: a heartbeat is paced by this publisher,
/// so it is exactly what stays fresh on a dead channel.
/// `dz_publisher_egress_datagrams_total` counts those heartbeats too, and
/// carries no Channel ID. `dz_publisher_egress_sequence_current` advances on
/// every datagram for the same reason. And the process-wide gauge above cannot
/// grow a `channel_id` label without changing what the playbook's own series
/// means.
///
/// # No `port_role` label
///
/// The messages that set this gauge — a quote, a trade, a level update, a book
/// clear, an instrument reset — are `mktdata` messages and the specification
/// permits them nowhere else. A label whose only value is `mktdata` is not a
/// dimension of what is being measured, and pre-creating the other two roles
/// would publish a series nothing can ever write to while its `HELP` text asks
/// for a staleness rule against it.
/// `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds` gates its own
/// pre-creation on the roles a heartbeat is permitted on to avoid exactly that.
/// The three `channel_id`-keyed reference-data gauges carry no `port_role`
/// either, so this is the majority shape for a per-channel series and not an
/// exception to one.
///
/// # The pre-created 0, and what it costs an alert
///
/// Every declared Channel ID gets a series at 0 from startup, because the
/// condition this family exists for includes *a channel that has published
/// nothing at all* and an alert cannot fire on a series that is absent. The
/// cost is that no `dz_publisher_uptime_seconds` guard can suppress that 0: the
/// distance between it and a real timestamp is the whole Unix epoch rather than
/// a function of uptime, so a staleness rule has to **exclude** the 0 series
/// and alert on them separately. The crate `README` carries both rules.
pub struct ChannelMetrics {
    last_published_timestamp_seconds: GaugeVec,
}

impl ChannelMetrics {
    /// `channel_ids` is every Channel ID this publisher sends on, so the gauge
    /// exists at 0 for each of them from startup rather than appearing only
    /// once a channel has published something — which is the case the series
    /// most exists for.
    pub(crate) fn new(
        registry: &Registry,
        labels: &HashMap<String, String>,
        channel_ids: &[u8],
    ) -> Self {
        let last_published_timestamp_seconds = GaugeVec::new(
            opts(
                "dz_publisher_channel_last_published_timestamp_seconds",
                "A proposed addition to the normative set. Unix timestamp the upstream's activity \
                 last put a message on this Channel ID: a quote, a trade, a level update, a book \
                 clear or an instrument reset. Nothing this publisher paces itself sets it — \
                 heartbeats, instrument definitions, manifests and snapshots all go on being sent \
                 by a channel whose upstream has died, which is why they are excluded. \
                 Pre-created at 0, and that 0 is the whole Unix epoch away from a real timestamp, \
                 so no `dz_publisher_uptime_seconds` guard can suppress it: a staleness rule must \
                 exclude `== 0` and alert on those separately. A channel whose instruments are \
                 dormant is silent and healthy, so alert on this channel being far staler than \
                 the freshest non-zero channel of the same publisher rather than on `time() - \
                 this` alone.",
                labels,
            ),
            &["channel_id"],
        )
        .expect("static metric definition");
        registry
            .register(Box::new(last_published_timestamp_seconds.clone()))
            .expect("static metric registration");
        for channel_id in channel_ids {
            last_published_timestamp_seconds.with_label_values(&[channel_id_label(*channel_id)]);
        }

        Self {
            last_published_timestamp_seconds,
        }
    }

    /// Sets the Unix timestamp the upstream's activity last put a message on
    /// `channel_id`.
    pub fn set_last_published(&self, channel_id: u8, unix_seconds: f64) {
        self.last_published_timestamp_seconds
            .with_label_values(&[channel_id_label(channel_id)])
            .set(unix_seconds);
    }
}
