//! What the recorder says about itself inside every archive it writes.
//!
//! This fills the pcapng Section Header block options. It is here, rather than
//! in the archive crate, because an object separated from its context — copied,
//! renamed, or pulled out of a bucket by hand — must still be able to say which
//! recorder, which build and which configuration wrote it. A finding is only
//! attributable if this travels with the bytes.

/// Provenance, carried in the archive rather than beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderIdentity {
    /// Label on every `dz_recorder_*` series and on every object key.
    pub site: String,
    /// Unique within the site.
    pub recorder: String,
    pub env: String,
    pub build_version: String,
    pub build_commit: String,
    /// Hex sha256 of the *parsed* configuration, so that a comment or a
    /// reordering does not invalidate provenance. See
    /// [`config`](crate::config).
    pub config_hash: String,
}

impl RecorderIdentity {
    /// `site/recorder`, which is what goes in the Section Header block's
    /// hardware option: one string that identifies the capture point.
    #[must_use]
    pub fn hardware(&self) -> String {
        format!("{}/{}", self.site, self.recorder)
    }
}
