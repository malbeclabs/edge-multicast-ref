use dz_edge_core::PortRole;
use dz_recorder_core::{ChannelInstance, RecordedDatagram, RecvTsKind};
use std::net::SocketAddrV4;

#[test]
fn a_kernel_stamp_is_distinguishable_from_a_fallback() {
    // A latency computed from an application-level stamp measures our own
    // scheduler. An archive that cannot say which kind it holds cannot be
    // trusted for latency at all, so the kind is carried, never inferred.
    assert_ne!(RecvTsKind::KernelSoftware, RecvTsKind::ApplicationFallback);
}

#[test]
fn the_channel_instance_is_source_channel_and_port() {
    // Two publishers may serve the same Channel ID to the same group and port,
    // each advancing its own sequence space. A key any coarser reads the
    // alternation as backward motion.
    let a: SocketAddrV4 = "192.0.2.1:0".parse().unwrap();
    let b: SocketAddrV4 = "192.0.2.2:0".parse().unwrap();
    assert_ne!(
        ChannelInstance::new(*a.ip(), 1, 40000),
        ChannelInstance::new(*b.ip(), 1, 40000),
    );
    assert_ne!(
        ChannelInstance::new(*a.ip(), 1, 40000),
        ChannelInstance::new(*a.ip(), 1, 40001),
    );
}

#[test]
fn a_recorded_datagram_borrows_its_payload() {
    let buf = [0u8; 64];
    let dg = RecordedDatagram {
        payload: &buf,
        src: "192.0.2.1:40000".parse().unwrap(),
        dst: "233.252.0.10:40000".parse().unwrap(),
        role: PortRole::Mktdata,
        recv_ts_ns: 1,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta: 0,
        ttl: Some(1),
        link_headers: None,
        wire_payload_len: 64,
    };
    assert_eq!(dg.payload.len(), 64);
    assert_eq!(dg.role.as_str(), "mktdata");
}
