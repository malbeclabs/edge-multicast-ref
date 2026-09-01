//! The headline sequence, at the altitude it actually runs: a real process, a
//! real signal, and an object on the disk afterwards.
//!
//! Everything else about the shutdown is tested over a fake capture, which
//! proves the ordering and not the wiring. This proves the wiring — that the
//! handler reaches the flag, that the flag reaches the loop while the feed is
//! quiet, and that the segment the process was holding is published before it
//! exits. A recorder that hangs on SIGTERM is one a supervisor eventually
//! SIGKILLs, and SIGKILL is exactly what abandons the open segment.
//!
//! Behind the `socket-e2e` feature: it sends over `IP_MULTICAST_LOOP` on a
//! documentation-range group, which is a property of the host and not of the
//! build. Every address here is documentation-range; this repository is public.
#![cfg(feature = "socket-e2e")]
#![forbid(unsafe_code)]

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_dz-recorder");
const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 21);
const MKTDATA_PORT: u16 = 41881;
const REFDATA_PORT: u16 = 41882;

/// The address of whichever interface this host would send multicast out of.
fn multicast_interface() -> Ipv4Addr {
    let probe = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).expect("probe socket");
    probe.connect((GROUP, 9)).expect("multicast route");
    match probe.local_addr().expect("local address") {
        SocketAddr::V4(addr) => *addr.ip(),
        SocketAddr::V6(_) => unreachable!("bound to an IPv4 address"),
    }
}

fn config(dir: &Path, interface: Ipv4Addr) -> String {
    format!(
        r#"
site     = "site-a"
recorder = "recorder-1"
env      = "test"

[[feed]]
spec            = "top-of-book"
multicast_group = "{GROUP}"
interface       = "{interface}"
mktdata_port    = {MKTDATA_PORT}
refdata_port    = {REFDATA_PORT}

[capture]
mode   = "socket"
buffer = "8MiB"

[archive]
staging_dir     = "{staging}"
completed_dir   = "{completed}"
rotate_bytes    = "16MiB"
rotate_interval = "3600s"
compression     = "zstd"
staging_max     = "64MiB"

[metrics]
listen_addr = "127.0.0.1:0"
"#,
        staging = dir.join("staging").display(),
        completed = dir.join("completed").display(),
    )
}

/// A datagram with a readable 24-byte header, which is all the recorder reads.
fn datagram(sequence_number: u64) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    buf[0..2].copy_from_slice(&0x4442u16.to_le_bytes());
    buf[2] = 1;
    buf[3] = 7;
    buf[4..12].copy_from_slice(&sequence_number.to_le_bytes());
    buf[12..20].copy_from_slice(&1_772_000_000_000_000_000u64.to_le_bytes());
    buf[20] = 1;
    buf[21] = 0;
    buf[22..24].copy_from_slice(&40u16.to_le_bytes());
    buf
}

fn objects_in(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// SIGTERM, without reaching for a crate: the binary under test is a child
/// process and `kill` is what a supervisor uses.
fn terminate(child: &Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success(), "SIGTERM was not delivered");
}

#[test]
fn sigterm_publishes_the_open_segment_and_exits_zero() {
    let interface = multicast_interface();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config_path = dir.path().join("recorder.toml");
    std::fs::write(&config_path, config(dir.path(), interface)).expect("the configuration");
    let completed = dir.path().join("completed").join("top-of-book");

    let mut child = Command::new(BINARY)
        .arg("--config")
        .arg(&config_path)
        .spawn()
        .expect("the binary under test runs");

    // Sent until the archive has something in it, because the recorder binds
    // and joins on its own schedule and a datagram sent before the join is a
    // datagram nothing asked for.
    let sender = UdpSocket::bind((interface, 0)).expect("sender socket");
    sender.set_multicast_loop_v4(true).expect("multicast loop");
    sender.set_multicast_ttl_v4(1).expect("multicast ttl");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut sequence_number = 0;
    while !dir.path().join("staging").join("top-of-book").exists() {
        assert!(
            Instant::now() < deadline,
            "the recorder never opened a segment"
        );
        sender
            .send_to(&datagram(sequence_number), (GROUP, MKTDATA_PORT))
            .expect("send");
        sequence_number += 1;
        std::thread::sleep(Duration::from_millis(20));
    }
    for _ in 0..50 {
        sender
            .send_to(&datagram(sequence_number), (GROUP, MKTDATA_PORT))
            .expect("send");
        sequence_number += 1;
    }
    std::thread::sleep(Duration::from_millis(300));

    // The feed goes quiet here, which is the case that matters: the loop must
    // still see the flag with nothing arriving.
    terminate(&child);

    let waited = Instant::now();
    let status = loop {
        assert!(
            waited.elapsed() < Duration::from_secs(20),
            "the recorder did not exit after SIGTERM: a quiet feed cannot be shut down"
        );
        if let Some(status) = child.try_wait().expect("waiting on the child") {
            break status;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    assert_eq!(status.code(), Some(0), "the shutdown reported a failure");
    let landed = objects_in(&completed);
    assert!(
        landed.iter().any(|name| name.ends_with(".pcapng.zst")),
        "the open segment was not published: {landed:?}"
    );
    assert!(
        landed.iter().any(|name| name.ends_with(".manifest.json")),
        "the object landed without its manifest: {landed:?}"
    );
}
