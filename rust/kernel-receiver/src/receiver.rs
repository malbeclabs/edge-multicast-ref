use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, Socket, Type};

use crate::config::Config;
use crate::shred_parser;
use crate::stats::Stats;

/// Create a UDP socket bound to the given port, joined to the multicast group
/// on the specified interface. Sets SO_RCVBUF.
fn create_multicast_socket(
    port: u16,
    multicast_group: &Ipv4Addr,
    interface_ip: &Ipv4Addr,
    recv_buf_size: usize,
) -> Result<Socket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("creating UDP socket")?;

    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    let bind_addr = std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    socket
        .bind(&bind_addr.into())
        .with_context(|| format!("binding to port {port}"))?;

    socket
        .join_multicast_v4(multicast_group, interface_ip)
        .with_context(|| {
            format!("joining multicast {multicast_group} on interface {interface_ip}")
        })?;

    socket
        .set_recv_buffer_size(recv_buf_size)
        .context("setting SO_RCVBUF")?;

    socket.set_nonblocking(true)?;

    Ok(socket)
}

/// Resolve the interface name to its IPv4 address.
/// Falls back to 0.0.0.0 if resolution fails.
fn resolve_interface_ip(interface: &str) -> Ipv4Addr {
    if let Ok(output) = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", interface])
        .output()
    {
        if let Ok(stdout) = std::str::from_utf8(&output.stdout) {
            // Parse line like: "26: doublezero1    inet 169.254.10.233/31 ..."
            for part in stdout.split_whitespace() {
                if let Some(ip_str) = part.split('/').next() {
                    if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                        return ip;
                    }
                }
            }
        }
    }
    eprintln!("warning: could not resolve IP for interface '{interface}', using 0.0.0.0");
    Ipv4Addr::UNSPECIFIED
}

/// Run the receive loop. Blocks until `shutdown` is set to true.
pub fn run_recv_loop(
    config: &Config,
    stats: Arc<RwLock<Stats>>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let multicast_group: Ipv4Addr = config
        .network
        .multicast_group
        .parse()
        .context("parsing multicast group address")?;
    let interface_ip = resolve_interface_ip(&config.network.interface);

    eprintln!(
        "Binding to interface {} ({}), multicast group {}",
        config.network.interface, interface_ip, multicast_group
    );

    let shred_socket = create_multicast_socket(
        config.network.shred_port,
        &multicast_group,
        &interface_ip,
        config.network.recv_buffer_size,
    )
    .context("creating shred socket")?;

    let heartbeat_socket = create_multicast_socket(
        config.network.heartbeat_port,
        &multicast_group,
        &interface_ip,
        config.network.recv_buffer_size,
    )
    .context("creating heartbeat socket")?;

    eprintln!(
        "Listening for shreds on port {}, heartbeats on port {}",
        config.network.shred_port, config.network.heartbeat_port
    );

    let mut buf = [0u8; 2048];
    let mut poll_fds = [
        libc::pollfd {
            fd: shred_socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: heartbeat_socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    while !shutdown.load(Ordering::Relaxed) {
        // Poll with 100ms timeout so we can check shutdown flag
        let ready = unsafe { libc::poll(poll_fds.as_mut_ptr(), 2, 100) };

        if ready < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err).context("poll() failed");
        }

        if ready == 0 {
            continue;
        }

        // Check shred socket
        if poll_fds[0].revents & libc::POLLIN != 0 {
            loop {
                match shred_socket.recv(unsafe {
                    &mut *(&mut buf as *mut [u8] as *mut [std::mem::MaybeUninit<u8>])
                }) {
                    Ok(n) => {
                        let payload = &buf[..n];
                        match shred_parser::parse_shred(payload) {
                            Some(parsed) => {
                                let mut s = stats.write().unwrap();
                                s.record_shred(
                                    parsed.slot,
                                    parsed.is_data,
                                    parsed.index,
                                    parsed.fec_set_index,
                                    parsed.signature,
                                );
                            }
                            None => {
                                stats.write().unwrap().record_parse_error();
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        eprintln!("recv error on shred socket: {e}");
                        break;
                    }
                }
            }
            poll_fds[0].revents = 0;
        }

        // Check heartbeat socket
        if poll_fds[1].revents & libc::POLLIN != 0 {
            loop {
                match heartbeat_socket.recv(unsafe {
                    &mut *(&mut buf as *mut [u8] as *mut [std::mem::MaybeUninit<u8>])
                }) {
                    Ok(_n) => {
                        stats.write().unwrap().record_heartbeat();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        eprintln!("recv error on heartbeat socket: {e}");
                        break;
                    }
                }
            }
            poll_fds[1].revents = 0;
        }
    }

    eprintln!("Receiver shutting down");
    Ok(())
}
