//! The address a named interface carries.
//!
//! `AF_PACKET` mode opens the ring on a device by name, but the membership that
//! makes the network deliver the group is a socket join, and a join takes an
//! address. Leaving that address unset asks the kernel for route discovery,
//! which sends the IGMP report out of the default route — and this crate's own
//! contract says the report has to leave by the interface the feed arrives on.
//! On the topology this recorder exists for, a feed arriving over a tunnel that
//! is not the default route, a discovered join never propagates and the feed is
//! silent in a way that looks exactly like a clean one.
//!
//! So the name is resolved here, at startup, where a device that carries no
//! address can be refused by name instead of becoming an outage nobody can see.

use std::net::Ipv4Addr;

use nix::ifaddrs::getifaddrs;
use thiserror::Error;

/// Why a device name could not become an address to join on.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeviceAddressError {
    #[error("reading the host's interfaces: {0}")]
    Interfaces(String),
    #[error(
        "interface `{device}` has no IPv4 address, so there is no address to join the group on. \
         A membership joined by route discovery leaves by the default route, which is not this \
         interface, and a group that never joins is silence a dashboard reads as a clean feed."
    )]
    NoAddress { device: String },
    #[error("the host has no interface named `{device}`")]
    NoSuchDevice { device: String },
}

/// The first IPv4 address assigned to `device`.
///
/// # Errors
///
/// [`DeviceAddressError::NoSuchDevice`] when the host has no such interface and
/// [`DeviceAddressError::NoAddress`] when it has one carrying no IPv4 address —
/// a device that is up but unnumbered, which is a real state and not one to
/// join a group from.
pub fn device_address(device: &str) -> Result<Ipv4Addr, DeviceAddressError> {
    let interfaces = getifaddrs().map_err(|e| DeviceAddressError::Interfaces(e.to_string()))?;
    let mut seen = false;
    for interface in interfaces {
        if interface.interface_name != device {
            continue;
        }
        seen = true;
        if let Some(address) = interface
            .address
            .as_ref()
            .and_then(|storage| storage.as_sockaddr_in())
        {
            return Ok(address.ip());
        }
    }
    if seen {
        Err(DeviceAddressError::NoAddress {
            device: device.to_owned(),
        })
    } else {
        Err(DeviceAddressError::NoSuchDevice {
            device: device.to_owned(),
        })
    }
}
