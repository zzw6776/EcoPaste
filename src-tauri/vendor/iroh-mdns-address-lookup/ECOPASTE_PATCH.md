This is `iroh-mdns-address-lookup` 0.5.0 from crates.io with EcoPaste's
multi-interface mDNS extension.

The upstream builder only starts `swarm-discovery` on the operating system's
default multicast interface. EcoPaste forwards Iroh's current local IPv4
addresses instead, and updates the interface set when Iroh reports a network
change. This keeps LAN discovery working when a VPN or virtual adapter owns the
default route.

Remove this vendored crate when upstream exposes equivalent initial and dynamic
IPv4 multicast-interface configuration.
