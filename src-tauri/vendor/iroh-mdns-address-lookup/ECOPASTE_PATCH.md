This is `iroh-mdns-address-lookup` 0.5.0 from crates.io with EcoPaste's
multi-interface mDNS extension.

The upstream builder only starts `swarm-discovery` on the operating system's
default multicast interface. EcoPaste forwards Iroh's current local IPv4
addresses instead, and updates the interface set when Iroh reports a network
change. This keeps LAN discovery working when a VPN or virtual adapter owns the
default route.

For IPv4, the vendored `swarm-discovery` receiver also accepts a service address
only when it matches the UDP packet's source IP. It merges paths observed on
different interfaces and expires each path independently. A response sent over
Wi-Fi can otherwise leak unrelated VPN or virtual-adapter addresses into the
peer's LAN routes, while replacing whole peer snapshots would discard valid
multi-interface paths. Cross-family records retain the upstream behavior; the
EcoPaste address filter publishes IPv4 routes only.

Remove this vendored crate when upstream exposes equivalent initial and dynamic
IPv4 multicast-interface configuration and source-interface route filtering.
