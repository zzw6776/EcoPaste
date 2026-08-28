# EcoPaste patch

This is `swarm-discovery` 0.6.3 from crates.io with one lifecycle fix in
`src/updater.rs`.

The upstream GC timer used non-blocking mailbox delivery. When delivery failed,
it recursively scheduled another attempt after 10 ms. That treated a full
mailbox and a permanently closed mailbox identically, leaving an endless retry
loop after mDNS shutdown.

The patched timer uses `ActoRef::send_wait` instead. It waits when the mailbox is
full and finishes when the mailbox is closed. Remove this vendored crate and the
root `[patch.crates-io]` entry after an upstream release contains the same fix.
