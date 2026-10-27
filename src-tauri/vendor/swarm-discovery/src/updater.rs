use crate::{Callback, Peer, TxtData};
use acto::{AcTokioRuntime, ActoCell, ActoInput, ActoRef};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use tokio::time::sleep;

pub enum Input {
    Peers(BTreeMap<String, Peer>),
    GC,
    SizeSubscription(ActoRef<usize>),
}

struct PeerObservations {
    paths: BTreeMap<Option<IpAddr>, ObservedPath>,
    last_seen: Instant,
    txt: TxtData,
}

struct ObservedPath {
    addrs: Vec<(IpAddr, u16)>,
    last_seen: Instant,
}

impl PeerObservations {
    fn new(peer: Peer) -> Self {
        let last_seen = peer.last_seen;
        Self {
            paths: [(
                peer.source,
                ObservedPath {
                    addrs: peer.addrs,
                    last_seen,
                },
            )]
            .into_iter()
            .collect(),
            last_seen,
            txt: peer.txt,
        }
    }

    /// Replaces one source path while retaining routes observed through other interfaces.
    fn merge(&mut self, peer: Peer) -> Peer {
        self.paths.insert(
            peer.source,
            ObservedPath {
                addrs: peer.addrs,
                last_seen: peer.last_seen,
            },
        );
        if peer.last_seen >= self.last_seen {
            self.last_seen = peer.last_seen;
            self.txt = peer.txt;
        }
        self.snapshot()
    }

    /// Expires each observed route without removing other live paths to the same peer.
    fn expire_routes(&mut self, now: Instant, grace_period: Duration) -> bool {
        let previous_len = self.paths.len();
        self.paths.retain(|_, path| {
            now.checked_duration_since(path.last_seen).unwrap_or_default() < grace_period
        });
        previous_len != self.paths.len()
    }

    fn snapshot(&self) -> Peer {
        let mut addrs = self
            .paths
            .values()
            .flat_map(|path| path.addrs.iter().copied())
            .collect::<Vec<_>>();
        addrs.sort_unstable();
        addrs.dedup();
        Peer {
            addrs,
            last_seen: self.last_seen,
            txt: self.txt.clone(),
            source: None,
        }
    }
}

fn gc(me: ActoRef<Input>, interval: Duration) {
    tokio::spawn(send_gc_after(me, interval));
}

/// Waits for mailbox capacity so a full queue does not disable GC, then exits if it is closed.
async fn send_gc_after(me: ActoRef<Input>, interval: Duration) {
    sleep(interval).await;
    let _ = me.send_wait(Input::GC).await;
}

pub async fn updater(
    mut ctx: ActoCell<Input, AcTokioRuntime>,
    tau: Duration,
    phi: f32,
    mut callback: Callback,
) {
    let gc_interval = tau * 12345 / 9999;
    gc(ctx.me(), gc_interval);

    let mut peers = BTreeMap::<String, PeerObservations>::new();
    #[allow(clippy::mutable_key_type)]
    let mut subscribers = BTreeSet::<ActoRef<usize>>::new();
    while let ActoInput::Message(msg) = ctx.recv().await {
        match msg {
            Input::Peers(msg) => {
                for (id, peer) in msg {
                    let (peer, inserted) = match peers.get_mut(&id) {
                        Some(observations) => (observations.merge(peer), false),
                        None => {
                            let observations = PeerObservations::new(peer);
                            let peer = observations.snapshot();
                            peers.insert(id.clone(), observations);
                            (peer, true)
                        }
                    };
                    callback(&id, &peer);
                    if inserted {
                        for sub in &subscribers {
                            sub.send(peers.len());
                        }
                    }
                }
            }
            Input::GC => {
                gc(ctx.me(), gc_interval);
                if peers.is_empty() {
                    continue;
                }
                let now = Instant::now();
                // we send min(swarmsize, ceil(tau * phi)) per cadence
                let expected_frequency =
                    (tau.as_secs_f32() * phi).ceil().min(peers.len() as f32) / tau.as_secs_f32();
                let frequency_per_peer = expected_frequency / peers.len() as f32;
                // take per-peer cadence times three to account for jitter
                let per_peer_grace_period = Duration::from_secs_f32(3.0 / frequency_per_peer);
                peers.retain(|peer_id, observations| {
                    let changed = observations.expire_routes(now, per_peer_grace_period);
                    let keep = !observations.paths.is_empty();
                    if !keep {
                        callback(
                            peer_id,
                            &Peer {
                                last_seen: observations.last_seen,
                                addrs: vec![],
                                txt: Default::default(),
                                source: None,
                            },
                        );
                    } else if changed {
                        callback(peer_id, &observations.snapshot());
                    }
                    keep
                });
                for sub in &subscribers {
                    sub.send(peers.len());
                }
            }
            Input::SizeSubscription(sub) => {
                subscribers.insert(sub);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(source: &str, address: &str, port: u16, last_seen: Instant) -> Peer {
        Peer {
            addrs: vec![(address.parse().unwrap(), port)],
            last_seen,
            txt: [("device".to_owned(), Some("EcoPaste".to_owned()))]
                .into_iter()
                .collect(),
            source: Some(source.parse().unwrap()),
        }
    }

    #[test]
    fn peer_routes_are_merged_and_expire_independently() {
        let now = Instant::now();
        let stale = now.checked_sub(Duration::from_secs(20)).unwrap();
        let mut observations =
            PeerObservations::new(peer("192.168.50.187", "192.168.50.187", 38466, stale));

        let merged = observations.merge(peer("10.0.0.25", "10.0.0.25", 38466, now));

        assert_eq!(
            merged.addrs(),
            &[
                ("10.0.0.25".parse().unwrap(), 38466),
                ("192.168.50.187".parse().unwrap(), 38466),
            ]
        );
        assert!(observations.expire_routes(now, Duration::from_secs(10)));
        assert_eq!(
            observations.snapshot().addrs(),
            &[("10.0.0.25".parse().unwrap(), 38466)]
        );
    }

    #[test]
    fn a_new_snapshot_replaces_routes_from_the_same_source() {
        let now = Instant::now();
        let source = "127.0.0.1";
        let mut observations =
            PeerObservations::new(peer(source, "127.0.0.1", 8000, now));

        let updated = observations.merge(peer(source, "::1", 9000, now));

        assert_eq!(updated.addrs(), &[("::1".parse().unwrap(), 9000)]);
    }

    #[tokio::test]
    async fn gc_task_finishes_after_mailbox_closes() {
        tokio::time::timeout(
            Duration::from_millis(100),
            send_gc_after(ActoRef::blackhole(), Duration::ZERO),
        )
        .await
        .expect("GC task should stop after its mailbox closes");
    }
}
