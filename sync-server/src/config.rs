use std::{net::SocketAddrV4, path::PathBuf};

use clap::Parser;
use ecopaste_sync_protocol::{DEFAULT_QUOTA_TARGET_PERCENT, DEFAULT_QUOTA_TRIGGER_PERCENT};

#[derive(Debug, Clone, Parser)]
#[command(version, about = "EcoPaste encrypted sync hub powered by Iroh")]
pub struct Config {
    /// Directory containing SQLite, endpoint identity, and encrypted blobs.
    #[arg(long, env = "ECOPASTE_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// UDP address used by Iroh QUIC.
    #[arg(long, env = "ECOPASTE_BIND", default_value = "0.0.0.0:44820")]
    pub bind: SocketAddrV4,

    /// Soft encrypted payload budget retained per sync group. Zero disables policy cleanup.
    #[arg(
        long,
        env = "ECOPASTE_MAX_GROUP_BYTES",
        default_value_t = 10 * 1024 * 1024 * 1024_u64
    )]
    pub max_group_bytes: u64,

    /// Group usage percentage that requests quota cleanup.
    #[arg(
        long,
        env = "ECOPASTE_GROUP_QUOTA_TRIGGER_PERCENT",
        default_value_t = DEFAULT_QUOTA_TRIGGER_PERCENT
    )]
    pub group_quota_trigger_percent: u32,

    /// Group usage percentage retained after quota cleanup.
    #[arg(
        long,
        env = "ECOPASTE_GROUP_QUOTA_TARGET_PERCENT",
        default_value_t = DEFAULT_QUOTA_TARGET_PERCENT
    )]
    pub group_quota_target_percent: u32,

    /// Maximum age of encrypted events retained by the Hub. Zero disables age pruning.
    #[arg(long, env = "ECOPASTE_EVENT_RETENTION_DAYS", default_value_t = 0)]
    pub event_retention_days: u32,

    /// Maximum raw encrypted events retained per group. Zero disables count pruning.
    #[arg(long, env = "ECOPASTE_MAX_EVENTS_PER_GROUP", default_value_t = 0)]
    pub max_events_per_group: u64,

    /// Disable the public Iroh relay network. Direct LAN/public-IP access remains available.
    #[arg(long, env = "ECOPASTE_NO_RELAY", default_value_t = true)]
    pub no_relay: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_defaults_keep_only_the_space_limit() {
        let config = Config::parse_from(["ecopaste-sync-server"]);
        assert_eq!(config.event_retention_days, 0);
        assert_eq!(config.max_events_per_group, 0);
        assert_eq!(config.max_group_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(
            config.group_quota_trigger_percent,
            DEFAULT_QUOTA_TRIGGER_PERCENT
        );
        assert_eq!(
            config.group_quota_target_percent,
            DEFAULT_QUOTA_TARGET_PERCENT
        );
    }
}
