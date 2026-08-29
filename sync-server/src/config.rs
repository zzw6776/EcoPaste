use std::{net::SocketAddrV4, path::PathBuf};

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(version, about = "EcoPaste encrypted sync hub powered by Iroh")]
pub struct Config {
    /// Directory containing SQLite, endpoint identity, and encrypted blobs.
    #[arg(long, env = "ECOPASTE_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// UDP address used by Iroh QUIC.
    #[arg(long, env = "ECOPASTE_BIND", default_value = "0.0.0.0:44820")]
    pub bind: SocketAddrV4,

    /// Maximum encrypted blob size accepted by the hub.
    #[arg(
        long,
        env = "ECOPASTE_MAX_BLOB_BYTES",
        default_value_t = 2 * 1024 * 1024 * 1024_u64
    )]
    pub max_blob_bytes: u64,

    /// Disable the public Iroh relay network. Direct LAN/public-IP access remains available.
    #[arg(long, env = "ECOPASTE_NO_RELAY", default_value_t = true)]
    pub no_relay: bool,
}
