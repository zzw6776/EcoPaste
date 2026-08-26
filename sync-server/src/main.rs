use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use data_encoding::HEXLOWER;
use ecopaste_sync_protocol::ALPN;
use ecopaste_sync_server::{config::Config, repository::Repository, service::HubService};
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets, protocol::Router};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = Config::parse();
    tokio::fs::create_dir_all(&config.data_dir)
        .await
        .with_context(|| format!("create data directory {}", config.data_dir.display()))?;
    let blob_root = config.data_dir.join("blobs");
    tokio::fs::create_dir_all(&blob_root)
        .await
        .with_context(|| format!("create blob directory {}", blob_root.display()))?;
    let secret_key = load_or_create_secret_key(&config.data_dir.join("iroh-secret.key")).await?;
    let repository = Repository::open(&config.data_dir.join("hub.sqlite3")).await?;
    let service = HubService::new(repository, blob_root, config.max_blob_bytes);

    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind_addr(config.bind)
        .context("configure Iroh UDP bind address")?;
    if config.no_relay {
        builder = builder.relay_mode(RelayMode::Disabled);
    }
    let endpoint = builder.bind().await.context("bind Iroh endpoint")?;
    if !config.no_relay {
        let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;
    }
    let address = endpoint.addr();
    info!(endpoint_id = %endpoint.id(), bind = %config.bind, "EcoPaste sync hub ready");
    println!("ECOPASTE_SERVER_ENDPOINT_ID={}", endpoint.id());
    for direct in address.ip_addrs() {
        println!("ECOPASTE_SERVER_DIRECT_ADDRESS={direct}");
    }
    for relay in address.relay_urls() {
        println!("ECOPASTE_SERVER_RELAY_URL={relay}");
    }

    let router = Router::builder(endpoint).accept(ALPN, service).spawn();
    tokio::signal::ctrl_c()
        .await
        .context("wait for shutdown signal")?;
    router.shutdown().await.context("shutdown Iroh router")?;
    Ok(())
}

async fn load_or_create_secret_key(path: &Path) -> Result<SecretKey> {
    match tokio::fs::read_to_string(path).await {
        Ok(value) => SecretKey::from_str(value.trim()).context("parse persisted Iroh secret key"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            let encoded = HEXLOWER.encode(&key.to_bytes());
            tokio::fs::write(path, format!("{encoded}\n"))
                .await
                .context("persist Iroh secret key")?;
            restrict_secret_permissions(path).await?;
            Ok(key)
        }
        Err(error) => Err(error).context("read persisted Iroh secret key"),
    }
}

#[cfg(unix)]
async fn restrict_secret_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .context("restrict Iroh secret key permissions")
}

#[cfg(not(unix))]
async fn restrict_secret_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
