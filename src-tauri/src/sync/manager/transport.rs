use super::*;

pub(super) fn connection_path(connection: &Connection) -> (Option<String>, Option<&'static str>) {
    let paths = connection.paths();
    let path = paths
        .iter()
        .find(|path| path.is_selected())
        .or_else(|| paths.iter().next());
    let Some(path) = path else {
        return (None, Some("unknown"));
    };
    let transport = if path.is_ip() {
        "direct"
    } else if path.is_relay() {
        "relay"
    } else {
        "unknown"
    };
    (Some(path.remote_addr().to_string()), Some(transport))
}

pub(super) async fn ensure_lan_connection(
    connection: &Connection,
    selection_timeout: Duration,
) -> Result<()> {
    ensure_lan_connection_with_addresses(connection, selection_timeout, None).await
}

/// Requires an outgoing LAN connection to stay on one of the current discovery routes.
pub(super) async fn ensure_discovered_lan_connection(
    connection: &Connection,
    selection_timeout: Duration,
    discovered_ips: &HashSet<IpAddr>,
) -> Result<()> {
    ensure_lan_connection_with_addresses(connection, selection_timeout, Some(discovered_ips)).await
}

async fn ensure_lan_connection_with_addresses(
    connection: &Connection,
    selection_timeout: Duration,
    discovered_ips: Option<&HashSet<IpAddr>>,
) -> Result<()> {
    let mut paths = connection.paths_stream();
    tokio::time::timeout(selection_timeout, async {
        while let Some(paths) = paths.next().await {
            let selected = paths
                .iter()
                .find(|path| path.is_selected())
                .map(|path| path.remote_addr().clone());
            if lan_path_snapshot_is_usable(
                selected,
                paths.iter().map(|path| path.remote_addr().clone()),
                discovered_ips,
            )? {
                return Ok(());
            }
        }
        bail!("连接已关闭")
    })
    .await
    .context("连接没有可用路径")?
}

/// Accepts an unselected snapshot only when every open path satisfies the active LAN policy.
pub(super) fn lan_path_snapshot_is_usable(
    selected: Option<TransportAddr>,
    paths: impl IntoIterator<Item = TransportAddr>,
    discovered_ips: Option<&HashSet<IpAddr>>,
) -> Result<bool> {
    if let Some(selected) = selected {
        ensure_lan_transport_addr(&selected, discovered_ips)?;
        return Ok(true);
    }

    let mut has_path = false;
    for path in paths {
        has_path = true;
        if ensure_lan_transport_addr(&path, discovered_ips).is_err() {
            return Ok(false);
        }
    }
    Ok(has_path)
}

/// 校验 Iroh 的结构化传输地址，避免把 `ip:地址` 展示文本误当作 SocketAddr 解析。
pub(super) fn ensure_lan_transport_addr(
    transport: &TransportAddr,
    discovered_ips: Option<&HashSet<IpAddr>>,
) -> Result<()> {
    let TransportAddr::Ip(address) = transport else {
        bail!("局域网同步禁止使用 Relay");
    };
    if !is_lan_ip(address.ip()) {
        bail!("局域网同步禁止使用公网地址");
    }
    if discovered_ips.is_some_and(|ips| !ips.contains(&address.ip())) {
        bail!("局域网连接未使用当前发现地址");
    }

    Ok(())
}

/// Checks a cached connection without waiting, so stale VPN paths are replaced before transfer.
pub(super) fn lan_connection_matches_discovery(
    connection: &Connection,
    discovered: &EndpointAddr,
) -> bool {
    if connection.remote_id() != discovered.id {
        return false;
    }

    let discovered_ips = discovered
        .ip_addrs()
        .map(SocketAddr::ip)
        .collect::<HashSet<_>>();
    if discovered_ips.is_empty() {
        return false;
    }
    let paths = connection.paths();
    let selected = paths
        .iter()
        .find(|path| path.is_selected())
        .map(|path| path.remote_addr().clone());
    lan_path_snapshot_is_usable(
        selected,
        paths.iter().map(|path| path.remote_addr().clone()),
        Some(&discovered_ips),
    )
    .unwrap_or(false)
}
