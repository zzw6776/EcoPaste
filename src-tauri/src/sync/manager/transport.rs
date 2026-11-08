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
            )? {
                return Ok(());
            }
        }
        bail!("连接已关闭")
    })
    .await
    .context("连接没有可用路径")?
}

/// Accepts Iroh's selected-path-less snapshot only when every open path is LAN-private.
pub(super) fn lan_path_snapshot_is_usable(
    selected: Option<TransportAddr>,
    paths: impl IntoIterator<Item = TransportAddr>,
) -> Result<bool> {
    if let Some(selected) = selected {
        ensure_lan_transport_addr(&selected)?;
        return Ok(true);
    }

    let mut has_path = false;
    for path in paths {
        has_path = true;
        if ensure_lan_transport_addr(&path).is_err() {
            return Ok(false);
        }
    }
    Ok(has_path)
}

/// 校验 Iroh 的结构化传输地址，避免把 `ip:地址` 展示文本误当作 SocketAddr 解析。
pub(super) fn ensure_lan_transport_addr(transport: &TransportAddr) -> Result<()> {
    let TransportAddr::Ip(address) = transport else {
        bail!("局域网同步禁止使用 Relay");
    };
    if !is_lan_ip(address.ip()) {
        bail!("局域网同步禁止使用公网地址");
    }

    Ok(())
}
