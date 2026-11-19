use anyhow::{Context, Result};
use ecopaste_sync_protocol::transfer::{self as multipart, PartPeer};
use ecopaste_sync_protocol::{
    ALPN, DeviceAnnouncement, ErrorCode, RemovedDevice, Request, Response, SyncItemWrite,
    read_frame, write_frame,
};
use ecopaste_sync_server::{repository::Repository, service::HubService};
use iroh::{
    Endpoint, RelayMode,
    endpoint::{Connection, presets},
    protocol::Router,
};

#[tokio::test]
async fn per_item_snapshots_cas_blobs_and_watch_round_trip() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let repository = Repository::open(&directory.path().join("hub.sqlite3")).await?;
    let service = HubService::new(
        repository.clone(),
        directory.path().join("blobs"),
        10 * 1024 * 1024,
        ecopaste_sync_protocol::QuotaPolicy::default(),
        90,
        100_000,
    );
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    let server_address = server.addr();
    let router = Router::builder(server).accept(ALPN, service).spawn();
    let client = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    let client_endpoint_id = client.id().to_string();
    let connection = client.connect(server_address, ALPN).await?;

    assert!(matches!(
        call(&connection, Request::CloudHealth).await?,
        Response::CloudHealth {
            protocol_version: ecopaste_sync_protocol::PROTOCOL_VERSION,
            ..
        }
    ));

    let group_id = "group_123456".to_owned();
    let access_token = vec![9_u8; 32];
    assert_eq!(
        call(
            &connection,
            Request::CreateGroup {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
            },
        )
        .await?,
        Response::GroupCreated
    );

    // LAN-only RPC must be rejected by Hub.
    let Response::Error { code, message } = call(
        &connection,
        Request::SyncItems {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            device: device("lan_device", "LAN", &client_endpoint_id),
            after_seq: 0,
            before_seq: None,
            items: Vec::new(),
            limit: 100,
        },
    )
    .await?
    else {
        anyhow::bail!("expected LAN protocol rejection");
    };
    assert_eq!(code, ErrorCode::InvalidRequest);
    assert!(message.contains("current cloud protocol"));

    let source_icon = b"encrypted-source-icon";
    let source_icon_id = blake3::hash(source_icon).to_hex().to_string();
    let item_1 = SyncItemWrite {
        item_key: "a".repeat(64),
        expected_hub_rev: 0,
        local_seq: 1,
        origin_device_id: "device_123456".into(),
        updated_at_ms: 100,
        deleted: false,
        protected: true,
        nonce: vec![1; 24],
        ciphertext: vec![2; 128],
        blob_ids: Vec::new(),
        optional_blob_ids: vec![source_icon_id.clone()],
    };
    let Response::CloudSynced {
        accepted,
        conflicts,
        items,
        latest_seq,
        ..
    } = call(
        &connection,
        cloud_sync_request(
            &group_id,
            &access_token,
            device("device_123456", "Mac", &client_endpoint_id),
            0,
            vec![item_1.clone()],
        ),
    )
    .await?
    else {
        anyhow::bail!("expected sync response");
    };
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].hub_rev, 1);
    assert_eq!(accepted[0].seq, 1);
    assert!(conflicts.is_empty());
    // Caller's own freshly written item is not echoed back in `items`.
    assert!(items.is_empty());
    assert_eq!(latest_seq, 1);

    // Concurrent stale write triggers CAS conflict without failing the batch.
    let item_2 = SyncItemWrite {
        item_key: "b".repeat(64),
        expected_hub_rev: 0,
        local_seq: 2,
        origin_device_id: "device_123456".into(),
        updated_at_ms: 200,
        deleted: false,
        protected: false,
        nonce: vec![3; 24],
        ciphertext: vec![4; 128],
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
    };
    let removed_device = RemovedDevice {
        device_id: "device_removed".into(),
        endpoint_id: "endpoint_removed".into(),
        removed_at_ms: 300,
        restored_at_ms: None,
    };
    let Response::CloudSynced {
        accepted,
        conflicts,
        removed_devices,
        latest_seq,
        ..
    } = call(
        &connection,
        Request::CloudSyncItems {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            device: device("device_123456", "Mac", &client_endpoint_id),
            after_seq: 1,
            before_seq: None,
            items: vec![item_1.clone(), item_2.clone()],
            removed_devices: vec![removed_device.clone()],
            limit: 100,
        },
    )
    .await?
    else {
        anyhow::bail!("expected combined sync response");
    };
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].item_key, item_2.item_key);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].item_key, item_1.item_key);
    assert_eq!(conflicts[0].hub_rev, 1);
    assert_eq!(removed_devices, vec![removed_device]);
    assert_eq!(latest_seq, 2);

    // Second device pulls newest-first window.
    let Response::CloudSynced { items, peers, .. } = call(
        &connection,
        cloud_sync_request(
            &group_id,
            &access_token,
            device("device_654321", "Windows", &client_endpoint_id),
            0,
            Vec::new(),
        ),
    )
    .await?
    else {
        anyhow::bail!("expected second sync response");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].item_key, item_2.item_key);
    assert_eq!(items[1].item_key, item_1.item_key);
    assert_eq!(peers[0].device_id, "device_123456");

    // Unauthorized access token fails.
    let unauthorized = cloud_sync_request(
        &group_id,
        &[7_u8; 32],
        device("device_987654", "Android", &client_endpoint_id),
        0,
        Vec::new(),
    );
    assert!(matches!(
        call(&connection, unauthorized).await?,
        Response::Error {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));

    // Blob upload + streaming upload + commit check.
    let blob = b"encrypted-file-payload";
    let blob_id = blake3::hash(blob).to_hex().to_string();
    upload_blob(&connection, &group_id, &access_token, &blob_id, blob).await?;
    assert_eq!(
        download_blob(&connection, &group_id, &access_token, &blob_id).await?,
        blob
    );

    let streamed_blob = b"streamed-encrypted-file-payload";
    let streamed_blob_id = "c".repeat(64);
    upload_stream_blob(
        &connection,
        &group_id,
        &access_token,
        &streamed_blob_id,
        streamed_blob,
    )
    .await?;
    assert_eq!(
        download_blob(&connection, &group_id, &access_token, &streamed_blob_id).await?,
        streamed_blob
    );

    let uncommitted_blob_id = "d".repeat(64);
    let response = upload_invalid_stream_blob(
        &connection,
        &group_id,
        &access_token,
        &uncommitted_blob_id,
        streamed_blob,
    )
    .await?;
    assert!(matches!(response, Response::Error { .. }));
    assert!(matches!(
        call(
            &connection,
            Request::GetBlob {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                blob_id: uncommitted_blob_id,
                transfer_id: next_transfer_id(),
                file_content: true,
            },
        )
        .await?,
        Response::Error {
            code: ErrorCode::NotFound,
            ..
        }
    ));

    // WatchCloud wakes up when referenced source icon blob is uploaded (bumping item seq).
    let (mut watch_send, mut watch_recv) = connection.open_bi().await?;
    write_frame(
        &mut watch_send,
        &Request::WatchCloud {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            after_seq: 2,
            after_removed_at_ms: 300,
        },
    )
    .await?;
    watch_send.finish()?;
    assert!(matches!(
        read_frame::<_, Response>(&mut watch_recv).await?,
        Response::CloudChanged {
            latest_seq: 2,
            latest_removed_at_ms: 300,
            ..
        }
    ));

    let Response::Devices {
        devices,
        hub_status,
    } = call(
        &connection,
        Request::ListDevices {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            device: device("device_654321", "Windows", &client_endpoint_id),
        },
    )
    .await?
    else {
        anyhow::bail!("expected cloud device directory");
    };
    assert_eq!(devices.len(), 2);
    assert!(devices.iter().all(|device| device.online));
    let hub_status = hub_status.context("expected Hub status")?;
    assert_eq!(hub_status.event_count, 2);
    assert_eq!(hub_status.max_bytes, 10 * 1024 * 1024);

    upload_source_icon(
        &connection,
        &group_id,
        &access_token,
        &source_icon_id,
        source_icon,
    )
    .await?;
    let Response::CloudChanged {
        latest_seq,
        latest_removed_at_ms,
        ..
    } = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_frame::<_, Response>(&mut watch_recv),
    )
    .await
    .context("watch did not react to a newly available source icon")??
    else {
        anyhow::bail!("expected cloud change after source icon upload");
    };
    assert_eq!(latest_seq, 3);
    assert_eq!(latest_removed_at_ms, 300);

    // ListItems pages newest non-deleted items only.
    let Response::ItemsPage {
        items,
        next_before_seq,
        total,
    } = call(
        &connection,
        Request::ListItems {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            before_seq: None,
            limit: 1,
        },
    )
    .await?
    else {
        anyhow::bail!("expected items page");
    };
    assert_eq!(total, 2);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].item_key, item_1.item_key);
    assert_eq!(items[0].available_blob_ids, vec![source_icon_id]);
    assert_eq!(next_before_seq, Some(3));

    // Self-removal rejects sync and returns merged removals.
    let Response::CloudSynced {
        accepted,
        items,
        removed_devices,
        ..
    } = call(
        &connection,
        Request::CloudSyncItems {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            device: device("device_123456", "Mac", &client_endpoint_id),
            after_seq: 3,
            before_seq: None,
            items: Vec::new(),
            removed_devices: vec![RemovedDevice {
                device_id: "device_123456".into(),
                endpoint_id: client_endpoint_id.clone(),
                removed_at_ms: 500,
                restored_at_ms: None,
            }],
            limit: 100,
        },
    )
    .await?
    else {
        anyhow::bail!("expected removed-device sync response");
    };
    assert!(accepted.is_empty());
    assert!(items.is_empty());
    assert!(removed_devices.iter().any(|device| {
        device.device_id == "device_123456" && device.endpoint_id == client_endpoint_id
    }));

    assert!(matches!(
        call(
            &connection,
            Request::ListItems {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                before_seq: None,
                limit: 10,
            },
        )
        .await?,
        Response::Error { message, .. } if message.contains("device was removed")
    ));

    // Protected item_1 survives aggressive cleanup while unprotected item_2 is evicted.
    let cleanup = repository
        .cleanup_group(
            &group_id,
            ecopaste_sync_server::repository::CleanupPolicy {
                retention_cutoff_ms: None,
                max_items: None,
                target_bytes: Some(0),
                orphan_cutoff_ms: 0,
                batch_size: 100,
            },
        )
        .await?;
    assert_eq!(cleanup.deleted_items, 1);
    assert_eq!(repository.group_usage(&group_id).await?.item_count, 1);

    connection.close(0_u32.into(), b"test complete");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

fn device(device_id: &str, name: &str, endpoint_id: &str) -> DeviceAnnouncement {
    DeviceAnnouncement {
        device_id: device_id.into(),
        device_name: name.into(),
        platform: "test".into(),
        endpoint_id: endpoint_id.into(),
        direct_addresses: vec!["127.0.0.1:44820".into()],
        relay_urls: Vec::new(),
        app_version: "1.1.231".into(),
    }
}

fn cloud_sync_request(
    group_id: &str,
    access_token: &[u8],
    device: DeviceAnnouncement,
    after_seq: u64,
    items: Vec<SyncItemWrite>,
) -> Request {
    Request::CloudSyncItems {
        group_id: group_id.into(),
        access_token: access_token.to_vec(),
        device,
        after_seq,
        before_seq: None,
        items,
        removed_devices: Vec::new(),
        limit: 100,
    }
}

async fn call(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await.context("read server response")
}

fn next_transfer_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

async fn upload_blob(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<()> {
    let transfer_id = next_transfer_id();
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutManagedBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
            transfer_id,
            file_content: true,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    multipart::send(
        &mut &blob[..],
        PartPeer::client(connection.clone(), transfer_id),
        blob.len() as u64,
        |_| {},
    )
    .await?;
    send.finish()?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobStored
    );
    Ok(())
}

async fn upload_stream_blob(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<()> {
    let transfer_id = next_transfer_id();
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutManagedStreamBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
            transfer_id,
            file_content: true,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    multipart::send(
        &mut &blob[..],
        PartPeer::client(connection.clone(), transfer_id),
        blob.len() as u64,
        |_| {},
    )
    .await?;
    write_frame(
        &mut send,
        &Request::CommitStreamBlob {
            blob_id: blob_id.into(),
        },
    )
    .await?;
    send.finish()?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobStored
    );
    Ok(())
}

async fn upload_invalid_stream_blob(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<Response> {
    let transfer_id = next_transfer_id();
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutManagedStreamBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
            transfer_id,
            file_content: true,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    multipart::send(
        &mut &blob[..],
        PartPeer::client(connection.clone(), transfer_id),
        blob.len() as u64,
        |_| {},
    )
    .await?;
    write_frame(
        &mut send,
        &Request::CommitStreamBlob {
            blob_id: "e".repeat(64),
        },
    )
    .await?;
    send.finish()?;
    read_frame(&mut recv)
        .await
        .context("read rejected stream upload")
}

async fn upload_source_icon(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<()> {
    let transfer_id = next_transfer_id();
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutManagedSourceIcon {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
            transfer_id,
            file_content: true,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    multipart::send(
        &mut &blob[..],
        PartPeer::client(connection.clone(), transfer_id),
        blob.len() as u64,
        |_| {},
    )
    .await?;
    send.finish()?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobStored
    );
    Ok(())
}

async fn download_blob(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
) -> Result<Vec<u8>> {
    let transfer_id = next_transfer_id();
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            transfer_id,
            file_content: true,
        },
    )
    .await?;
    send.finish()?;
    let Response::BlobReady { size } = read_frame(&mut recv).await? else {
        anyhow::bail!("expected blob header");
    };
    let mut bytes = Vec::new();
    multipart::receive(
        &mut bytes,
        PartPeer::client(connection.clone(), transfer_id),
        size,
        |_| {},
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobStored
    );
    Ok(bytes)
}
