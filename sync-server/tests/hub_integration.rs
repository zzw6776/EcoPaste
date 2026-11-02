use anyhow::{Context, Result};
use ecopaste_sync_protocol::{
    ALPN, DeviceAnnouncement, EncryptedEvent, ErrorCode, RemovedDevice, Request, Response,
    RetentionMapping, SyncEventKind, read_frame, write_frame,
};
use ecopaste_sync_server::{repository::Repository, service::HubService};
use iroh::{
    Endpoint, RelayMode,
    endpoint::{Connection, presets},
    protocol::Router,
};

#[tokio::test]
async fn encrypted_events_routes_and_blobs_round_trip() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let repository = Repository::open(&directory.path().join("hub.sqlite3")).await?;
    let service = HubService::new(
        repository.clone(),
        directory.path().join("blobs"),
        1024 * 1024,
        10 * 1024 * 1024,
        90,
        100_000,
    );
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    assert!(
        server
            .bound_sockets()
            .iter()
            .all(|address| address.is_ipv4())
    );
    let server_address = server.addr();
    let router = Router::builder(server).accept(ALPN, service).spawn();
    let client = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    assert!(
        client
            .bound_sockets()
            .iter()
            .all(|address| address.is_ipv4())
    );
    let client_endpoint_id = client.id().to_string();
    let connection = client.connect(server_address, ALPN).await?;

    assert!(matches!(
        call(&connection, Request::Health).await?,
        Response::Health {
            protocol_version: ecopaste_sync_protocol::PROTOCOL_VERSION,
            ..
        }
    ));
    assert!(matches!(
        call(&connection, Request::HealthV2).await?,
        Response::HealthV2 {
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

    let Response::Error { code, message } = call(
        &connection,
        Request::Sync {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            device: device("legacy_device", "Legacy", &client_endpoint_id),
            after_cursor: 0,
            events: Vec::new(),
            limit: 100,
            byte_limited: Some(true),
            events_complete: None,
            bounded_hub: None,
        },
    )
    .await?
    else {
        anyhow::bail!("expected legacy client rejection");
    };
    assert_eq!(code, ErrorCode::InvalidRequest);
    assert!(message.contains("版本过低"));

    let source_icon = b"encrypted-source-icon";
    let source_icon_id = blake3::hash(source_icon).to_hex().to_string();
    let event = EncryptedEvent {
        event_id: "event_123456".into(),
        origin_device_id: "device_123456".into(),
        origin_sequence: 1,
        created_at_ms: 100,
        nonce: vec![1; 24],
        ciphertext: vec![2; 128],
        kind: SyncEventKind::Content,
        blob_ids: vec![source_icon_id.clone()],
        optional_blob_ids: vec![source_icon_id.clone()],
        retention_key: None,
    };
    let first = sync_request(
        &group_id,
        &access_token,
        device("device_123456", "Mac", &client_endpoint_id),
        0,
        vec![event.clone()],
    );
    let Response::Synced {
        accepted_event_ids,
        events,
        ..
    } = call(&connection, first).await?
    else {
        anyhow::bail!("expected sync response");
    };
    assert_eq!(accepted_event_ids, vec![event.event_id.clone()]);
    assert_eq!(events.len(), 1);

    assert_eq!(
        call(
            &connection,
            Request::Watch {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                after_cursor: 0,
            },
        )
        .await?,
        Response::Changed { latest_cursor: 1 }
    );

    let watch_connection = connection.clone();
    let watch_group_id = group_id.clone();
    let watch_access_token = access_token.clone();
    let watch = tokio::spawn(async move {
        call(
            &watch_connection,
            Request::Watch {
                group_id: watch_group_id,
                access_token: watch_access_token,
                after_cursor: 1,
            },
        )
        .await
    });
    tokio::task::yield_now().await;

    let next_event = EncryptedEvent {
        event_id: "event_654321".into(),
        origin_device_id: "device_123456".into(),
        origin_sequence: 2,
        created_at_ms: 200,
        nonce: vec![3; 24],
        ciphertext: vec![4; 128],
        kind: SyncEventKind::Content,
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        retention_key: None,
    };
    let removed_device = RemovedDevice {
        device_id: "device_removed".into(),
        endpoint_id: "endpoint_removed".into(),
        removed_at_ms: 300,
        restored_at_ms: None,
    };
    let notify_watch = Request::SyncV2 {
        group_id: group_id.clone(),
        access_token: access_token.clone(),
        device: device("device_123456", "Mac", &client_endpoint_id),
        after_cursor: 1,
        events: vec![next_event.clone()],
        removed_devices: vec![removed_device.clone()],
        limit: 100,
        byte_limited: None,
        events_complete: Some(true),
        bounded_hub: Some(true),
    };
    let Response::SyncedV2 {
        accepted_event_ids,
        removed_devices,
        ..
    } = call(&connection, notify_watch.clone()).await?
    else {
        anyhow::bail!("expected combined sync response");
    };
    assert_eq!(accepted_event_ids, vec![next_event.event_id.clone()]);
    assert_eq!(removed_devices, vec![removed_device]);
    let Response::SyncedV2 {
        accepted_event_ids, ..
    } = call(&connection, notify_watch).await?
    else {
        anyhow::bail!("expected duplicate combined sync response");
    };
    assert!(accepted_event_ids.is_empty());
    assert_eq!(
        call(
            &connection,
            Request::BootstrapPlan {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
            },
        )
        .await?,
        Response::BootstrapPlan {
            start_cursor: 0,
            latest_cursor: 2,
        }
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), watch)
            .await
            .context("watch did not react to a new event")???,
        Response::Changed { latest_cursor: 2 }
    );

    let second = sync_request(
        &group_id,
        &access_token,
        device("device_654321", "Windows", &client_endpoint_id),
        0,
        Vec::new(),
    );
    let Response::Synced { events, peers, .. } = call(&connection, second).await? else {
        anyhow::bail!("expected second sync response");
    };
    assert_eq!(events.len(), 2);
    let mut expected_event = event;
    expected_event.optional_blob_ids.clear();
    assert_eq!(events[0].event, expected_event);
    assert_eq!(events[1].event, next_event);
    assert_eq!(peers[0].device_id, "device_123456");

    let unauthorized = sync_request(
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

    let blob = b"encrypted-file-payload";
    let blob_id = blake3::hash(blob).to_hex().to_string();
    upload_blob(&connection, &group_id, &access_token, &blob_id, blob).await?;
    let downloaded = download_blob(&connection, &group_id, &access_token, &blob_id).await?;
    assert_eq!(downloaded, blob);

    let (mut asset_watch_send, mut asset_watch_recv) = connection.open_bi().await?;
    write_frame(
        &mut asset_watch_send,
        &Request::WatchGroupStreamV3 {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            after_cursor: 2,
            after_removed_at_ms: 300,
            watch_slot: 0,
        },
    )
    .await?;
    asset_watch_send.finish()?;
    assert!(matches!(
        read_frame::<_, Response>(&mut asset_watch_recv).await?,
        Response::GroupChangedV2 {
            latest_cursor: 2,
            latest_removed_at_ms: 300,
            ..
        }
    ));
    let (mut asset_backup_send, mut asset_backup_recv) = connection.open_bi().await?;
    write_frame(
        &mut asset_backup_send,
        &Request::WatchGroupStreamV3 {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
            after_cursor: 2,
            after_removed_at_ms: 300,
            watch_slot: 1,
        },
    )
    .await?;
    asset_backup_send.finish()?;
    assert!(matches!(
        read_frame::<_, Response>(&mut asset_backup_recv).await?,
        Response::GroupChangedV2 {
            latest_cursor: 2,
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
    assert!(devices.iter().all(|device| device.app_version == "1.1.154"));
    let hub_status = hub_status.context("expected bounded Hub status")?;
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
    assert_eq!(
        download_blob(&connection, &group_id, &access_token, &source_icon_id).await?,
        source_icon
    );
    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_frame::<_, Response>(&mut asset_watch_recv),
        )
        .await
        .context("watch did not react to a new source icon")??,
        Response::GroupChangedV2 {
            latest_cursor: 2,
            latest_removed_at_ms: 300,
            ..
        }
    ));
    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_frame::<_, Response>(&mut asset_backup_recv),
        )
        .await
        .context("backup watch did not react to a new source icon")??,
        Response::GroupChangedV2 {
            latest_cursor: 2,
            latest_removed_at_ms: 300,
            ..
        }
    ));

    let audit_group = "group_audit".to_owned();
    assert_eq!(
        call(
            &connection,
            Request::CreateGroup {
                group_id: audit_group.clone(),
                access_token: access_token.clone()
            }
        )
        .await?,
        Response::GroupCreated
    );
    let mut content = next_event.clone();
    content.retention_key = Some("a".repeat(64));
    let mut state = content.clone();
    state.event_id = "audit_state".into();
    state.origin_sequence += 1;
    state.kind = SyncEventKind::ItemState;
    call(
        &connection,
        sync_request(
            &audit_group,
            &access_token,
            device("device_123456", "Mac", &client_endpoint_id),
            0,
            vec![content, state],
        ),
    )
    .await?;
    for (include_states, expected) in [
        (None, vec![SyncEventKind::Content]),
        (
            Some(true),
            vec![SyncEventKind::ItemState, SyncEventKind::Content],
        ),
    ] {
        let mut before_cursor = None;
        let mut kinds = Vec::new();
        loop {
            let Response::EventsPage {
                events,
                next_before_cursor,
                total,
            } = call(
                &connection,
                Request::ListEvents {
                    group_id: audit_group.clone(),
                    access_token: access_token.clone(),
                    before_cursor,
                    limit: 1,
                    include_states,
                },
            )
            .await?
            else {
                anyhow::bail!("expected event page")
            };
            assert_eq!(total, expected.len() as u64);
            kinds.extend(events.into_iter().map(|stored| stored.event.kind));
            before_cursor = next_before_cursor;
            if before_cursor.is_none() {
                break;
            }
        }
        assert_eq!(kinds, expected);
    }

    let Response::RetentionPlan {
        epoch,
        events: legacy_events,
        has_more,
    } = call(
        &connection,
        Request::RetentionPlan {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
        },
    )
    .await?
    else {
        anyhow::bail!("expected retention plan")
    };
    assert!(!epoch.is_empty());
    assert!(!has_more);
    assert_eq!(legacy_events.len(), 2);
    let item_key = "a".repeat(64);
    assert_eq!(
        call(
            &connection,
            Request::SetRetention {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                device: device("device_123456", "Mac", &client_endpoint_id),
                generation: 10,
                protected_keys: Some(vec![item_key.clone()]),
                mappings: legacy_events
                    .iter()
                    .map(|stored| RetentionMapping {
                        event_id: stored.event.event_id.clone(),
                        item_key: item_key.clone(),
                        blob_ids: stored.event.blob_ids.clone(),
                    })
                    .collect(),
            }
        )
        .await?,
        Response::RetentionUpdated
    );
    assert!(matches!(call(&connection, Request::RetentionPlan {
        group_id: group_id.clone(), access_token: access_token.clone(),
    }).await?, Response::RetentionPlan { events, has_more: false, .. } if events.is_empty()));
    assert!(matches!(
        call(
            &connection,
            Request::SetRetention {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                device: device("device_123456", "Mac", "different_endpoint"),
                generation: 11,
                protected_keys: Some(Vec::new()),
                mappings: Vec::new(),
            }
        )
        .await?,
        Response::Error {
            code: ErrorCode::InvalidRequest,
            ..
        }
    ));

    let rejected_event = EncryptedEvent {
        event_id: "event_after_removal".into(),
        origin_device_id: "device_123456".into(),
        origin_sequence: 3,
        created_at_ms: 400,
        nonce: vec![5; 24],
        ciphertext: vec![6; 128],
        kind: SyncEventKind::Content,
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        retention_key: None,
    };
    let remove_self = Request::SyncV2 {
        group_id: group_id.clone(),
        access_token: access_token.clone(),
        device: device("device_123456", "Mac", &client_endpoint_id),
        after_cursor: 2,
        events: vec![rejected_event],
        removed_devices: vec![RemovedDevice {
            device_id: "device_123456".into(),
            endpoint_id: client_endpoint_id.clone(),
            removed_at_ms: 500,
            restored_at_ms: None,
        }],
        limit: 100,
        byte_limited: None,
        events_complete: Some(true),
        bounded_hub: Some(true),
    };
    let Response::SyncedV2 {
        accepted_event_ids,
        events,
        removed_devices,
        ..
    } = call(&connection, remove_self).await?
    else {
        anyhow::bail!("expected removed-device sync response");
    };
    assert!(accepted_event_ids.is_empty());
    assert!(events.is_empty());
    assert!(removed_devices.iter().any(|device| {
        device.device_id == "device_123456" && device.endpoint_id == client_endpoint_id
    }));

    assert_eq!(
        call(
            &connection,
            Request::SyncRemovedDevices {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                devices: Vec::new(),
            },
        )
        .await?,
        Response::RemovedDevices {
            devices: removed_devices,
        }
    );
    for request in [
        Request::BootstrapPlan {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
        },
        Request::RetentionPlan {
            group_id: group_id.clone(),
            access_token: access_token.clone(),
        },
    ] {
        assert!(matches!(
            call(&connection, request).await?,
            Response::Error { message, .. } if message.contains("device was removed")
        ));
    }

    repository
        .merge_removed_devices(
            &group_id,
            &[RemovedDevice {
                device_id: "device_123456".into(),
                endpoint_id: client_endpoint_id.clone(),
                removed_at_ms: 500,
                restored_at_ms: Some(600),
            }],
        )
        .await?;
    assert_eq!(repository.retention_epoch(&group_id).await?, epoch);
    assert_eq!(
        call(
            &connection,
            Request::SetRetention {
                group_id: group_id.clone(),
                access_token: access_token.clone(),
                device: device("device_123456", "Mac", &client_endpoint_id),
                generation: 11,
                protected_keys: Some(vec![item_key]),
                mappings: Vec::new(),
            },
        )
        .await?,
        Response::RetentionUpdated
    );
    let cleanup = repository
        .cleanup_group(
            &group_id,
            ecopaste_sync_server::repository::CleanupPolicy {
                retention_cutoff_ms: None,
                legacy_blob_cutoff_ms: None,
                max_events: None,
                target_bytes: Some(0),
                orphan_cutoff_ms: 0,
                batch_size: 100,
            },
        )
        .await?;
    assert_eq!(cleanup.deleted_events, 0);
    assert_eq!(repository.group_usage(&group_id).await?.event_count, 2);

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
        app_version: "1.1.154".into(),
    }
}

fn sync_request(
    group_id: &str,
    access_token: &[u8],
    device: DeviceAnnouncement,
    after_cursor: u64,
    events: Vec<EncryptedEvent>,
) -> Request {
    Request::Sync {
        group_id: group_id.into(),
        access_token: access_token.to_vec(),
        device,
        after_cursor,
        events,
        limit: 100,
        byte_limited: None,
        events_complete: Some(true),
        bounded_hub: Some(true),
    }
}

async fn call(connection: &Connection, request: Request) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await.context("read server response")
}

async fn upload_blob(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    send.write_all(blob).await?;
    send.finish()?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobStored
    );
    Ok(())
}

async fn upload_source_icon(
    connection: &Connection,
    group_id: &str,
    access_token: &[u8],
    blob_id: &str,
    blob: &[u8],
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::PutManagedSourceIcon {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
            size: blob.len() as u64,
        },
    )
    .await?;
    assert_eq!(
        read_frame::<_, Response>(&mut recv).await?,
        Response::BlobReady { size: 0 }
    );
    send.write_all(blob).await?;
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
    let (mut send, mut recv) = connection.open_bi().await?;
    write_frame(
        &mut send,
        &Request::GetBlob {
            group_id: group_id.into(),
            access_token: access_token.to_vec(),
            blob_id: blob_id.into(),
        },
    )
    .await?;
    send.finish()?;
    let Response::BlobReady { size } = read_frame(&mut recv).await? else {
        anyhow::bail!("expected blob header");
    };
    recv.read_to_end(size as usize)
        .await
        .context("read encrypted blob")
}
