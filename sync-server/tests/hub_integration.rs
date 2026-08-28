use anyhow::{Context, Result};
use ecopaste_sync_protocol::{
    ALPN, DeviceAnnouncement, EncryptedEvent, ErrorCode, Request, Response, read_frame, write_frame,
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
    let service = HubService::new(repository, directory.path().join("blobs"), 1024 * 1024);
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    let server_address = server.addr();
    let router = Router::builder(server).accept(ALPN, service).spawn();
    let client = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;
    let client_endpoint_id = client.id().to_string();
    let connection = client.connect(server_address, ALPN).await?;

    assert!(matches!(
        call(&connection, Request::Health).await?,
        Response::Health {
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

    let event = EncryptedEvent {
        event_id: "event_123456".into(),
        origin_device_id: "device_123456".into(),
        origin_sequence: 1,
        created_at_ms: 100,
        nonce: vec![1; 24],
        ciphertext: vec![2; 128],
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
    };
    let notify_watch = sync_request(
        &group_id,
        &access_token,
        device("device_123456", "Mac", &client_endpoint_id),
        1,
        vec![next_event.clone()],
    );
    assert!(matches!(
        call(&connection, notify_watch).await?,
        Response::Synced { .. }
    ));
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
    assert_eq!(events[0].event, event);
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
