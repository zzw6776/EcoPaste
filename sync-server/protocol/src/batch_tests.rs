use super::*;

fn sample_device() -> DeviceAnnouncement {
    DeviceAnnouncement {
        device_id: "device_123".into(),
        device_name: "Mac".into(),
        platform: "macos".into(),
        endpoint_id: "endpoint_123".into(),
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        app_version: "1.1.231".into(),
    }
}

fn sample_write(index: u64, payload_bytes: usize) -> SyncItemWrite {
    SyncItemWrite {
        item_key: format!("{index:064x}"),
        expected_hub_rev: 0,
        local_seq: index,
        origin_device_id: "device_123".into(),
        updated_at_ms: 1_700_000_000_000 + index as i64,
        deleted: false,
        protected: false,
        nonce: vec![1; 24],
        ciphertext: vec![7; payload_bytes],
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
    }
}

fn sample_record(seq: u64, payload_bytes: usize) -> SyncItemRecord {
    SyncItemRecord {
        seq,
        hub_rev: 1,
        item_key: format!("{seq:064x}"),
        origin_device_id: "device_123".into(),
        updated_at_ms: 1_700_000_000_000 + seq as i64,
        deleted: false,
        protected: false,
        nonce: vec![1; 24],
        ciphertext: vec![7; payload_bytes],
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        available_blob_ids: Vec::new(),
    }
}

#[test]
fn limit_sync_request_splits_large_batches_within_frame_limit() {
    let items = vec![
        sample_write(3, 3 * 1024 * 1024),
        sample_write(2, 3 * 1024 * 1024),
        sample_write(1, 3 * 1024 * 1024),
    ];
    let mut request = Request::CloudSyncItems {
        group_id: "group".into(),
        access_token: vec![1; 32],
        device: sample_device(),
        after_seq: 0,
        before_seq: None,
        items: items.clone(),
        removed_devices: Vec::new(),
        limit: MAX_ITEMS_PER_BATCH,
    };

    let kept = limit_sync_request(&mut request).unwrap();
    assert_eq!(kept, 2);
    assert!(minicbor::to_vec(&request).unwrap().len() <= MAX_FRAME_BYTES);
    let Request::CloudSyncItems {
        items: kept_items, ..
    } = request
    else {
        unreachable!();
    };
    assert_eq!(kept_items, &items[..2]);
}

#[test]
fn limit_sync_request_rejects_oversized_first_item() {
    let mut request = Request::SyncItems {
        group_id: "group".into(),
        access_token: vec![1; 32],
        device: sample_device(),
        after_seq: 0,
        before_seq: None,
        items: vec![sample_write(1, MAX_FRAME_BYTES)],
        limit: MAX_ITEMS_PER_BATCH,
    };

    assert!(matches!(
        limit_sync_request(&mut request),
        Err(FrameError::TooLarge { .. })
    ));
}

#[test]
fn limit_sync_response_sets_next_before_seq_on_truncation() {
    let mut response = Response::CloudSynced {
        accepted: Vec::new(),
        conflicts: Vec::new(),
        items: vec![
            sample_record(100, 3 * 1024 * 1024),
            sample_record(99, 3 * 1024 * 1024),
            sample_record(98, 3 * 1024 * 1024),
        ],
        peers: Vec::new(),
        removed_devices: Vec::new(),
        latest_seq: 100,
        pruned_through_seq: 0,
        next_before_seq: None,
    };

    limit_sync_response(&mut response, false).unwrap();
    assert!(minicbor::to_vec(&response).unwrap().len() <= MAX_FRAME_BYTES);
    let Response::CloudSynced {
        items,
        next_before_seq,
        ..
    } = response
    else {
        unreachable!();
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].seq, 100);
    assert_eq!(items[1].seq, 99);
    assert_eq!(next_before_seq, Some(99));
}

#[test]
fn limit_items_page_preserves_continuation_cursor() {
    let mut response = Response::ItemsPage {
        items: vec![
            sample_record(30, 3 * 1024 * 1024),
            sample_record(29, 3 * 1024 * 1024),
            sample_record(28, 3 * 1024 * 1024),
        ],
        next_before_seq: Some(28),
        total: 10,
    };

    limit_items_page(&mut response).unwrap();
    let Response::ItemsPage {
        items,
        next_before_seq,
        ..
    } = response
    else {
        unreachable!();
    };
    assert_eq!(items.len(), 2);
    assert_eq!(next_before_seq, Some(29));
}
