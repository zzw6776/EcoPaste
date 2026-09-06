use super::*;

/// Keeps the pre-batching wire shape independent of the current protocol types.
#[derive(Debug, Encode, Decode)]
enum LegacyRequest {
    #[n(2)]
    Sync {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_cursor: u64,
        #[n(4)]
        events: Vec<EncryptedEvent>,
        #[n(5)]
        limit: u16,
    },
    #[n(12)]
    SyncV2 {
        #[n(0)]
        group_id: String,
        #[n(1)]
        access_token: Vec<u8>,
        #[n(2)]
        device: DeviceAnnouncement,
        #[n(3)]
        after_cursor: u64,
        #[n(4)]
        events: Vec<EncryptedEvent>,
        #[n(5)]
        removed_devices: Vec<RemovedDevice>,
        #[n(6)]
        limit: u16,
    },
}

#[derive(Debug, Encode, Decode)]
enum LegacyResponse {
    #[n(2)]
    Synced {
        #[n(0)]
        accepted_event_ids: Vec<String>,
        #[n(1)]
        events: Vec<CloudEvent>,
        #[n(2)]
        peers: Vec<PeerAnnouncement>,
        #[n(3)]
        latest_cursor: u64,
    },
    #[n(12)]
    SyncedV2 {
        #[n(0)]
        accepted_event_ids: Vec<String>,
        #[n(1)]
        events: Vec<CloudEvent>,
        #[n(2)]
        peers: Vec<PeerAnnouncement>,
        #[n(3)]
        latest_cursor: u64,
        #[n(4)]
        removed_devices: Vec<RemovedDevice>,
    },
}

fn event(index: u64, size: usize) -> EncryptedEvent {
    EncryptedEvent {
        event_id: format!("event-{index}"),
        origin_device_id: "device".into(),
        origin_sequence: index,
        created_at_ms: 100,
        nonce: vec![255; 24],
        ciphertext: vec![255; size],
    }
}

fn request(v2: bool, events: Vec<EncryptedEvent>) -> Request {
    let device = DeviceAnnouncement {
        device_id: "device".into(),
        device_name: "test".into(),
        platform: "macos".into(),
        endpoint_id: "endpoint".into(),
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
    };
    if v2 {
        Request::SyncV2 {
            group_id: "group".into(),
            access_token: vec![1; 32],
            device,
            after_cursor: 0,
            events,
            removed_devices: Vec::new(),
            limit: MAX_EVENTS_PER_BATCH,
            byte_limited: Some(true),
        }
    } else {
        Request::Sync {
            group_id: "group".into(),
            access_token: vec![1; 32],
            device,
            after_cursor: 0,
            events,
            limit: MAX_EVENTS_PER_BATCH,
            byte_limited: Some(true),
        }
    }
}

fn response(v2: bool, events: Vec<CloudEvent>, after_cursor: u64) -> Response {
    let latest_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(after_cursor);
    if v2 {
        Response::SyncedV2 {
            accepted_event_ids: vec!["accepted".into()],
            events,
            peers: Vec::new(),
            latest_cursor,
            removed_devices: Vec::new(),
            has_more: None,
        }
    } else {
        Response::Synced {
            accepted_event_ids: vec!["accepted".into()],
            events,
            peers: Vec::new(),
            latest_cursor,
            has_more: None,
        }
    }
}

#[test]
fn optional_batch_fields_are_compatible_in_both_directions() {
    for v2 in [false, true] {
        let current = request(v2, vec![event(1, 100)]);
        let legacy: LegacyRequest = minicbor::decode(&minicbor::to_vec(&current).unwrap()).unwrap();
        let restored: Request = minicbor::decode(&minicbor::to_vec(&legacy).unwrap()).unwrap();
        let mut expected = current;
        match &mut expected {
            Request::Sync { byte_limited, .. } | Request::SyncV2 { byte_limited, .. } => {
                *byte_limited = None;
            }
            _ => unreachable!(),
        }
        assert_eq!(restored, expected);

        let mut current = response(
            v2,
            vec![CloudEvent {
                cursor: 1,
                event: event(1, 100),
            }],
            0,
        );
        let expected = current.clone();
        limit_sync_response(&mut current, 0, MAX_EVENTS_PER_BATCH, true).unwrap();
        let legacy: LegacyResponse =
            minicbor::decode(&minicbor::to_vec(&current).unwrap()).unwrap();
        let restored: Response = minicbor::decode(&minicbor::to_vec(&legacy).unwrap()).unwrap();
        assert_eq!(restored, expected);
    }
}

#[test]
fn request_prefix_fits_real_encoding_including_array_header_boundaries() {
    for v2 in [false, true] {
        for (count, size) in [
            (0, 0),
            (23, 1),
            (24, 1),
            (255, 1),
            (256, 1),
            (128, 64 * 1024),
        ] {
            let events = (0..count)
                .map(|index| event(index, size))
                .collect::<Vec<_>>();
            let mut outgoing = request(v2, events.clone());
            let kept = limit_sync_request(&mut outgoing).unwrap();
            assert!(minicbor::to_vec(&outgoing).unwrap().len() <= MAX_FRAME_BYTES);
            let included = match &outgoing {
                Request::Sync { events, .. } | Request::SyncV2 { events, .. } => events,
                _ => unreachable!(),
            };
            assert_eq!(included, &events[..kept]);
            if kept < events.len() {
                let extended = request(v2, events[..=kept].to_vec());
                assert!(minicbor::to_vec(&extended).unwrap().len() > MAX_FRAME_BYTES);
            }
        }
    }
}

#[test]
fn response_continuation_drains_many_short_batches_without_skipping_cursors() {
    let events = (1..=10)
        .map(|cursor| CloudEvent {
            cursor,
            event: event(cursor, 2 * 1024 * 1024 + 64),
        })
        .collect::<Vec<_>>();
    for v2 in [false, true] {
        let mut cursor = 0;
        let mut received = Vec::new();
        let mut batches = 0;
        loop {
            let mut outgoing = response(
                v2,
                events
                    .iter()
                    .filter(|event| event.cursor > cursor)
                    .cloned()
                    .collect(),
                cursor,
            );
            limit_sync_response(&mut outgoing, cursor, MAX_EVENTS_PER_BATCH, true).unwrap();
            let encoded = minicbor::to_vec(&outgoing).unwrap();
            assert!(encoded.len() <= MAX_FRAME_BYTES);
            let decoded: Response = minicbor::decode(&encoded).unwrap();
            let (page, latest, more) = match decoded {
                Response::Synced {
                    events,
                    latest_cursor,
                    has_more,
                    ..
                }
                | Response::SyncedV2 {
                    events,
                    latest_cursor,
                    has_more,
                    ..
                } => (events, latest_cursor, has_more),
                _ => unreachable!(),
            };
            assert_eq!(
                latest,
                page.last().map(|event| event.cursor).unwrap_or(cursor)
            );
            assert!(latest > cursor);
            received.extend(page.into_iter().map(|event| event.cursor));
            cursor = latest;
            batches += 1;
            if more == Some(false) {
                break;
            }
            assert_eq!(more, Some(true));
            assert!(batches < 20);
        }
        assert!(batches > 8);
        assert_eq!(received, (1..=10).collect::<Vec<_>>());

        let mut empty = response(v2, Vec::new(), cursor);
        limit_sync_response(&mut empty, cursor, MAX_EVENTS_PER_BATCH, true).unwrap();
        match empty {
            Response::Synced {
                latest_cursor,
                has_more,
                ..
            }
            | Response::SyncedV2 {
                latest_cursor,
                has_more,
                ..
            } => {
                assert_eq!(latest_cursor, cursor);
                assert_eq!(has_more, Some(false));
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn legacy_response_is_not_shortened_and_oversized_first_event_is_not_skipped() {
    for v2 in [false, true] {
        let oversized = event(1, MAX_FRAME_BYTES / 2);
        let mut outgoing = request(v2, vec![oversized.clone(), event(2, 1)]);
        assert!(matches!(
            limit_sync_request(&mut outgoing),
            Err(FrameError::TooLarge { .. })
        ));
        let events = vec![CloudEvent {
            cursor: 1,
            event: oversized,
        }];
        let mut outgoing = response(v2, events, 0);
        let original = outgoing.clone();
        limit_sync_response(&mut outgoing, 0, MAX_EVENTS_PER_BATCH, false).unwrap();
        assert_eq!(outgoing, original);
        assert!(matches!(
            limit_sync_response(&mut outgoing, 0, MAX_EVENTS_PER_BATCH, true),
            Err(FrameError::TooLarge { .. })
        ));
    }
}
