use super::*;

fn event(index: u64, size: usize) -> EncryptedEvent {
    EncryptedEvent {
        event_id: format!("event-{index}"),
        origin_device_id: "device".into(),
        origin_sequence: index,
        created_at_ms: 100,
        nonce: vec![255; 24],
        ciphertext: vec![255; size],
        kind: SyncEventKind::Content,
        blob_ids: Vec::new(),
        optional_blob_ids: Vec::new(),
        retention_key: None,
    }
}

#[test]
fn lan_content_plan_is_frame_bounded_without_skipping_event_order() {
    let events = (0..4)
        .map(|index| event(index, 3 * 1024 * 1024))
        .collect::<Vec<_>>();
    let mut request = Request::PlanLanContent {
        group_id: "group".into(),
        access_token: vec![7; 32],
        device_id: "device".into(),
        events: events.clone(),
    };

    let count = limit_lan_content_plan(&mut request).unwrap();
    let encoded = minicbor::to_vec(&request).unwrap();
    assert!(encoded.len() <= MAX_FRAME_BYTES);
    assert!(count > 0 && count < events.len());
    let Request::PlanLanContent {
        events: planned, ..
    } = request
    else {
        panic!("expected LAN content plan")
    };
    assert_eq!(planned, events[..count]);
}

#[test]
fn retention_plan_pages_fit_the_frame_without_skipping_legacy_events() {
    let events = (0..4)
        .map(|cursor| CloudEvent {
            cursor,
            event: event(cursor, 3 * 1024 * 1024),
            available_blob_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut response = Response::RetentionPlan {
        epoch: "epoch".into(),
        events: events.clone(),
        has_more: false,
    };
    limit_retention_plan(&mut response).unwrap();
    let bytes = minicbor::to_vec(&response).unwrap();
    assert!(bytes.len() <= MAX_FRAME_BYTES);
    let Response::RetentionPlan {
        events: page,
        has_more,
        ..
    } = minicbor::decode::<Response>(&bytes).unwrap()
    else {
        panic!("expected retention plan")
    };
    assert_eq!(page, events[..page.len()]);
    assert!(!page.is_empty() && page.len() < events.len());
    assert!(has_more);

    let mut oversized = Response::RetentionPlan {
        epoch: "epoch".into(),
        events: vec![CloudEvent {
            cursor: 1,
            event: event(1, MAX_FRAME_BYTES),
            available_blob_ids: Vec::new(),
        }],
        has_more: false,
    };
    assert!(matches!(
        limit_retention_plan(&mut oversized),
        Err(FrameError::TooLarge { .. })
    ));
}

#[test]
fn verification_pages_keep_large_state_events_in_order_with_a_resumable_cursor() {
    let events = (1..=4)
        .rev()
        .map(|cursor| {
            let mut encrypted = event(cursor, 3 * 1024 * 1024);
            encrypted.kind = SyncEventKind::ItemState;
            CloudEvent {
                cursor,
                event: encrypted,
                available_blob_ids: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    let mut remaining = events.clone();
    let mut received = Vec::new();
    loop {
        let mut response = Response::EventsPage {
            events: remaining.clone(),
            next_before_cursor: None,
            total: 4,
        };
        limit_events_page(&mut response).unwrap();
        let bytes = minicbor::to_vec(&response).unwrap();
        assert!(bytes.len() <= MAX_FRAME_BYTES);
        let Response::EventsPage {
            events: page,
            next_before_cursor,
            ..
        } = minicbor::decode::<Response>(&bytes).unwrap()
        else {
            panic!("expected event page")
        };
        received.extend(page.clone());
        let Some(before) = next_before_cursor else {
            break;
        };
        assert_eq!(before, page.last().unwrap().cursor);
        remaining.retain(|event| event.cursor < before);
    }
    assert_eq!(received, events);
    let mut empty = Response::EventsPage {
        events: Vec::new(),
        next_before_cursor: None,
        total: 0,
    };
    limit_events_page(&mut empty).unwrap();
    assert!(matches!(
        empty,
        Response::EventsPage {
            next_before_cursor: None,
            ..
        }
    ));
}

fn request(cloud: bool, events: Vec<EncryptedEvent>) -> Request {
    let device = DeviceAnnouncement {
        device_id: "device".into(),
        device_name: "test".into(),
        platform: "macos".into(),
        endpoint_id: "endpoint".into(),
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        app_version: "1.1.154".into(),
    };
    if cloud {
        Request::CloudSync {
            group_id: "group".into(),
            access_token: vec![1; 32],
            device,
            after_cursor: 0,
            events,
            removed_devices: Vec::new(),
            limit: MAX_EVENTS_PER_BATCH,
            bounded_hub: true,
        }
    } else {
        Request::Sync {
            group_id: "group".into(),
            access_token: vec![1; 32],
            device,
            after_cursor: 0,
            events,
            limit: MAX_EVENTS_PER_BATCH,
            events_complete: true,
        }
    }
}

fn response(cloud: bool, events: Vec<CloudEvent>, after_cursor: u64) -> Response {
    let latest_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(after_cursor);
    if cloud {
        Response::CloudSynced {
            accepted_event_ids: vec!["accepted".into()],
            events,
            peers: Vec::new(),
            latest_cursor,
            removed_devices: Vec::new(),
            has_more: false,
        }
    } else {
        let events = events
            .into_iter()
            .map(|event| CursorEvent {
                cursor: event.cursor,
                event: event.event,
            })
            .collect();
        Response::Synced {
            accepted_event_ids: vec!["accepted".into()],
            events,
            peers: Vec::new(),
            latest_cursor,
            has_more: false,
        }
    }
}

#[test]
fn optional_blob_references_round_trip() {
    let mut current = event(1, 32);
    current.blob_ids = vec!["a".repeat(64), "b".repeat(64)];
    current.optional_blob_ids = vec!["b".repeat(64)];

    let decoded: EncryptedEvent = minicbor::decode(&minicbor::to_vec(&current).unwrap()).unwrap();

    assert_eq!(decoded, current);
}

#[test]
fn request_prefix_fits_real_encoding_including_array_header_boundaries() {
    for cloud in [false, true] {
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
            let mut outgoing = request(cloud, events.clone());
            let kept = limit_sync_request(&mut outgoing).unwrap();
            assert!(minicbor::to_vec(&outgoing).unwrap().len() <= MAX_FRAME_BYTES);
            let included = match &outgoing {
                Request::Sync { events, .. } | Request::CloudSync { events, .. } => events,
                _ => unreachable!(),
            };
            assert_eq!(included, &events[..kept]);
            if kept < events.len() {
                let extended = request(cloud, events[..=kept].to_vec());
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
            available_blob_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    for cloud in [false, true] {
        let mut cursor = 0;
        let mut received = Vec::new();
        let mut batches = 0;
        loop {
            let mut outgoing = response(
                cloud,
                events
                    .iter()
                    .filter(|event| event.cursor > cursor)
                    .cloned()
                    .collect(),
                cursor,
            );
            limit_sync_response(&mut outgoing, cursor, MAX_EVENTS_PER_BATCH).unwrap();
            let encoded = minicbor::to_vec(&outgoing).unwrap();
            assert!(encoded.len() <= MAX_FRAME_BYTES);
            let decoded: Response = minicbor::decode(&encoded).unwrap();
            let (page, latest, more) = match decoded {
                Response::Synced {
                    events,
                    latest_cursor,
                    has_more,
                    ..
                } => (
                    events
                        .into_iter()
                        .map(|event| event.cursor)
                        .collect::<Vec<_>>(),
                    latest_cursor,
                    has_more,
                ),
                Response::CloudSynced {
                    events,
                    latest_cursor,
                    has_more,
                    ..
                } => (
                    events
                        .into_iter()
                        .map(|event| event.cursor)
                        .collect::<Vec<_>>(),
                    latest_cursor,
                    has_more,
                ),
                _ => unreachable!(),
            };
            assert_eq!(latest, page.last().copied().unwrap_or(cursor));
            assert!(latest > cursor);
            received.extend(page);
            cursor = latest;
            batches += 1;
            if !more {
                break;
            }
            assert!(more);
            assert!(batches < 20);
        }
        assert!(batches > 8);
        assert_eq!(received, (1..=10).collect::<Vec<_>>());

        let mut empty = response(cloud, Vec::new(), cursor);
        limit_sync_response(&mut empty, cursor, MAX_EVENTS_PER_BATCH).unwrap();
        match empty {
            Response::Synced {
                latest_cursor,
                has_more,
                ..
            }
            | Response::CloudSynced {
                latest_cursor,
                has_more,
                ..
            } => {
                assert_eq!(latest_cursor, cursor);
                assert!(!has_more);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn oversized_first_event_is_not_skipped() {
    for cloud in [false, true] {
        let oversized = event(1, MAX_FRAME_BYTES / 2);
        let mut outgoing = request(cloud, vec![oversized.clone(), event(2, 1)]);
        assert!(matches!(
            limit_sync_request(&mut outgoing),
            Err(FrameError::TooLarge { .. })
        ));
        let events = vec![CloudEvent {
            cursor: 1,
            event: oversized,
            available_blob_ids: Vec::new(),
        }];
        let mut outgoing = response(cloud, events, 0);
        assert!(matches!(
            limit_sync_response(&mut outgoing, 0, MAX_EVENTS_PER_BATCH),
            Err(FrameError::TooLarge { .. })
        ));
    }
}
