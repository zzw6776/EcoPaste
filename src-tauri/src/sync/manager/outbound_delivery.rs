use super::*;

impl SyncManager {
    pub(super) async fn start_preparation_transfer(
        &self,
        pool: &SqlitePool,
        operation: &db::sync_queue::ContentOutboxOperation,
        event_id: &str,
        item: &ClipboardItem,
        target: &PreparationTarget,
        auto_upload_max_mb: u32,
    ) -> Result<Option<super::super::progress::TransferHandle>> {
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let Some(total_bytes) = calculate_current_file_item_size(pool, item, &resources).await?
        else {
            return Ok(None);
        };
        let total_bytes = u64::try_from(total_bytes).context("invalid clipboard file size")?;
        if !(operation.force_files || operation.manual_share)
            && !permits_automatic_file_upload(total_bytes, auto_upload_max_mb)
        {
            return Ok(None);
        }
        let task_key = format!("{event_id}\0prepare");
        let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
        Ok(Some(self.delivery.start_preparation(
            super::super::progress::TransferTask {
                id: task_id,
                operation_id: event_id.to_owned(),
                item_id: item.id.clone(),
                direction: super::super::progress::TransferDirection::Upload,
                route: target.route,
                target: "preparation".to_owned(),
                file_name: None,
                phase: super::super::progress::TransferPhase::Preparing,
                transferred_bytes: 0,
                total_bytes,
                operation_total_bytes: total_bytes,
                speed_bytes_per_second: 0,
                elapsed_ms: 0,
                eta_seconds: None,
                error: None,
                cancellable: true,
            },
            operation.operation_order,
            item.source_revision.clone(),
        )))
    }

    /// Lets the LAN receiver apply its download threshold before any attachment bytes are sent.
    async fn plan_lan_content(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        device_id: &str,
        events: &[StoredSyncEvent],
    ) -> Result<(usize, HashSet<String>)> {
        let mut request = Request::PlanLanContent {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            device_id: device_id.to_owned(),
            events: events.iter().map(|stored| stored.event.clone()).collect(),
        };
        let planned_count = limit_lan_content_plan(&mut request)?;
        let planned_ids = match &request {
            Request::PlanLanContent { events, .. } => events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<HashSet<_>>(),
            _ => unreachable!(),
        };
        let response = call(connection, request).await?;
        let Response::LanContentPlan { deferred_event_ids } = response else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("局域网设备返回了无效的文件接收计划");
        };
        if deferred_event_ids
            .iter()
            .any(|event_id| !planned_ids.contains(event_id))
        {
            bail!("局域网设备返回了未知的文件接收计划");
        }
        Ok((planned_count, deferred_event_ids.into_iter().collect()))
    }

    /// Uploads attachments in bounded parallel streams and confirms unrelated records independently.
    pub(super) async fn sync_connection(
        self: &Arc<Self>,
        connection: &Connection,
        target: &SyncConnectionTarget,
        after_cursor: u64,
        cycle: &SyncCycleContext,
    ) -> Result<SyncConnectionOutcome> {
        let _sync_activity = SyncActivityGuard::acquire();
        let is_cloud = target.is_cloud();
        let target_id = target.delivery_target();
        let pool = cycle.pool.clone();
        let group = &cycle.group;
        let device_id = &cycle.device_id;
        if !is_cloud {
            match call(connection, Request::Health).await? {
                Response::Health {
                    protocol_version, ..
                } if protocol_version >= STREAMING_BLOB_PROTOCOL_VERSION => {}
                Response::Health { .. } => bail!("对端设备版本过低，请先升级后再同步"),
                Response::Error { message, .. } => bail!(message),
                _ => bail!("对端设备返回了无效的协议版本响应"),
            }
        }
        if is_cloud
            && self
                .refresh_cloud_retention(
                    connection,
                    &pool,
                    group,
                    &cycle.device,
                    MAX_SYNC_BATCHES_PER_CONNECTION,
                )
                .await?
        {
            return Ok(SyncConnectionOutcome { has_more: true });
        }
        let managed_hub_blobs = is_cloud && self.bounded_hub_supported(connection, &group.group_id);
        let optional_blob_refs =
            is_cloud && self.cloud_optional_blob_refs_supported(connection, &group.group_id);
        let deferred_icons = is_cloud
            && self.cloud_async_source_icons_supported(connection, &group.group_id)
            && (!managed_hub_blobs || optional_blob_refs);
        let context = SyncExchange {
            connection,
            target,
            group,
            pool: &pool,
            device_id,
            device: &cycle.device,
            settings: &cycle.settings,
        };
        let mut cursor = after_cursor;
        let mut has_more = false;
        for _ in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            let preparation_target = PreparationTarget {
                route: if is_cloud {
                    super::super::progress::TransferRoute::Cloud
                } else {
                    super::super::progress::TransferRoute::Lan
                },
                target: target_id.to_owned(),
            };
            let materialization = self
                .materialize_local_operations(true, 1, Some(&preparation_target), Some(cycle))
                .await?;
            let local_materialized = materialization.processed > 0;
            let _materialized_staging = materialization.staging;
            let mut pending = if is_cloud {
                repository::pending_origin_events_for_target(
                    &pool,
                    target_id,
                    device_id,
                    MAX_EVENTS_PER_BATCH,
                )
                .await?
            } else {
                repository::pending_events_for_target(&pool, target_id, MAX_EVENTS_PER_BATCH)
                    .await?
            };
            let mut more_available = pending.len() == usize::from(MAX_EVENTS_PER_BATCH);
            if is_cloud {
                let key = group.content_key_bytes()?;
                for stored in &mut pending {
                    stored.event.retention_key =
                        Some(crypto::retention_mapping(&key, &stored.event)?.item_key);
                }
            }
            if managed_hub_blobs {
                repository::attach_event_blob_ids(&pool, &mut pending).await?;
                if optional_blob_refs {
                    mark_optional_source_icon_refs(&mut pending, &group.content_key_bytes()?);
                }
            }
            let mut deferred_event_ids = if is_cloud || pending.is_empty() {
                HashSet::new()
            } else {
                let original_count = pending.len();
                let (planned_count, deferred) = self
                    .plan_lan_content(connection, group, device_id, &pending)
                    .await?;
                pending.truncate(planned_count);
                more_available |= planned_count < original_count;
                deferred
            };
            let route = if is_cloud {
                super::super::progress::TransferRoute::Cloud
            } else {
                super::super::progress::TransferRoute::Lan
            };
            self.delivery
                .filter_pending(&pool, target_id, &mut pending, &mut deferred_event_ids)
                .await?;
            let ids = pending
                .iter()
                .map(|stored| stored.event.event_id.clone())
                .collect::<Vec<_>>();
            self.delivery.queued(&pool, target_id, &ids).await?;
            let blobs = repository::blobs_for_events(&pool, &ids).await?;
            let mut queue = super::super::transfer::TransferQueue::new(
                pending,
                blobs,
                &group.content_key_bytes()?,
                managed_hub_blobs && !deferred_icons,
                &deferred_event_ids,
            )?;
            let mut uploads = tokio::task::JoinSet::new();
            let (upload_started, mut upload_notifications) = mpsc::unbounded_channel();
            let mut failure = None;
            for (id, reason) in queue.take_failures() {
                self.delivery
                    .failed(&pool, target_id, &[id], &reason)
                    .await?;
                failure.get_or_insert_with(|| anyhow::anyhow!(reason));
            }
            let mut pull_needed = true;
            while pull_needed || queue.has_pending() {
                self.delivery
                    .reconcile_queue(&pool, target_id, &mut queue)
                    .await?;
                while uploads.len() < MAX_CONCURRENT_BLOB_TRANSFERS {
                    let Some(blob) = queue.next_blob() else {
                        break;
                    };
                    let transfer = loop {
                        let Some(event_id) = queue.event_for_blob(&blob.blob_id).map(str::to_owned)
                        else {
                            break None;
                        };
                        match self
                            .delivery
                            .start_upload(&pool, target_id, route, &event_id, &blob)
                            .await?
                        {
                            UploadTransferStart::Ready(transfer) => break Some(transfer),
                            UploadTransferStart::Canceled => {
                                queue.cancel_events(&HashSet::from([event_id]));
                            }
                        }
                    };
                    let Some(transfer) = transfer else {
                        continue;
                    };
                    let connection = connection.clone();
                    let group = group.clone();
                    let manager = self.clone();
                    let blob_id = blob.blob_id.clone();
                    let options = BlobUploadOptions {
                        kind: BlobUploadKind::Content,
                        managed_hub_blob: managed_hub_blobs,
                        is_cloud,
                        language: crate::i18n::current_language(&self.app),
                        started: Some(upload_started.clone()),
                        transfer,
                    };
                    uploads.spawn(async move {
                        let result = async {
                            let (staged, _staging_lease) = if blob.encrypted_path.is_empty() {
                                (blob.clone(), None)
                            } else {
                                let (staged, lease) =
                                    manager.lease_outgoing_staging(&group, &blob).await?;
                                (staged, Some(lease))
                            };
                            upload_blob(&connection, &group, &staged, options).await
                        }
                        .await;
                        (blob_id, result)
                    });
                }
                self.emit_updated();
                let prepared = self
                    .delivery
                    .prepare_ready_events(&pool, target_id, route, &deferred_event_ids, &mut queue)
                    .await?;
                let mut ready = prepared.events;
                let metadata_only_deliveries = prepared.metadata_only_operations;
                if ready.is_empty() && !pull_needed {
                    let result = tokio::select! {
                        Some(blob_id) = upload_notifications.recv() => {
                            self.delivery
                                .syncing(
                                    &pool,
                                    target_id,
                                    &queue.events_for_blob(&blob_id),
                                )
                                .await?;
                            self.emit_updated();
                            continue;
                        }
                        result = uploads.join_next() => result,
                    };
                    let Some(result) = result else {
                        break;
                    };
                    let (blob_id, result) = result.context("attachment upload task failed")?;
                    match result {
                        Ok(()) => queue.blob_stored(blob_id),
                        Err(error) => {
                            let ids = queue.fail_blob(&blob_id);
                            log::warn!(
                                "sync attachment upload failed: blobRef={} error={error:#}",
                                blob_log_ref(&blob_id)
                            );
                            self.delivery
                                .failed(&pool, target_id, &ids, &error.to_string())
                                .await?;
                            self.emit_updated();
                            let message = error.to_string();
                            failure.get_or_insert(error);
                            if connection.close_reason().is_some() {
                                self.delivery
                                    .failed(&pool, target_id, &queue.active_events(), &message)
                                    .await?;
                                break;
                            }
                        }
                    }
                    continue;
                }
                pull_needed = false;
                let more_outgoing = more_available || queue.has_pending();
                match self
                    .exchange_ready_events(&context, cursor, &mut ready, more_outgoing)
                    .await
                {
                    Ok(outcome) => {
                        self.delivery.complete_metadata_only(
                            &metadata_only_deliveries,
                            &outcome.event_ids,
                            route,
                            target_id,
                        );
                        queue.confirm_events(&outcome.event_ids);
                        cursor = outcome.latest_cursor;
                        has_more = outcome.has_more || local_materialized;
                    }
                    Err(error) => {
                        self.delivery.retry_metadata_only(
                            &metadata_only_deliveries,
                            route,
                            target_id,
                        );
                        if is_cloud && should_invalidate_cloud_connection(connection, &error) {
                            self.invalidate_cloud_connection(connection.stable_id());
                            self.runtime_state.watch_wake.notify_one();
                        }
                        let submitted = ready
                            .iter()
                            .map(|stored| stored.event.event_id.clone())
                            .collect::<Vec<_>>();
                        queue.fail_events(&submitted);
                        let message = error.to_string();
                        self.delivery
                            .failed(&pool, target_id, &submitted, &message)
                            .await?;
                        self.emit_updated();
                        failure.get_or_insert(error);
                        if connection.close_reason().is_some() {
                            self.delivery
                                .failed(&pool, target_id, &queue.active_events(), &message)
                                .await?;
                            break;
                        }
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            if !has_more {
                break;
            }
        }
        if is_cloud {
            has_more |= self
                .refresh_cloud_retention(connection, &pool, group, &cycle.device, 1)
                .await?;
            has_more |= self
                .reconcile_legacy_cloud_deliveries(connection, &pool, device_id, group)
                .await?;
        }
        self.runtime_state.source_asset_wake.notify_one();
        Ok(SyncConnectionOutcome { has_more })
    }

    /// Publishes only attachment-ready records and applies the Hub/peer acknowledgement immediately.
    async fn exchange_ready_events(
        self: &Arc<Self>,
        context: &SyncExchange<'_>,
        after_cursor: u64,
        pending: &mut Vec<StoredSyncEvent>,
        more_outgoing: bool,
    ) -> Result<SyncReadyOutcome> {
        let SyncExchange {
            connection,
            target,
            group,
            pool,
            device_id,
            device,
            settings,
        } = *context;
        let is_cloud = target.is_cloud();
        let target_id = target.delivery_target();
        let pool = pool.clone();
        let language = crate::i18n::current_language(&self.app);
        let use_sync_v2 = is_cloud && self.cloud_sync_v2_supported(connection, &group.group_id);
        let managed_hub_blobs = is_cloud && self.bounded_hub_supported(connection, &group.group_id);
        let optional_blob_refs =
            is_cloud && self.cloud_optional_blob_refs_supported(connection, &group.group_id);
        let publish_source_icons_after_event = is_cloud
            && self.cloud_async_source_icons_supported(connection, &group.group_id)
            && (!managed_hub_blobs || optional_blob_refs);
        let removed_devices = if use_sync_v2 {
            repository::removed_devices(&pool).await?
        } else {
            self.sync_removed_devices(
                connection,
                &pool,
                group,
                device_id,
                &device.endpoint_id,
                is_cloud,
            )
            .await?;
            Vec::new()
        };
        let batch_started_at = Instant::now();
        let available_count = pending.len();
        let outgoing_events = pending.iter().map(|item| item.event.clone()).collect();
        // Keep the field present while byte-limiting so the final continuation value cannot
        // make an otherwise fitting request exceed the frame limit.
        let mut request = if use_sync_v2 {
            Request::SyncV2 {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: self.cloud_device_announcement_for(device),
                after_cursor,
                events: outgoing_events,
                removed_devices: removed_devices.clone(),
                limit: MAX_EVENTS_PER_BATCH,
                byte_limited: Some(true),
                events_complete: Some(false),
                bounded_hub: managed_hub_blobs.then_some(true),
            }
        } else {
            Request::Sync {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: if is_cloud {
                    self.cloud_device_announcement_for(device)
                } else {
                    device.clone()
                },
                after_cursor,
                events: outgoing_events,
                limit: MAX_EVENTS_PER_BATCH,
                byte_limited: Some(true),
                events_complete: Some(false),
                bounded_hub: managed_hub_blobs.then_some(true),
            }
        };
        let pending_count = match limit_sync_request(&mut request) {
            Ok(count) => count,
            Err(error) => {
                pending.truncate(1);
                return Err(error.into());
            }
        };
        let outgoing_has_more = more_outgoing || pending_count < available_count;
        match &mut request {
            Request::Sync {
                events_complete, ..
            }
            | Request::SyncV2 {
                events_complete, ..
            } => *events_complete = Some(!outgoing_has_more),
            _ => unreachable!("sync request variant was constructed above"),
        }
        pending.truncate(pending_count);
        let event_ids = pending
            .iter()
            .map(|item| item.event.event_id.clone())
            .collect::<Vec<_>>();
        let source_icon_blob_ids = source_icon_blob_ids(pending, &group.content_key_bytes()?);
        self.delivery
            .confirming(&pool, target_id, &event_ids)
            .await?;
        self.emit_updated();
        let blob_started_at = Instant::now();
        let blobs = repository::blobs_for_events(&pool, &event_ids).await?;
        let (required_blobs, source_icon_blobs) =
            split_source_icon_blobs(blobs, &source_icon_blob_ids);
        let required_blob_count = required_blobs.len();
        let source_icon_blob_count = source_icon_blobs.len();
        // A bounded Hub can reclaim an icon while this QUIC connection is still alive.
        let (source_icon_blobs, source_icon_cache_hits) = if is_cloud && !managed_hub_blobs {
            self.runtime_state
                .confirmed_cloud_source_icons
                .write()
                .expect("confirmed cloud source icons poisoned")
                .retain_unconfirmed(connection.stable_id(), &group.group_id, source_icon_blobs)
        } else {
            (source_icon_blobs, 0)
        };
        let source_icon_blob_checks = source_icon_blobs.len();
        let checked_source_icon_blob_ids = source_icon_blobs
            .iter()
            .map(|blob| blob.blob_id.clone())
            .collect::<Vec<_>>();
        let blob_elapsed = blob_started_at.elapsed();
        let mut deferred_source_icon_blobs = source_icon_blobs;
        let mut source_icon_elapsed = Duration::ZERO;
        let mut queued_source_icon_count = 0;
        if !publish_source_icons_after_event {
            let source_icon_started_at = Instant::now();
            match upload_blobs(
                self.clone(),
                connection,
                group,
                std::mem::take(&mut deferred_source_icon_blobs),
                BlobUploadOptions {
                    kind: BlobUploadKind::SourceIcon,
                    managed_hub_blob: managed_hub_blobs,
                    is_cloud,
                    language,
                    started: None,
                    transfer: None,
                },
            )
            .await
            {
                Ok(()) if is_cloud => {
                    self.runtime_state
                        .confirmed_cloud_source_icons
                        .write()
                        .expect("confirmed cloud source icons poisoned")
                        .confirm(
                            connection.stable_id(),
                            &group.group_id,
                            &checked_source_icon_blob_ids,
                        );
                }
                Ok(()) => {}
                Err(error) if managed_hub_blobs => return Err(error),
                Err(error) => log::warn!("publish optional source app icons failed: {error}"),
            }
            source_icon_elapsed = source_icon_started_at.elapsed();
        }
        let request_started_at = Instant::now();
        let result = async {
            let protocol = if use_sync_v2 { "v2" } else { "legacy" };
            let response = if is_cloud {
                call_cloud(connection, request, language).await?
            } else {
                call_lan(connection, request).await?
            };
            let response = match response {
                Response::Synced {
                    events,
                    peers,
                    latest_cursor,
                    has_more,
                    ..
                } => SyncBatchResponse {
                    events,
                    peers,
                    latest_cursor,
                    removed_devices: Vec::new(),
                    has_more,
                },
                Response::SyncedV2 {
                    events,
                    peers,
                    latest_cursor,
                    removed_devices,
                    has_more,
                    ..
                } => SyncBatchResponse {
                    events,
                    peers,
                    latest_cursor,
                    removed_devices,
                    has_more,
                },
                Response::Error {
                    code: ErrorCode::HistoryGap,
                    ..
                } if is_cloud => return Err(anyhow::Error::new(CloudHistoryGap)),
                Response::Error { message, .. } => bail!(message),
                _ => bail!("同步端返回了无效响应"),
            };
            Ok::<_, anyhow::Error>((response, protocol))
        }
        .await;
        let (response, protocol) = match result {
            Ok(response) => response,
            Err(error) => {
                if is_cloud && error.downcast_ref::<CloudHistoryGap>().is_some() {
                    let (reset_cursor, _) = self
                        .prepare_cloud_cursor(connection, group, &pool, true)
                        .await?;
                    log::info!("cloud history gap recovered: bootstrapCursor={reset_cursor}");
                    return Ok(SyncReadyOutcome {
                        latest_cursor: reset_cursor,
                        has_more: true,
                        event_ids: Vec::new(),
                    });
                }
                return Err(error);
            }
        };
        let request_elapsed = request_started_at.elapsed();
        let received_count = response.events.len();
        let incoming_has_more = response
            .has_more
            .unwrap_or(received_count == usize::from(MAX_EVENTS_PER_BATCH));
        let mut has_more = outgoing_has_more || incoming_has_more;
        let removed_count = response.removed_devices.len();
        if !response.removed_devices.is_empty() {
            self.apply_removed_devices(
                &pool,
                response.removed_devices,
                device_id,
                &device.endpoint_id,
            )
            .await?;
        }
        if publish_source_icons_after_event && !deferred_source_icon_blobs.is_empty() {
            repository::enqueue_source_icon_uploads(
                &pool,
                &group.group_id,
                &deferred_source_icon_blobs,
            )
            .await?;
        }
        self.delivery
            .completed(&pool, target_id, &event_ids)
            .await?;
        self.release_staging_for_events(&pool, &event_ids).await?;
        self.emit_updated();
        for peer in response.peers {
            if peer.device_id != device_id {
                if is_cloud {
                    repository::upsert_cloud_peer_identity(&pool, &peer).await?;
                } else {
                    repository::upsert_peer(&pool, &peer).await?;
                }
            }
        }
        let apply_started_at = Instant::now();
        let content_has_more = self
            .accept_remote_events(
                connection,
                &pool,
                settings,
                device_id,
                response.events,
                group,
                target,
                response.latest_cursor,
                !incoming_has_more,
            )
            .await?;
        has_more |= content_has_more;
        let apply_elapsed = apply_started_at.elapsed();
        if publish_source_icons_after_event {
            queued_source_icon_count = deferred_source_icon_blobs.len();
            if queued_source_icon_count > 0 {
                self.runtime_state.source_asset_wake.notify_one();
            }
        }
        if is_cloud {
            log::info!(
                "cloud sync batch completed: protocol={protocol} sentEvents={pending_count} receivedEvents={received_count} removals={removed_count} requiredBlobs={required_blob_count} sourceIconBlobs={source_icon_blob_count} sourceIconBlobChecks={source_icon_blob_checks} sourceIconBlobCacheHits={source_icon_cache_hits} eventFirstSourceIcons={publish_source_icons_after_event} queuedSourceIcons={queued_source_icon_count} blobMs={} requestMs={} applyMs={} sourceIconMs={} totalMs={}",
                blob_elapsed.as_millis(),
                request_elapsed.as_millis(),
                apply_elapsed.as_millis(),
                source_icon_elapsed.as_millis(),
                batch_started_at.elapsed().as_millis()
            );
        }
        Ok(SyncReadyOutcome {
            latest_cursor: response.latest_cursor,
            has_more,
            event_ids,
        })
    }
}
