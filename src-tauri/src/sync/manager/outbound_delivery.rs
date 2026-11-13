use super::*;

/// Selects automatic cloud deliveries whose authenticated file metadata should precede content.
fn deferred_cloud_file_events(
    events: &[StoredSyncEvent],
    key: &[u8; 32],
    auto_upload_max_mb: u32,
    forced_event_ids: &HashSet<String>,
) -> Result<HashSet<String>> {
    let mut deferred = HashSet::new();
    for stored in events {
        let decrypted = crypto::decrypt_sync_event(key, &stored.event)?;
        let DecryptedSyncEvent::Clipboard(envelope) = decrypted else {
            continue;
        };
        if !forced_event_ids.contains(&stored.event.event_id)
            && should_defer_remote_files(&envelope, auto_upload_max_mb)
        {
            deferred.insert(stored.event.event_id.clone());
        }
    }
    Ok(deferred)
}

struct LanContentPlan {
    planned_count: usize,
    metadata_only_event_ids: HashSet<String>,
    available_event_ids: HashSet<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct OutgoingCardError {
    event_id: String,
    source: anyhow::Error,
}

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
        let total_bytes = if item.remote_file_state.is_some() {
            repository::external_download_sessions(pool, std::slice::from_ref(&item.id))
                .await?
                .into_iter()
                .next()
                .context(crate::i18n::commands::label(
                    crate::i18n::current_language(&self.app),
                    crate::i18n::commands::Key::RemoteFileNotDownloaded,
                ))?
                .targets
                .into_iter()
                .fold(0_u64, |total, target| {
                    total.saturating_add(target.expected_size)
                })
        } else {
            let Some(total_bytes) =
                calculate_current_file_item_size(pool, item, &resources).await?
            else {
                return Ok(None);
            };
            u64::try_from(total_bytes).context("invalid clipboard file size")?
        };
        if target.route == super::super::progress::TransferRoute::Cloud
            && !(operation.force_files || operation.manual_share)
            && !permits_automatic_file_upload(total_bytes, auto_upload_max_mb)
        {
            return Ok(None);
        }
        if target.route == super::super::progress::TransferRoute::Lan {
            return Ok(None);
        }
        if let Some(handle) =
            self.delivery
                .promote_local_import(super::super::progress::LocalImportPromotion {
                    item_id: item.id.clone(),
                    operation_id: event_id.to_owned(),
                    route: target.route,
                    target: target.target.clone(),
                    total_bytes,
                    operation_order: operation.operation_order,
                    source_revision: item.source_revision.clone(),
                })
        {
            return Ok(Some(handle));
        }
        let task_key = format!("{event_id}\0prepare");
        let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
        Ok(Some(self.delivery.start_preparation(
            super::super::progress::TransferTask {
                id: task_id,
                revision: 0,
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                operation_id: event_id.to_owned(),
                item_id: item.id.clone(),
                direction: super::super::progress::TransferDirection::Upload,
                route: target.route,
                target: "preparation".to_owned(),
                file_name: None,
                phase: super::super::progress::TransferPhase::Preparing,
                transferred_bytes: 0,
                total_bytes,
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
    ) -> Result<LanContentPlan> {
        let mut request = Request::PlanLanContent {
            group_id: group.group_id.clone(),
            access_token: group.access_token_bytes()?,
            device_id: device_id.to_owned(),
            events: events.iter().map(|stored| stored.event.clone()).collect(),
        };
        let planned_count = match limit_lan_content_plan(&mut request) {
            Ok(count) => count,
            Err(error) => {
                if let Some(stored) = events.first() {
                    return Err(OutgoingCardError {
                        event_id: stored.event.event_id.clone(),
                        source: error.into(),
                    }
                    .into());
                }
                return Err(error.into());
            }
        };
        let planned_ids = match &request {
            Request::PlanLanContent { events, .. } => events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<HashSet<_>>(),
            _ => unreachable!(),
        };
        let response = call(connection, request).await?;
        let Response::LanContentPlan {
            metadata_only_event_ids,
            available_event_ids,
        } = response
        else {
            if let Response::Error { code, message } = response {
                return Err(RemoteSyncError { code, message }.into());
            }
            bail!("局域网设备返回了无效的文件接收计划");
        };
        if metadata_only_event_ids
            .iter()
            .chain(&available_event_ids)
            .any(|event_id| !planned_ids.contains(event_id))
        {
            bail!("局域网设备返回了未知的文件接收计划");
        }
        let metadata_only_event_ids = metadata_only_event_ids.into_iter().collect::<HashSet<_>>();
        let available_event_ids = available_event_ids.into_iter().collect::<HashSet<_>>();
        if !metadata_only_event_ids.is_disjoint(&available_event_ids) {
            bail!("局域网设备返回了冲突的文件接收计划");
        }
        Ok(LanContentPlan {
            planned_count,
            metadata_only_event_ids,
            available_event_ids,
        })
    }

    /// Flushes durable event receipts independently from sync pagination and transfer state.
    async fn flush_lan_receipts(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        device: &DeviceAnnouncement,
        peer_device_id: &str,
    ) -> Result<bool> {
        for _ in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            let receipts =
                repository::pending_peer_receipts(pool, peer_device_id, MAX_EVENTS_PER_BATCH)
                    .await?;
            if receipts.is_empty() {
                return Ok(false);
            }
            let complete_event_ids = receipts
                .iter()
                .filter(|receipt| receipt.content_complete)
                .map(|receipt| receipt.event_id.clone())
                .collect::<Vec<_>>();
            let event_ids = receipts
                .iter()
                .map(|receipt| receipt.event_id.clone())
                .collect::<Vec<_>>();
            let response = call_lan(
                connection,
                Request::AcknowledgeLan {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    device: device.clone(),
                    event_ids: event_ids.clone(),
                    complete_event_ids,
                },
            )
            .await?;
            match response {
                Response::LanAcknowledged => {
                    repository::complete_peer_receipts(pool, peer_device_id, &receipts).await?;
                }
                Response::Error { code, message } => {
                    return Err(RemoteSyncError { code, message }.into());
                }
                _ => bail!("局域网设备返回了无效的投递回执响应"),
            }
        }

        Ok(!repository::pending_peer_receipts(pool, peer_device_id, 1)
            .await?
            .is_empty())
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
            match call_lan(connection, Request::Health).await? {
                Response::Health {
                    protocol_version, ..
                } if protocol_version == PROTOCOL_VERSION => {}
                Response::Health {
                    protocol_version, ..
                } => bail!(
                    "局域网同步协议版本不一致：本机 {PROTOCOL_VERSION}，对端 {protocol_version}，请同时升级两台设备"
                ),
                Response::Error { code, message } => {
                    return Err(RemoteSyncError { code, message }.into());
                }
                _ => bail!("局域网设备返回了无效的协议版本响应"),
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
        let availability_has_more = if is_cloud {
            self.sync_cloud_availability(connection, &pool, group)
                .await?
        } else {
            false
        };
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
        let mut follow_up_needed = availability_has_more;
        let mut has_more = false;
        if let Some(peer_device_id) = target.peer_device_id() {
            follow_up_needed |= self
                .flush_lan_receipts(connection, &pool, group, &cycle.device, peer_device_id)
                .await?;
        }
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
                repository::pending_cloud_events_for_target(
                    &pool,
                    target_id,
                    device_id,
                    MAX_EVENTS_PER_BATCH,
                )
                .await?
            } else {
                repository::pending_peer_events_for_target(
                    &pool,
                    target_id,
                    target
                        .peer_device_id()
                        .context("missing LAN peer device identity")?,
                    MAX_EVENTS_PER_BATCH,
                )
                .await?
            };
            let mut more_available = pending.len() == usize::from(MAX_EVENTS_PER_BATCH);
            let key = group.content_key_bytes()?;
            let mut validated = Vec::with_capacity(pending.len());
            for stored in pending {
                let validation = crypto::decrypt_sync_event(&key, &stored.event)
                    .and_then(|event| validate_decrypted_sync_event(&event));
                if let Err(error) = validation {
                    log::warn!(
                        "skip invalid outgoing card: eventId={} error={error:#}",
                        stored.event.event_id
                    );
                    self.delivery
                        .blocked(
                            &pool,
                            target_id,
                            std::slice::from_ref(&stored.event.event_id),
                            &error.to_string(),
                        )
                        .await?;
                    self.emit_updated();
                    continue;
                }
                validated.push(stored);
            }
            pending = validated;
            if is_cloud {
                let key = group.content_key_bytes()?;
                for stored in &mut pending {
                    stored.event.retention_key =
                        Some(crypto::retention_mapping(&key, &stored.event)?.item_key);
                }
            }
            let (mut metadata_only_event_ids, mut available_event_ids) = if is_cloud
                || pending.is_empty()
            {
                if is_cloud {
                    let forced_event_ids = repository::event_ids_with_requested_content(
                        &pool, &pending, target_id, "cloud",
                    )
                    .await?;
                    let mut metadata_only_event_ids = deferred_cloud_file_events(
                        &pending,
                        &group.content_key_bytes()?,
                        cycle.settings.auto_upload_max_mb,
                        &forced_event_ids,
                    )?;
                    let routed_away =
                        repository::event_ids_routed_away_from(&pool, &pending, "cloud").await?;
                    metadata_only_event_ids
                        .extend(routed_away.difference(&forced_event_ids).cloned());
                    (metadata_only_event_ids, HashSet::new())
                } else {
                    (HashSet::new(), HashSet::new())
                }
            } else {
                let original_count = pending.len();
                let mut plan = loop {
                    match self
                        .plan_lan_content(connection, group, device_id, &pending)
                        .await
                    {
                        Ok(plan) => break plan,
                        Err(error) => {
                            let Some(card) = error.downcast_ref::<OutgoingCardError>() else {
                                return Err(error);
                            };
                            self.delivery
                                .blocked(
                                    &pool,
                                    target_id,
                                    std::slice::from_ref(&card.event_id),
                                    &card.to_string(),
                                )
                                .await?;
                            pending.retain(|stored| stored.event.event_id != card.event_id);
                            self.emit_updated();
                        }
                    }
                };
                pending.truncate(plan.planned_count);
                more_available |= plan.planned_count < original_count;
                let routed_away =
                    repository::event_ids_routed_away_from(&pool, &pending, "lan").await?;
                let requested =
                    repository::event_ids_with_requested_content(&pool, &pending, target_id, "lan")
                        .await?;
                plan.metadata_only_event_ids
                    .extend(routed_away.difference(&requested).cloned());
                (plan.metadata_only_event_ids, plan.available_event_ids)
            };
            repository::attach_event_blob_ids(&pool, &mut pending).await?;
            mark_metadata_only_blob_refs(
                &mut pending,
                &group.content_key_bytes()?,
                &metadata_only_event_ids,
            );
            if is_cloud {
                mark_optional_source_icon_refs(&mut pending, &group.content_key_bytes()?);
            }
            let route = if is_cloud {
                super::super::progress::TransferRoute::Cloud
            } else {
                super::super::progress::TransferRoute::Lan
            };
            self.delivery
                .filter_pending(&pool, target_id, &mut pending, &mut metadata_only_event_ids)
                .await?;
            let remaining_event_ids = pending
                .iter()
                .map(|stored| stored.event.event_id.as_str())
                .collect::<HashSet<_>>();
            available_event_ids.retain(|event_id| remaining_event_ids.contains(event_id.as_str()));
            let ids = pending
                .iter()
                .map(|stored| stored.event.event_id.clone())
                .collect::<Vec<_>>();
            self.delivery
                .queued(&pool, target_id, &ids, &metadata_only_event_ids)
                .await?;
            let mut content_event_ids = HashSet::new();
            for stored in &pending {
                if let DecryptedSyncEvent::Clipboard(envelope) =
                    crypto::decrypt_sync_event(&key, &stored.event)?
                {
                    if !envelope.blobs.is_empty() {
                        content_event_ids.insert(stored.event.event_id.clone());
                    }
                }
            }
            let missing_records =
                repository::event_ids_without_record(&pool, &pending, target_id).await?;
            let mut records = pending
                .iter()
                .filter(|stored| {
                    !content_event_ids.contains(&stored.event.event_id)
                        || missing_records.contains(&stored.event.event_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            has_more = more_available || local_materialized;
            let mut records_exchanged = false;
            while !records.is_empty() {
                let mut ready = records.clone();
                mark_metadata_only_blob_refs(&mut ready, &key, &content_event_ids);
                match self
                    .exchange_ready_events(&context, cursor, &mut ready, &content_event_ids)
                    .await
                {
                    Ok(outcome) => {
                        records_exchanged = true;
                        cursor = outcome.latest_cursor;
                        has_more |= outcome.has_more;
                        if outcome.event_ids.is_empty() {
                            return Ok(SyncConnectionOutcome { has_more: true });
                        }
                        records
                            .retain(|stored| !outcome.event_ids.contains(&stored.event.event_id));
                    }
                    Err(error) => {
                        if let Some(card) = error.downcast_ref::<OutgoingCardError>() {
                            self.delivery
                                .blocked(
                                    &pool,
                                    target_id,
                                    std::slice::from_ref(&card.event_id),
                                    &card.to_string(),
                                )
                                .await?;
                            records.retain(|stored| stored.event.event_id != card.event_id);
                            pending.retain(|stored| stored.event.event_id != card.event_id);
                            self.emit_updated();
                            continue;
                        }
                        let submitted = ready
                            .iter()
                            .map(|stored| stored.event.event_id.clone())
                            .collect::<Vec<_>>();
                        self.delivery
                            .failed(&pool, target_id, &submitted, &error.to_string())
                            .await?;
                        self.emit_updated();
                        return Err(error);
                    }
                }
            }
            self.delivery.complete_metadata_only(
                &metadata_only_event_ids.iter().cloned().collect::<Vec<_>>(),
                route,
                target_id,
            );
            pending.retain(|stored| {
                content_event_ids.contains(&stored.event.event_id)
                    && !metadata_only_event_ids.contains(&stored.event.event_id)
            });
            if pending.is_empty() {
                if !records_exchanged {
                    let outcome = self
                        .exchange_ready_events(&context, cursor, &mut Vec::new(), &HashSet::new())
                        .await?;
                    cursor = outcome.latest_cursor;
                    has_more |= outcome.has_more;
                }
                if has_more {
                    continue;
                }
                break;
            }
            let ids = pending
                .iter()
                .map(|stored| stored.event.event_id.clone())
                .collect::<Vec<_>>();
            let blobs = repository::blobs_for_events(&pool, &ids).await?;
            let skip_content_event_ids = metadata_only_event_ids
                .union(&available_event_ids)
                .cloned()
                .collect::<HashSet<_>>();
            let mut queue = super::super::transfer::TransferQueue::new(
                pending,
                blobs,
                &group.content_key_bytes()?,
                &skip_content_event_ids,
            )?;
            let mut uploads = tokio::task::JoinSet::new();
            let (upload_started, mut upload_notifications) = mpsc::unbounded_channel();
            let mut failure = None;
            let mut blocked_event_ids = HashSet::new();
            for (id, reason) in queue.take_failures() {
                log::warn!("skip undeliverable sync event: eventId={id} error={reason}");
                self.delivery
                    .blocked(&pool, target_id, std::slice::from_ref(&id), &reason)
                    .await?;
                blocked_event_ids.insert(id);
            }
            if !blocked_event_ids.is_empty() {
                queue.cancel_events(&blocked_event_ids);
                self.emit_updated();
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
                        let Some((event_id, operation_total_bytes)) = queue
                            .operation_for_blob(&blob.blob_id)
                            .map(|(event_id, total_bytes)| (event_id.to_owned(), total_bytes))
                        else {
                            break None;
                        };
                        match self
                            .delivery
                            .start_upload(
                                &pool,
                                target_id,
                                route,
                                &event_id,
                                operation_total_bytes,
                                &blob,
                            )
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
                        managed_hub_blob: is_cloud,
                        is_cloud,
                        language: crate::i18n::current_language(&self.app),
                        started: Some(upload_started.clone()),
                        transfer,
                    };
                    uploads.spawn(async move {
                        let transfer = options.transfer.clone();
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
                        if let (Err(error), Some(transfer)) = (&result, &transfer) {
                            if transfer.canceled() {
                                transfer.mark_canceled(0);
                            } else {
                                transfer.failed(0, error.to_string());
                            }
                        }
                        (blob_id, result)
                    });
                }
                self.emit_updated();
                let mut ready = self
                    .delivery
                    .prepare_ready_events(&pool, target_id, &mut queue)
                    .await?;
                if ready.is_empty() && !pull_needed {
                    let result = tokio::select! {
                        Some(blob_id) = upload_notifications.recv() => {
                            self.delivery
                                .attempted(
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
                            if is_shared_sync_failure(connection, &error) {
                                let message = error.to_string();
                                self.delivery
                                    .failed(&pool, target_id, &ids, &message)
                                    .await?;
                                self.delivery
                                    .failed(&pool, target_id, &queue.active_events(), &message)
                                    .await?;
                                failure.get_or_insert(error);
                                break;
                            }
                            self.delivery
                                .blocked(&pool, target_id, &ids, &error.to_string())
                                .await?;
                            queue.cancel_events(&ids.into_iter().collect());
                            self.emit_updated();
                        }
                    }
                    continue;
                }
                pull_needed = false;
                match self
                    .exchange_ready_events(&context, cursor, &mut ready, &metadata_only_event_ids)
                    .await
                {
                    Ok(outcome) => {
                        queue.confirm_events(&outcome.event_ids);
                        cursor = outcome.latest_cursor;
                        has_more = outcome.has_more || local_materialized || more_available;
                    }
                    Err(error) => {
                        if let Some(card) = error.downcast_ref::<OutgoingCardError>() {
                            self.delivery
                                .blocked(
                                    &pool,
                                    target_id,
                                    std::slice::from_ref(&card.event_id),
                                    &card.to_string(),
                                )
                                .await?;
                            queue.cancel_events(&HashSet::from([card.event_id.clone()]));
                            self.emit_updated();
                            continue;
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
        }
        has_more |= follow_up_needed;
        self.runtime_state.source_asset_wake.notify_one();
        Ok(SyncConnectionOutcome { has_more })
    }

    /// Publishes records or ready-content confirmations and applies their acknowledgement.
    async fn exchange_ready_events(
        self: &Arc<Self>,
        context: &SyncExchange<'_>,
        after_cursor: u64,
        pending: &mut Vec<StoredSyncEvent>,
        metadata_only_event_ids: &HashSet<String>,
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
        let removed_devices = if is_cloud {
            repository::removed_devices(&pool).await?
        } else {
            self.sync_lan_removed_devices(connection, &pool, group, device_id, &device.endpoint_id)
                .await?;
            Vec::new()
        };
        let batch_started_at = Instant::now();
        let available_count = pending.len();
        let outgoing_events = pending.iter().map(|item| item.event.clone()).collect();
        // Reserve the final continuation value before selecting the fitting event prefix.
        let mut request = if is_cloud {
            Request::CloudSync {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: self.cloud_device_announcement_for(device),
                after_cursor,
                events: outgoing_events,
                removed_devices: removed_devices.clone(),
                limit: MAX_EVENTS_PER_BATCH,
                bounded_hub: true,
            }
        } else {
            Request::Sync {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: device.clone(),
                after_cursor,
                events: outgoing_events,
                limit: MAX_EVENTS_PER_BATCH,
                events_complete: false,
            }
        };
        let pending_count = match limit_sync_request(&mut request) {
            Ok(count) => count,
            Err(error) => {
                pending.truncate(1);
                if let Some(stored) = pending.first() {
                    return Err(OutgoingCardError {
                        event_id: stored.event.event_id.clone(),
                        source: error.into(),
                    }
                    .into());
                }
                return Err(error.into());
            }
        };
        let outgoing_has_more = pending_count < available_count;
        if let Request::Sync {
            events_complete, ..
        } = &mut request
        {
            *events_complete = !outgoing_has_more;
        }
        pending.truncate(pending_count);
        let event_ids = pending
            .iter()
            .map(|item| item.event.event_id.clone())
            .collect::<Vec<_>>();
        let source_icon_blob_ids = source_icon_blob_ids(pending, &group.content_key_bytes()?);
        let blob_started_at = Instant::now();
        let blobs = repository::blobs_for_events(&pool, &event_ids).await?;
        let (required_blobs, source_icon_blobs) =
            split_source_icon_blobs(blobs, &source_icon_blob_ids);
        let required_blob_count = required_blobs.len();
        let source_icon_blob_count = source_icon_blobs.len();
        let blob_elapsed = blob_started_at.elapsed();
        let mut deferred_source_icon_blobs = source_icon_blobs;
        let mut queued_source_icon_count = 0;
        if !is_cloud {
            if let Err(error) = upload_blobs(
                self.clone(),
                connection,
                group,
                std::mem::take(&mut deferred_source_icon_blobs),
                BlobUploadOptions {
                    kind: BlobUploadKind::SourceIcon,
                    managed_hub_blob: false,
                    is_cloud,
                    language,
                    started: None,
                    transfer: None,
                },
            )
            .await
            {
                log::warn!("publish optional source app icons failed: {error}");
            }
        }
        let request_started_at = Instant::now();
        let result = async {
            let protocol = if is_cloud { "cloud" } else { "lan" };
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
                    events: events.into_iter().map(ReceivedSyncEvent::from).collect(),
                    peers,
                    latest_cursor,
                    removed_devices: Vec::new(),
                    has_more,
                },
                Response::CloudSynced {
                    events,
                    peers,
                    latest_cursor,
                    removed_devices,
                    has_more,
                    ..
                } => SyncBatchResponse {
                    events: events.into_iter().map(ReceivedSyncEvent::from).collect(),
                    peers,
                    latest_cursor,
                    removed_devices,
                    has_more,
                },
                Response::Error {
                    code: ErrorCode::HistoryGap,
                    ..
                } if is_cloud => return Err(anyhow::Error::new(CloudHistoryGap)),
                Response::Error { code, message } => {
                    return Err(RemoteSyncError { code, message }.into());
                }
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
                if is_cloud && should_invalidate_cloud_connection(connection, &error) {
                    self.invalidate_cloud_connection(connection.stable_id());
                    self.runtime_state.watch_wake.notify_one();
                }
                return Err(error);
            }
        };
        let request_elapsed = request_started_at.elapsed();
        let received_count = response.events.len();
        let incoming_has_more = response.has_more;
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
        if is_cloud && !deferred_source_icon_blobs.is_empty() {
            repository::enqueue_source_icon_uploads(
                &pool,
                &group.group_id,
                &deferred_source_icon_blobs,
            )
            .await?;
        }
        self.delivery
            .completed(&pool, target_id, &event_ids, metadata_only_event_ids)
            .await?;
        self.release_staging_for_events(&pool, &event_ids).await?;
        for peer in response.peers {
            if peer.device_id != device_id {
                repository::upsert_peer_identity(&pool, &peer).await?;
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
        if let Some(peer_device_id) = target.peer_device_id() {
            has_more |= self
                .flush_lan_receipts(connection, &pool, group, device, peer_device_id)
                .await?;
        }
        if !event_ids.is_empty() || received_count > 0 {
            self.emit_updated();
        }
        let apply_elapsed = apply_started_at.elapsed();
        if is_cloud {
            queued_source_icon_count = deferred_source_icon_blobs.len();
            if queued_source_icon_count > 0 {
                self.runtime_state.source_asset_wake.notify_one();
            }
        }
        if is_cloud {
            log::info!(
                "cloud sync batch completed: protocol={protocol} sentEvents={pending_count} receivedEvents={received_count} removals={removed_count} requiredBlobs={required_blob_count} sourceIconBlobs={source_icon_blob_count} queuedSourceIcons={queued_source_icon_count} blobMs={} requestMs={} applyMs={} totalMs={}",
                blob_elapsed.as_millis(),
                request_elapsed.as_millis(),
                apply_elapsed.as_millis(),
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
