use ecopaste_sync_protocol::{CloudBlobCheck, LanContentLevel, LanItemAck, MAX_ITEMS_PER_BATCH};

use super::*;

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct OversizedOutgoingItem {
    item_key: String,
    source: anyhow::Error,
}

impl SyncManager {
    pub(super) async fn start_preparation_transfer(
        &self,
        pool: &SqlitePool,
        operation: &db::sync_items::PendingContentPrep,
        operation_id: &str,
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
            && super::super::delivery_plan::resolve_cloud_blob_policy(
                true,
                total_bytes,
                auto_upload_max_mb,
                operation.cloud_blob_policy,
                operation.force_files,
            ) == db::sync_items::CloudBlobPolicy::Defer
        {
            return Ok(None);
        }
        if target.route == super::super::progress::TransferRoute::Lan {
            return Ok(None);
        }
        let task_key = format!("{operation_id}\0prepare");
        let task_id = blake3::hash(task_key.as_bytes()).to_hex().to_string();
        Ok(Some(self.delivery.start_preparation(
            super::super::progress::TransferTask {
                id: task_id,
                revision: 0,
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                operation_id: operation_id.to_owned(),
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
            operation.local_seq,
            item.source_revision.clone(),
        )))
    }

    /// Sends asynchronous LAN content completion acknowledgements over an active peer connection.
    pub(super) async fn send_lan_acks(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        acks: Vec<LanItemAck>,
    ) -> Result<()> {
        if acks.is_empty() {
            return Ok(());
        }
        let device = self.receipt_device_announcement()?;
        for chunk in acks.chunks(usize::from(MAX_ITEMS_PER_BATCH)) {
            let response = call_lan(
                connection,
                Request::AckLanItems {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    device: device.clone(),
                    acks: chunk.to_vec(),
                },
            )
            .await?;
            match response {
                Response::LanAcknowledged => {}
                Response::Error { code, message } => {
                    return Err(RemoteSyncError { code, message }.into());
                }
                _ => bail!("局域网设备返回了无效的投递回执响应"),
            }
        }
        Ok(())
    }

    async fn prepare_realtime_outbound_lanes(
        &self,
        target: &PreparationTarget,
        cycle: &SyncCycleContext,
    ) -> Result<bool> {
        let _ = self.materialize_dirty_states(Some(cycle)).await?;
        let text_run = self
            .materialize_content_lane(
                db::sync_items::OutboxContentLane::Text,
                db::sync_items::OutboxContentScope::RealtimeOnly,
                usize::from(MAX_ITEMS_PER_BATCH),
                Some(target),
                Some(cycle),
            )
            .await?;
        let mut local_has_more = text_run.hit_limit;
        text_run.staging.retain_for_delivery();

        let image_run = self
            .materialize_content_lane(
                db::sync_items::OutboxContentLane::Image,
                db::sync_items::OutboxContentScope::RealtimeOnly,
                4,
                Some(target),
                Some(cycle),
            )
            .await?;
        local_has_more |= image_run.hit_limit;
        image_run.staging.retain_for_delivery();
        Ok(local_has_more)
    }

    async fn block_oversized_item_key(
        &self,
        pool: &SqlitePool,
        target_id: &str,
        item_key: &str,
        error: &str,
    ) -> Result<()> {
        if let Some(meta) = repository::load_sync_item_meta_by_item_key(pool, item_key).await? {
            self.delivery
                .blocked(pool, target_id, &meta.sync_key, error)
                .await?;
            self.emit_updated();
        }
        Ok(())
    }

    async fn mark_delivery_error_by_item_keys(
        &self,
        pool: &SqlitePool,
        target_id: &str,
        item_keys: &[String],
        error: &str,
    ) -> Result<()> {
        let mut sync_keys = Vec::with_capacity(item_keys.len());
        for item_key in item_keys {
            if let Some(meta) = repository::load_sync_item_meta_by_item_key(pool, item_key).await? {
                sync_keys.push(meta.sync_key);
            }
        }
        if !sync_keys.is_empty() {
            self.delivery
                .failed(pool, target_id, &sync_keys, error)
                .await?;
        }
        Ok(())
    }

    /// Runs the Cloud Hub control-plane sync loop (`CloudSyncItems`) over the dedicated control
    /// connection, never blocking on attachment bytes.
    pub(super) async fn sync_cloud_control(
        self: &Arc<Self>,
        connection: &Connection,
        cycle: &SyncCycleContext,
    ) -> Result<SyncConnectionOutcome> {
        let _sync_activity = SyncActivityGuard::acquire();
        let pool = cycle.pool.clone();
        let group = &cycle.group;
        let language = crate::i18n::current_language(&self.app);
        let prep_target = PreparationTarget {
            route: super::super::progress::TransferRoute::Cloud,
        };

        let mut local_has_more = self
            .prepare_realtime_outbound_lanes(&prep_target, cycle)
            .await?;
        let observed = self
            .runtime_state
            .cloud_observed_latest_seq
            .load(Ordering::Acquire);
        let mut remote_latest_hint: Option<u64> = (observed > 0).then_some(observed);
        let mut has_more = false;
        let mut active_connection = connection.clone();
        let exchange_lock = self
            .runtime_state
            .exchange_locks
            .lock()
            .await
            .entry(CLOUD_TARGET.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        for batch_index in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            if active_connection.close_reason().is_some() {
                if let Some(replacement) = self
                    .cached_cloud_connection()
                    .filter(|candidate| self.cloud_group_is_ready(candidate, &group.group_id))
                {
                    active_connection = replacement;
                } else if is_locally_closed_cloud_connection(&active_connection) {
                    return Err(CloudConnectionSuperseded.into());
                }
            }
            if batch_index > 0
                && db::sync_items::has_pending_realtime_outbox(&pool, cycle.settings.sync_sensitive)
                    .await?
            {
                local_has_more |= self
                    .prepare_realtime_outbound_lanes(&prep_target, cycle)
                    .await?;
            }
            let observed = self
                .runtime_state
                .cloud_observed_latest_seq
                .load(Ordering::Acquire);
            if observed > 0 {
                remote_latest_hint =
                    Some(remote_latest_hint.map_or(observed, |prev| prev.max(observed)));
            }

            let exchange_guard = exchange_lock.lock().await;
            let mut window = repository::cloud_window_cursor(&pool).await?;
            let (after_seq, before_seq) = window.next_pull_params(remote_latest_hint);
            let outbound_batch =
                repository::pending_outbound_writes(&pool, CLOUD_TARGET, MAX_ITEMS_PER_BATCH)
                    .await?;
            let writes = outbound_batch.writes;
            let has_more_writes = outbound_batch.has_more;
            let available_write_count = writes.len();
            let remote_ahead = remote_latest_hint.is_some_and(|latest| latest > window.synced_seq);
            if batch_index > 0
                && writes.is_empty()
                && !window.has_catchup_pending()
                && !remote_ahead
                && !local_has_more
            {
                has_more = false;
                break;
            }

            let removed_devices = if batch_index == 0 {
                repository::removed_devices(&pool).await?
            } else {
                Vec::new()
            };
            let first_item_key = writes.first().map(|w| w.item_key.clone());
            let mut request = Request::CloudSyncItems {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: self.cloud_device_announcement_for(&cycle.device),
                after_seq,
                before_seq,
                items: writes,
                removed_devices,
                limit: MAX_ITEMS_PER_BATCH,
            };
            let submitted_count = match limit_sync_request(&mut request) {
                Ok(count) => count,
                Err(error) => {
                    drop(exchange_guard);
                    if let Some(item_key) = first_item_key {
                        let err = OversizedOutgoingItem {
                            item_key: item_key.clone(),
                            source: error.into(),
                        };
                        self.block_oversized_item_key(
                            &pool,
                            CLOUD_TARGET,
                            &err.item_key,
                            &err.to_string(),
                        )
                        .await?;
                        continue;
                    }
                    return Err(error.into());
                }
            };
            let outgoing_truncated = submitted_count < available_write_count;
            let pushed_local_seqs: HashMap<String, u64> = match &request {
                Request::CloudSyncItems { items, .. } => items
                    .iter()
                    .map(|item| (item.item_key.clone(), item.local_seq))
                    .collect(),
                _ => HashMap::new(),
            };
            let submitted_keys = pushed_local_seqs.keys().cloned().collect::<Vec<_>>();

            let started_at = Instant::now();
            let response = match call_cloud(&active_connection, request, language).await {
                Ok(response) => response,
                Err(error) => {
                    drop(exchange_guard);
                    if is_locally_closed_cloud_connection(&active_connection) {
                        return Err(CloudConnectionSuperseded.into());
                    }
                    let _ = self
                        .mark_delivery_error_by_item_keys(
                            &pool,
                            CLOUD_TARGET,
                            &submitted_keys,
                            &error.to_string(),
                        )
                        .await;
                    if should_invalidate_cloud_connection(&active_connection, &error) {
                        self.invalidate_cloud_connection(active_connection.stable_id());
                        self.runtime_state.watch_wake.notify_one();
                    }
                    return Err(error);
                }
            };

            let Response::CloudSynced {
                accepted,
                conflicts,
                items,
                peers,
                removed_devices,
                latest_seq,
                pruned_through_seq,
                next_before_seq,
            } = response
            else {
                drop(exchange_guard);
                if let Response::Error { code, message } = response {
                    let err = RemoteSyncError { code, message };
                    let _ = self
                        .mark_delivery_error_by_item_keys(
                            &pool,
                            CLOUD_TARGET,
                            &submitted_keys,
                            &err.to_string(),
                        )
                        .await;
                    return Err(err.into());
                }
                bail!("云端返回了无效的同步响应");
            };

            repository::record_cloud_accepted(&pool, &accepted, &pushed_local_seqs).await?;
            if !accepted.is_empty() {
                self.notify_local_outbox_backfill();
            }
            if !removed_devices.is_empty() {
                self.apply_removed_devices(
                    &pool,
                    removed_devices,
                    &cycle.device_id,
                    &cycle.device.endpoint_id,
                )
                .await?;
            }
            for peer in peers {
                if peer.device_id != cycle.device_id {
                    repository::upsert_peer_identity(&pool, &peer).await?;
                }
            }

            let conflict_summary = self
                .ingest_remote_records(&pool, group, CLOUD_TARGET, &conflicts, true)
                .await?;
            let pull_summary = self
                .ingest_remote_records(&pool, group, CLOUD_TARGET, &items, true)
                .await?;
            window.apply_response(
                after_seq,
                before_seq,
                latest_seq,
                pruned_through_seq,
                next_before_seq,
            );
            let pull_more = window.has_catchup_pending() || latest_seq > window.synced_seq;
            repository::update_cloud_window_cursor(&pool, window).await?;
            remote_latest_hint = Some(latest_seq);
            drop(exchange_guard);

            let pending_inbound_blobs = self
                .apply_pending_inline_items(&pool, group, &cycle.settings)
                .await?;
            let has_pending_uploads = !repository::pending_cloud_blob_uploads(&pool, false, 1)
                .await?
                .is_empty();
            if pending_inbound_blobs
                || conflict_summary.needs_blob_pull
                || pull_summary.needs_blob_pull
                || has_pending_uploads
            {
                self.wake_attachments();
            }
            if cycle.settings.lan_enabled && (!items.is_empty() || !conflicts.is_empty()) {
                self.wake_lan_transfer();
            }
            if !submitted_keys.is_empty() || !items.is_empty() || !conflicts.is_empty() {
                self.emit_updated();
            }

            log::info!(
                "cloud sync batch completed: sent={} accepted={} conflicts={} received={} latestSeq={latest_seq} pullMore={pull_more} elapsedMs={}",
                submitted_count,
                accepted.len(),
                conflicts.len(),
                items.len(),
                started_at.elapsed().as_millis(),
            );

            let needs_repush = conflict_summary.needs_repush || pull_summary.needs_repush;
            has_more = outgoing_truncated
                || has_more_writes
                || pull_more
                || needs_repush
                || local_has_more;
            local_has_more = false;
            if !has_more {
                break;
            }
        }

        if !has_more {
            match self
                .verify_existing_cloud_blobs(
                    &active_connection,
                    &pool,
                    group,
                    &cycle.settings,
                    language,
                )
                .await
            {
                Ok(needs_control_push) => {
                    has_more = needs_control_push;
                }
                Err(error) => {
                    if is_locally_closed_cloud_connection(&active_connection) {
                        return Err(CloudConnectionSuperseded.into());
                    }
                    if should_invalidate_cloud_connection(&active_connection, &error) {
                        self.invalidate_cloud_connection(active_connection.stable_id());
                        self.runtime_state.watch_wake.notify_one();
                    }
                    return Err(error);
                }
            }
        }

        self.runtime_state.source_asset_wake.notify_one();
        Ok(SyncConnectionOutcome { has_more })
    }

    /// Checks once per Cloud connection whether blobs belonging to deferred LAN-routed or
    /// metadata-only items already exist on the Cloud Hub, promoting matching items to green
    /// without re-uploading attachment bytes.
    async fn verify_existing_cloud_blobs(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        settings: &SyncSettings,
        language: Language,
    ) -> Result<bool> {
        let candidates = repository::pending_cloud_blob_verifications(
            pool,
            settings.sync_sensitive,
            MAX_ITEMS_PER_BATCH.saturating_mul(2),
        )
        .await?;
        if candidates.is_empty() {
            return Ok(false);
        }

        let connection_id = connection.stable_id();
        let mut to_check = Vec::new();
        let mut queued_blob_ids = HashSet::new();
        {
            let checked = self
                .runtime_state
                .checked_cloud_blobs
                .read()
                .expect("checked cloud blobs poisoned");
            for entry in candidates {
                if checked.contains(&(connection_id, entry.blob_id.clone()))
                    || !queued_blob_ids.insert(entry.blob_id.clone())
                {
                    continue;
                }
                to_check.push(CloudBlobCheck {
                    blob_id: entry.blob_id,
                    size: entry.size,
                });
            }
        }
        if to_check.is_empty() {
            return Ok(false);
        }

        let checked_keys: Vec<(usize, String)> = to_check
            .iter()
            .map(|entry| (connection_id, entry.blob_id.clone()))
            .collect();
        let mut available_blob_ids = Vec::new();
        for chunk in to_check.chunks(usize::from(MAX_ITEMS_PER_BATCH)) {
            let response = call_cloud(
                connection,
                Request::CheckCloudBlobs {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    blobs: chunk.to_vec(),
                },
                language,
            )
            .await?;
            let Response::CloudBlobsChecked {
                available_blob_ids: chunk_hits,
            } = response
            else {
                if let Response::Error { code, message } = response {
                    return Err(RemoteSyncError { code, message }.into());
                }
                bail!("云端返回了无效的附件校验响应");
            };
            available_blob_ids.extend(chunk_hits);
        }

        {
            let mut checked = self
                .runtime_state
                .checked_cloud_blobs
                .write()
                .expect("checked cloud blobs poisoned");
            checked.retain(|(id, _)| *id == connection_id);
            checked.extend(checked_keys);
        }

        let promotion =
            repository::promote_cloud_blobs_available(pool, &available_blob_ids).await?;
        if promotion.promoted_items > 0 {
            log::info!(
                "verified existing cloud blobs without re-upload: checked={} hits={} promotedItems={} needsControlPush={}",
                queued_blob_ids.len(),
                available_blob_ids.len(),
                promotion.promoted_items,
                promotion.needs_control_push,
            );
            self.emit_updated();
        }
        Ok(promotion.needs_control_push)
    }

    /// Runs the LAN peer control-plane sync loop (`SyncItems`) over a peer connection.
    pub(super) async fn sync_lan_control(
        self: &Arc<Self>,
        connection: &Connection,
        target: &SyncConnectionTarget,
        cycle: &SyncCycleContext,
    ) -> Result<SyncConnectionOutcome> {
        let _sync_activity = SyncActivityGuard::acquire();
        let pool = cycle.pool.clone();
        let group = &cycle.group;
        let peer_device_id = target
            .peer_device_id()
            .context("missing LAN peer device identity")?;
        let target_id = target.delivery_target();

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

        self.sync_lan_removed_devices(
            connection,
            &pool,
            group,
            &cycle.device_id,
            &cycle.device.endpoint_id,
        )
        .await?;

        let prep_target = PreparationTarget {
            route: super::super::progress::TransferRoute::Lan,
        };
        let mut local_has_more = self
            .prepare_realtime_outbound_lanes(&prep_target, cycle)
            .await?;
        let mut remote_latest_hint: Option<u64> = None;
        let mut has_more = false;
        let exchange_lock = self
            .runtime_state
            .exchange_locks
            .lock()
            .await
            .entry(target_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        for batch_index in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            if batch_index > 0
                && db::sync_items::has_pending_realtime_outbox(&pool, cycle.settings.sync_sensitive)
                    .await?
            {
                local_has_more |= self
                    .prepare_realtime_outbound_lanes(&prep_target, cycle)
                    .await?;
            }

            let exchange_guard = exchange_lock.lock().await;
            let mut window = repository::peer_window_cursor(&pool, peer_device_id).await?;
            let (after_seq, before_seq) = window.next_pull_params(remote_latest_hint);
            let outbound_batch =
                repository::pending_outbound_writes(&pool, target_id, MAX_ITEMS_PER_BATCH).await?;
            let writes = outbound_batch.writes;
            let has_more_writes = outbound_batch.has_more;
            let available_write_count = writes.len();
            let remote_ahead = remote_latest_hint.is_some_and(|latest| latest > window.synced_seq);
            if batch_index > 0
                && writes.is_empty()
                && !window.has_catchup_pending()
                && !remote_ahead
                && !local_has_more
            {
                has_more = false;
                break;
            }

            let first_item_key = writes.first().map(|w| w.item_key.clone());
            let mut request = Request::SyncItems {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                device: cycle.device.clone(),
                after_seq,
                before_seq,
                items: writes,
                limit: MAX_ITEMS_PER_BATCH,
            };
            let submitted_count = match limit_sync_request(&mut request) {
                Ok(count) => count,
                Err(error) => {
                    drop(exchange_guard);
                    if let Some(item_key) = first_item_key {
                        self.block_oversized_item_key(
                            &pool,
                            target_id,
                            &item_key,
                            &error.to_string(),
                        )
                        .await?;
                        continue;
                    }
                    return Err(error.into());
                }
            };
            let outgoing_truncated = submitted_count < available_write_count;
            let submitted_keys = match &request {
                Request::SyncItems { items, .. } => {
                    items.iter().map(|w| w.item_key.clone()).collect::<Vec<_>>()
                }
                _ => Vec::new(),
            };

            let response = match call_lan(connection, request).await {
                Ok(response) => response,
                Err(error) => {
                    drop(exchange_guard);
                    let _ = self
                        .mark_delivery_error_by_item_keys(
                            &pool,
                            target_id,
                            &submitted_keys,
                            &error.to_string(),
                        )
                        .await;
                    return Err(error);
                }
            };

            let Response::LanSynced {
                acks,
                items,
                latest_seq,
                next_before_seq,
            } = response
            else {
                drop(exchange_guard);
                if let Response::Error { code, message } = response {
                    let err = RemoteSyncError { code, message };
                    let _ = self
                        .mark_delivery_error_by_item_keys(
                            &pool,
                            target_id,
                            &submitted_keys,
                            &err.to_string(),
                        )
                        .await;
                    return Err(err.into());
                }
                bail!("局域网设备返回了无效的同步响应");
            };

            repository::record_lan_acks(&pool, target_id, &acks).await?;
            if !acks.is_empty() {
                self.notify_local_outbox_backfill();
            }
            let pull_summary = self
                .ingest_remote_records(&pool, group, target_id, &items, false)
                .await?;
            window.apply_response(after_seq, before_seq, latest_seq, 0, next_before_seq);
            let pull_more = window.has_catchup_pending() || latest_seq > window.synced_seq;
            repository::update_peer_window_cursor(&pool, peer_device_id, window).await?;
            remote_latest_hint = Some(latest_seq);
            drop(exchange_guard);

            let pending_inbound_blobs = self
                .apply_pending_inline_items(&pool, group, &cycle.settings)
                .await?;
            // Acknowledge pulled items immediately over the open control link so the peer's WiFi
            // status converges even when it didn't initiate a reciprocal push.
            if !items.is_empty() {
                let mut pulled_acks = Vec::with_capacity(items.len());
                for record in &items {
                    let level = if record.deleted || record.blob_ids.is_empty() {
                        LanContentLevel::Complete
                    } else {
                        let meta =
                            repository::load_sync_item_meta_by_item_key(&pool, &record.item_key)
                                .await?;
                        if meta.as_ref().is_some_and(|row| {
                            matches!(row.local_state.as_str(), "managed" | "external")
                                && row.item_id.is_some()
                        }) {
                            LanContentLevel::Complete
                        } else {
                            LanContentLevel::Metadata
                        }
                    };
                    pulled_acks.push(LanItemAck {
                        item_key: record.item_key.clone(),
                        local_seq: record.seq,
                        level,
                    });
                }
                if let Err(error) = self.send_lan_acks(connection, group, pulled_acks).await {
                    log::debug!("send LAN pull acks to {peer_device_id} failed: {error:#}");
                }
            }
            if pending_inbound_blobs || pull_summary.needs_blob_pull {
                self.wake_attachments();
            }
            if !submitted_keys.is_empty() || !items.is_empty() || !acks.is_empty() {
                self.emit_updated();
            }

            has_more = outgoing_truncated
                || has_more_writes
                || pull_more
                || pull_summary.needs_repush
                || local_has_more;
            local_has_more = false;
            if !has_more {
                break;
            }
        }

        self.runtime_state.source_asset_wake.notify_one();
        Ok(SyncConnectionOutcome { has_more })
    }

    /// Uploads the single highest-priority pending local attachment item to Cloud Hub over
    /// `data_connection`. Returns `Ok(true)` when an item was processed so the worker re-queries
    /// fresh priority order, or `Ok(false)` when no eligible unclaimed item remains so the worker
    /// sleeps cleanly without busy-looping.
    pub(super) async fn upload_one_cloud_blob_item(
        self: &Arc<Self>,
        data_connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        realtime_only: bool,
    ) -> Result<bool> {
        let prep_target = PreparationTarget {
            route: super::super::progress::TransferRoute::Cloud,
        };
        let mut uploads = repository::pending_cloud_blob_uploads(pool, realtime_only, 4).await?;
        if uploads.is_empty() {
            let sync_sensitive = self
                .app
                .state::<SettingsStore>()
                .snapshot()
                .sync
                .sync_sensitive;
            let scope = if realtime_only {
                Some(db::sync_items::OutboxContentScope::RealtimeOnly)
            } else if !db::sync_items::has_pending_realtime_outbox(pool, sync_sensitive).await? {
                Some(db::sync_items::OutboxContentScope::BackfillOnly)
            } else {
                None
            };
            if let Some(scope) = scope {
                let image_run = self
                    .materialize_content_lane(
                        db::sync_items::OutboxContentLane::Image,
                        scope,
                        2,
                        Some(&prep_target),
                        None,
                    )
                    .await?;
                let prepared_any = image_run.processed > 0;
                image_run.staging.retain_for_delivery();
                if prepared_any {
                    self.runtime_state.cloud_transfer_wake.notify_one();
                    uploads =
                        repository::pending_cloud_blob_uploads(pool, realtime_only, 4).await?;
                }
            }
        }
        if uploads.is_empty() {
            return Ok(false);
        }

        let Some((upload, _claim)) = uploads.into_iter().find_map(|candidate| {
            self.delivery
                .claim_upload(CLOUD_TARGET, &candidate.sync_key)
                .map(|claim| (candidate, claim))
        }) else {
            return Ok(false);
        };

        let language = crate::i18n::current_language(&self.app);
        let is_files = upload.kind == "files";
        let is_realtime = !upload.history_backfill || upload.manual_share;
        let track_transfer = super::tracks_individual_transfer(
            &upload.kind,
            is_realtime,
            upload.manual_share,
            upload.payload_updated_at_ms,
            Utc::now().timestamp_millis(),
        );
        let operation_total_bytes = upload
            .blobs
            .iter()
            .fold(0_u64, |total, blob| total.saturating_add(blob.size));
        let stream_priority = if is_realtime {
            BLOB_PRIORITY_REALTIME
        } else {
            BLOB_PRIORITY_BACKFILL
        };

        let _cloud_data_activity = self.acquire_cloud_data_activity();
        let mut active_connection = data_connection.clone();
        let mut item_failed = false;
        let mut needs_control_push = false;
        for blob_row in &upload.blobs {
            let stored_blob = blob_row.to_stored_blob();
            let transfer = if track_transfer {
                match self
                    .delivery
                    .start_upload(
                        pool,
                        super::super::delivery::UploadStartRequest {
                            target: CLOUD_TARGET,
                            route: super::super::progress::TransferRoute::Cloud,
                            sync_key: &upload.sync_key,
                            item_id: upload.item_id.as_deref(),
                            operation_total_bytes,
                            blob: &stored_blob,
                        },
                    )
                    .await?
                {
                    UploadTransferStart::Ready(transfer) => transfer,
                    UploadTransferStart::Canceled => {
                        self.delivery
                            .cancel_upload_operation(&upload.sync_key, CLOUD_TARGET);
                        item_failed = true;
                        break;
                    }
                }
            } else {
                if repository::is_delivery_canceled(pool, CLOUD_TARGET, &upload.sync_key).await? {
                    self.delivery
                        .cancel_upload_operation(&upload.sync_key, CLOUD_TARGET);
                    item_failed = true;
                    break;
                }
                None
            };
            let options = BlobUploadOptions {
                kind: BlobUploadKind::Content,
                file_content: is_files,
                is_cloud: true,
                language,
                stream_priority,
                started: None,
                transfer: transfer.clone(),
            };
            let mut upload_started = false;
            let mut unhealthy_connection = false;
            let result = async {
                let (staged, _lease) =
                    match stored_blob.outgoing_source().ok_or(RemoteBlobUnavailable)? {
                        OutgoingBlobSource::DirectStream { .. } => (stored_blob.clone(), None),
                        OutgoingBlobSource::StagedLease => {
                            let (staged, lease) =
                                self.lease_outgoing_staging(group, &stored_blob).await?;
                            (staged, Some(lease))
                        }
                    };
                upload_started = true;
                let (first_result, first_unhealthy, first_written_bytes) =
                    upload_blob_attempt(&active_connection, group, &staged, options.clone(), false)
                        .await;
                let Err(first_error) = first_result else {
                    unhealthy_connection = first_unhealthy;
                    return Ok(());
                };
                let canceled = transfer
                    .as_ref()
                    .is_some_and(super::super::progress::TransferHandle::canceled);
                let connection_failure = first_unhealthy
                    || is_cloud_data_timeout(&first_error)
                    || should_invalidate_cloud_connection(&active_connection, &first_error);
                if !canceled
                    && connection_failure
                    && first_error
                        .downcast_ref::<RemoteBlobUnavailable>()
                        .is_none()
                {
                    self.invalidate_cloud_data_connection(active_connection.stable_id());
                    let settings = self.app.state::<SettingsStore>().snapshot().sync;
                    if let Ok(fresh_connection) = self.ensure_cloud_data_connection(&settings).await
                    {
                        active_connection = fresh_connection;
                        let (retry_result, retry_unhealthy, _) =
                            upload_blob_attempt(&active_connection, group, &staged, options, true)
                                .await;
                        unhealthy_connection = retry_unhealthy;
                        return retry_result;
                    }
                }
                if !canceled {
                    if let Some(handle) = &transfer {
                        handle.failed(first_written_bytes, first_error.to_string());
                    }
                }
                unhealthy_connection = first_unhealthy;
                Err(first_error)
            }
            .await;

            match result {
                Ok(()) => {
                    let outcome = repository::mark_blob_cloud_available(
                        pool,
                        &upload.sync_key,
                        &blob_row.blob_id,
                    )
                    .await?;
                    needs_control_push |= outcome.needs_control_push;
                    if !blob_row.encrypted_path.is_empty()
                        && blob_row.source_path.is_some()
                        && blob_row.stream_nonce.is_some()
                    {
                        self.delivery.request_staging_release(
                            &blob_row.blob_id,
                            PathBuf::from(&blob_row.encrypted_path),
                        );
                    }
                    if outcome.completed {
                        self.emit_updated();
                    }
                }
                Err(error) => {
                    if !upload_started {
                        if let Some(handle) = &transfer {
                            if handle.canceled() {
                                handle.mark_canceled(0);
                            } else {
                                handle.failed(0, error.to_string());
                            }
                        }
                    }
                    if error.downcast_ref::<RemoteBlobUnavailable>().is_some() {
                        let _ = repository::mark_blob_local_unavailable(
                            pool,
                            &upload.sync_key,
                            &blob_row.blob_id,
                        )
                        .await;
                        self.delivery
                            .blocked(pool, CLOUD_TARGET, &upload.sync_key, &error.to_string())
                            .await?;
                    } else {
                        self.delivery
                            .failed(
                                pool,
                                CLOUD_TARGET,
                                std::slice::from_ref(&upload.sync_key),
                                &error.to_string(),
                            )
                            .await?;
                        self.emit_updated();
                        if unhealthy_connection
                            || is_cloud_data_timeout(&error)
                            || should_invalidate_cloud_connection(&active_connection, &error)
                        {
                            self.invalidate_cloud_data_connection(active_connection.stable_id());
                        }
                        return Err(error);
                    }
                    self.emit_updated();
                    item_failed = true;
                    break;
                }
            }
        }

        if !item_failed {
            if needs_control_push {
                self.runtime_state.cloud_transfer_wake.notify_one();
            }
            if upload.history_backfill {
                self.notify_local_outbox_backfill();
            }
        }

        Ok(true)
    }
}
