use ecopaste_sync_protocol::CloudAvailabilityUpdate;

use super::*;

pub(super) fn extract_event_metadata(
    events: &[EncryptedEvent],
    key: &[u8; 32],
) -> HashMap<String, (String, String, i64)> {
    let mut metadata = HashMap::with_capacity(events.len());
    for event in events {
        let entry = match crypto::decrypt_sync_event(key, event) {
            Ok(DecryptedSyncEvent::Clipboard(envelope)) => (
                envelope.item.kind,
                envelope.item.sync_key,
                envelope
                    .item
                    .updated_at_ms
                    .unwrap_or(envelope.item.created_at_ms),
            ),
            Ok(DecryptedSyncEvent::ItemState(patch)) => {
                ("state".to_owned(), patch.sync_key, event.created_at_ms)
            }
            Err(_) => continue,
        };
        metadata.insert(event.event_id.clone(), entry);
    }
    metadata
}

/// Enforces cumulative receiver-side 70% historical attachment admission lazily per batch.
pub(super) struct AttachmentQuotaInspector {
    app: AppHandle,
    target_bytes: u64,
    latest_local_updated_at: Option<chrono::DateTime<Utc>>,
    measured_base_bytes: Option<u64>,
    reserved_bytes: u64,
}

impl AttachmentQuotaInspector {
    pub(super) async fn for_history(
        app: &AppHandle,
        pool: &SqlitePool,
        history: &crate::settings::History,
    ) -> Result<Self> {
        let target_bytes = crate::clipboard::cleanup::history_target_bytes(history);
        let latest_local_updated_at = if target_bytes > 0 {
            repository::latest_clipboard_item_updated_at(pool).await?
        } else {
            None
        };
        Ok(Self {
            app: app.clone(),
            target_bytes,
            latest_local_updated_at,
            measured_base_bytes: None,
            reserved_bytes: 0,
        })
    }

    pub(super) fn observe_timestamp(&mut self, ts: chrono::DateTime<Utc>) {
        if self.target_bytes == 0 {
            return;
        }
        self.latest_local_updated_at =
            Some(self.latest_local_updated_at.map_or(ts, |cur| cur.max(ts)));
    }

    pub(super) async fn admit_or_reject_historical(
        &mut self,
        item_updated_at: chrono::DateTime<Utc>,
        attachment_bytes: u64,
    ) -> bool {
        if self.target_bytes == 0
            || !self
                .latest_local_updated_at
                .is_some_and(|latest| item_updated_at < latest)
        {
            return true;
        }
        let base = match self.measured_base_bytes {
            Some(base) => base,
            None => {
                let measured = crate::clipboard::cleanup::storage_bytes(&self.app)
                    .await
                    .unwrap_or(0);
                self.measured_base_bytes = Some(measured);
                measured
            }
        };
        if base
            .saturating_add(self.reserved_bytes)
            .saturating_add(attachment_bytes)
            > self.target_bytes
        {
            return false;
        }
        self.reserved_bytes = self.reserved_bytes.saturating_add(attachment_bytes);
        true
    }
}

impl SyncManager {
    /// Applies the Hub's independent, resumable blob-availability stream.
    pub(super) async fn sync_cloud_availability(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<bool> {
        let mut changed = false;
        for _ in 0..MAX_SYNC_BATCHES_PER_CONNECTION {
            let after_cursor = repository::cloud_availability_cursor(pool).await?;
            let response = call_cloud(
                connection,
                Request::SyncCloudAvailability {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    after_cursor,
                    limit: MAX_EVENTS_PER_BATCH,
                },
                crate::i18n::current_language(&self.app),
            )
            .await?;
            let Response::CloudAvailabilitySynced {
                updates,
                latest_cursor,
                has_more,
                pruned_availability_cursor,
            } = response
            else {
                if let Response::Error { message, .. } = response {
                    bail!(message);
                }
                bail!("云端返回了无效的内容可用性响应");
            };
            let key = group.content_key_bytes()?;
            changed |=
                apply_cloud_availability_updates(pool, &key, &updates, &[], latest_cursor).await?;
            let reconciled_cursor = repository::cloud_pruned_availability_cursor(pool).await?;
            if pruned_availability_cursor > reconciled_cursor {
                changed |= self
                    .reconcile_pruned_cloud_availability(
                        connection,
                        pool,
                        group,
                        latest_cursor,
                        pruned_availability_cursor,
                    )
                    .await?;
            }
            if !has_more {
                if changed {
                    self.wake_attachments();
                    self.emit_updated();
                }
                return Ok(false);
            }
        }
        if changed {
            self.wake_attachments();
            self.emit_updated();
        }
        Ok(true)
    }

    /// Revalidates retained cloud facts once per Hub cleanup, including equivalent older events.
    async fn reconcile_pruned_cloud_availability(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        availability_cursor: u64,
        pruned_availability_cursor: u64,
    ) -> Result<bool> {
        let key = group.content_key_bytes()?;
        let mut after_event_id = String::new();
        let mut changed = false;
        loop {
            let event_ids =
                repository::confirmed_cloud_event_ids(pool, &after_event_id, MAX_EVENTS_PER_BATCH)
                    .await?;
            let Some(last_event_id) = event_ids.last() else {
                break;
            };
            after_event_id = last_event_id.clone();
            let response = call_cloud(
                connection,
                Request::CheckCloudAvailability {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    event_ids,
                },
                crate::i18n::current_language(&self.app),
            )
            .await?;
            let Response::CloudAvailabilityChecked {
                updates,
                missing_event_ids,
            } = response
            else {
                if let Response::Error { message, .. } = response {
                    bail!(message);
                }
                bail!("云端返回了无效的内容可用性响应");
            };
            changed |= apply_cloud_availability_updates(
                pool,
                &key,
                &updates,
                &missing_event_ids,
                availability_cursor,
            )
            .await?;
        }
        repository::mark_cloud_pruning_reconciled(pool, pruned_availability_cursor).await?;
        Ok(changed)
    }

    /// Stores one authenticated remote batch and updates its continuation cursor atomically.
    pub(super) async fn store_remote_events(
        &self,
        pool: &SqlitePool,
        device_id: &str,
        events: Vec<ReceivedSyncEvent>,
        group: &GroupSecrets,
        source: &SyncConnectionTarget,
        latest_cursor: u64,
    ) -> Result<(bool, repository::StoreRemoteBatchResult)> {
        let _sync_activity = SyncActivityGuard::acquire();
        let removed_peers = repository::removed_peer_ids(pool).await?;
        let events = events
            .into_iter()
            .filter(|received| !removed_peers.contains(&received.event.origin_device_id))
            .collect::<Vec<_>>();
        let key = group.content_key_bytes()?;
        let cloud_availability = if source.is_cloud() {
            events
                .iter()
                .filter_map(|received| {
                    cloud_content_availability(received, &key)
                        .map(|state| (received.event.event_id.clone(), state))
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let received_content = !events.is_empty();
        let encrypted_events = events
            .into_iter()
            .map(|received| received.event)
            .collect::<Vec<_>>();
        let metadata_only_event_ids = metadata_only_content_event_ids(&encrypted_events, &key);
        let event_metadata = extract_event_metadata(&encrypted_events, &key);
        let stored = repository::store_remote_batch_with_content_policy(
            pool,
            source.remote_batch_source(),
            Some(latest_cursor),
            &encrypted_events,
            device_id,
            &cloud_availability,
            &metadata_only_event_ids,
            &event_metadata,
        )
        .await?;
        for (event_id, origin_device_id) in &stored.conflicts {
            log::warn!(
                "ignore sync event {event_id} with a reused origin sequence from {origin_device_id}"
            );
        }
        Ok((received_content, stored))
    }

    /// Applies pending events after storing a batch and wakes downstream attachment/LAN queues.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_stored_remote_events(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        sync_settings: &SyncSettings,
        source: &SyncConnectionTarget,
        received_content: bool,
        stored: &repository::StoreRemoteBatchResult,
        group: &GroupSecrets,
        allow_content: bool,
    ) -> Result<bool> {
        let content_has_more = self
            .apply_pending_events(
                connection,
                pool,
                group,
                sync_settings,
                source,
                allow_content,
            )
            .await?;
        if received_content {
            self.wake_attachments();
        }
        if stored.received_new_events && source.is_cloud() {
            self.wake_lan_transfer();
        }
        Ok(content_has_more)
    }

    /// Persists one card's retry without treating its application failure as a session failure.
    async fn defer_card_apply(&self, pool: &SqlitePool, event_id: &str, error: &str) -> Result<()> {
        log::warn!("defer synchronized card: eventId={event_id} error={error}");
        let delay = repository::mark_apply_failed(pool, event_id, error).await?;
        self.schedule_transfer_wake(delay);
        self.emit_updated();
        Ok(())
    }

    /// Replays state before content so a later tombstone suppresses content even across pages.
    pub(super) async fn apply_pending_events(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_settings: &SyncSettings,
        source: &SyncConnectionTarget,
        allow_content: bool,
    ) -> Result<bool> {
        self.apply_pending_events_lane(
            connection,
            pool,
            group,
            sync_settings,
            source,
            allow_content,
            false,
            false,
        )
        .await
    }

    pub(super) async fn apply_pending_attachment_events(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        settings: &SyncSettings,
        source: &SyncConnectionTarget,
        files: bool,
    ) -> Result<bool> {
        self.apply_pending_events_lane(connection, pool, group, settings, source, true, files, true)
            .await
    }

    /// Keeps large downloads off the lock used to apply text, images and card state.
    #[allow(clippy::too_many_arguments)]
    async fn apply_pending_events_lane(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_settings: &SyncSettings,
        source: &SyncConnectionTarget,
        allow_content: bool,
        files: bool,
        attachments: bool,
    ) -> Result<bool> {
        // 仅串行选取任务与写入元数据；附件认领后释放锁，网络 I/O 不阻塞记录同步。
        let mut apply_guard = Some(self.apply_lock.lock().await);
        let clipboard_generation = self
            .app
            .state::<Arc<ClipboardFingerprintState>>()
            .generation();
        let key = group.content_key_bytes()?;
        let mut source_assets_pending = false;
        let mut latest_clipboard_item = None;
        #[cfg(target_os = "android")]
        let mut applied_any = false;

        if !attachments {
            loop {
                let pending_states = repository::unapplied_events_by_kind(
                    pool,
                    SyncEventKind::ItemState,
                    MAX_EVENTS_PER_BATCH,
                )
                .await?;
                if pending_states.is_empty() {
                    break;
                }
                for stored in pending_states {
                    let event = stored.event;
                    let decrypted = crypto::decrypt_sync_event(&key, &event).and_then(|event| {
                        validate_decrypted_sync_event(&event)?;
                        Ok(event)
                    });
                    match decrypted {
                        Ok(DecryptedSyncEvent::ItemState(patch)) => {
                            match self.apply_item_state_event(pool, &event, patch).await {
                                Ok(()) => {
                                    #[cfg(target_os = "android")]
                                    {
                                        applied_any = true;
                                    }
                                }
                                Err(error) => {
                                    log::warn!(
                                        "defer synchronized state event {}: {error:#}",
                                        event.event_id
                                    );
                                    let delay = repository::mark_apply_failed(
                                        pool,
                                        &event.event_id,
                                        &error.to_string(),
                                    )
                                    .await?;
                                    self.schedule_transfer_wake(delay);
                                }
                            }
                        }
                        Ok(DecryptedSyncEvent::Clipboard(_)) => {
                            log::warn!(
                                "discard synchronized state event {} with content payload",
                                event.event_id
                            );
                            repository::mark_rejected(
                                pool,
                                &event.event_id,
                                "状态事件包含剪贴板内容载荷",
                            )
                            .await?;
                        }
                        Err(error) => {
                            log::warn!("discard invalid sync event {}: {error}", event.event_id);
                            repository::mark_rejected(pool, &event.event_id, &error.to_string())
                                .await?;
                        }
                    }
                }
            }
        }

        if !allow_content {
            drop(apply_guard.take());
            #[cfg(target_os = "android")]
            if applied_any {
                crate::commands::android::notify_overlay_clipboard_changed();
            }
            return Ok(false);
        }

        // Replaying still-unapplied content makes a download/apply interruption recoverable even
        // when the Hub no longer returns the duplicate event.
        let pending_apply = repository::unapplied_content_events_for_target(
            pool,
            source.delivery_target(),
            MAX_EVENTS_PER_BATCH,
            attachments,
            Some(files),
        )
        .await?;
        let complete_content_source_event_ids = repository::event_ids_with_complete_content_source(
            pool,
            &pending_apply,
            source.delivery_target(),
            source.is_cloud(),
        )
        .await?;
        let mut pending_content = Vec::new();
        let mut made_content_progress = false;
        for stored in pending_apply {
            let event = stored.event;
            let decrypted = crypto::decrypt_sync_event(&key, &event).and_then(|event| {
                validate_decrypted_sync_event(&event)?;
                Ok(event)
            });
            match decrypted {
                Ok(DecryptedSyncEvent::Clipboard(envelope)) => {
                    pending_content.push((event, *envelope));
                }
                Ok(DecryptedSyncEvent::ItemState(_)) => {
                    log::warn!(
                        "discard synchronized content event {} with state payload",
                        event.event_id
                    );
                    repository::mark_rejected(pool, &event.event_id, "内容事件包含状态载荷")
                        .await?;
                    made_content_progress = true;
                }
                Err(error) => {
                    log::warn!("discard invalid sync event {}: {error}", event.event_id);
                    repository::mark_rejected(pool, &event.event_id, &error.to_string()).await?;
                    made_content_progress = true;
                }
            }
        }

        // Filter lane eligibility in memory first so unchosen or attachment-only items do not
        // trigger redundant SQLite write transactions before a download slot actually claims them.
        let mut eligible_content = Vec::with_capacity(pending_content.len());
        let mut woke_attachments = false;
        for (event, envelope) in pending_content {
            if envelope.item.kind != "files"
                && !envelope.blobs.is_empty()
                && !complete_content_source_event_ids.contains(&event.event_id)
            {
                continue;
            }
            if !attachments && envelope.item.kind != "files" && !envelope.blobs.is_empty() {
                woke_attachments = true;
                continue;
            }
            if attachments && envelope.blobs.is_empty() {
                continue;
            }
            eligible_content.push((event, envelope));
        }
        if woke_attachments {
            self.wake_attachments();
        }

        let mut body_claim = None;
        if attachments {
            let mut attachment_groups: Vec<ClipboardEnvelope> = Vec::new();
            eligible_content.retain(|(_, envelope)| {
                if !matches!(envelope.item.kind.as_str(), "files" | "image") {
                    return true;
                }
                if attachment_groups
                    .iter()
                    .any(|other| same_attachment_content(envelope, other))
                {
                    return false;
                }
                attachment_groups.push(envelope.clone());
                true
            });
            let mut selected = Vec::new();
            for (event, envelope) in eligible_content {
                let claim_key = format!("apply:{}", envelope.item.sync_key);
                let Some(claim) = self.delivery.claim_remote_download(&claim_key) else {
                    continue;
                };
                if !repository::content_event_pending(pool, &event.event_id).await? {
                    continue;
                }
                if matches!(envelope.item.kind.as_str(), "files" | "image") {
                    let (ids, retry) = self
                        .equivalent_pending_files(pool, &event.event_id, &envelope, &key, source)
                        .await?;
                    if let Some((deadline, representative)) = retry {
                        repository::copy_apply_retry(pool, &ids, &representative).await?;
                        self.schedule_transfer_wake(deadline - Utc::now());
                        continue;
                    }
                }
                body_claim = Some(claim);
                selected.push((event, envelope));
                break;
            }
            eligible_content = selected;
        }
        let _body_claim = body_claim;

        // SQLite 写入按游标顺序串行完成，避免并发事务互相触发 SQLITE_BUSY；
        // 只有不持有数据库事务的 Blob 网络与文件 I/O 才并发执行。
        let mut ready_content = Vec::new();
        let mut metadata_only_content = Vec::new();
        let mut deduplicated_content = Vec::new();
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let history = self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .clipboard
            .history;
        let local_storage_limit_bytes = history.max_storage_bytes;
        let mut quota = AttachmentQuotaInspector::for_history(&self.app, pool, &history).await?;
        for (event, envelope) in &eligible_content {
            let (_, item_updated_at) = synced_item_timestamps(&envelope.item, event.created_at_ms);
            quota.observe_timestamp(item_updated_at);
        }
        for (event, envelope) in eligible_content {
            let event_id = event.event_id.clone();
            let preparation: Result<()> = async {
                let presence = SyncedItemStatePatch {
                    sync_key: envelope.item.sync_key.clone(),
                    is_favorite: None,
                    is_pinned: None,
                    note: None,
                    note_changed: false,
                    is_present: Some(true),
                };
                let _resource_guard = crate::clipboard::shared_files::lock().await;
                let outcome = db::sync_item_state::apply_remote_patch(
                    pool,
                    &presence,
                    &db::sync_item_state::SyncEventVersion {
                        origin_sequence: event.origin_sequence,
                        origin_device_id: event.origin_device_id.clone(),
                    },
                )
                .await?;

                if !outcome.image_files.is_empty() {
                    let store = self.app.state::<ImageStore>();
                    if let Err(error) = crate::clipboard::cache::reclaim_deleted_images(
                        pool,
                        store.inner(),
                        &outcome.image_files,
                    )
                    .await
                    {
                        log::warn!(
                            "reclaim deleted synchronized images after content presence failed: {error:#}"
                        );
                    }
                }
                if outcome.canonical_presence == Some(false) {
                    if outcome.deleted {
                        self.emit_clipboard_reconciled();
                        #[cfg(target_os = "android")]
                        crate::commands::android::notify_overlay_clipboard_changed();
                    }
                    if let Err(error) = crate::clipboard::shared_files::cleanup_resources_locked(
                        pool,
                        resources.clone(),
                    )
                    .await
                    {
                        log::warn!(
                            "reclaim deleted synchronized files after content presence failed: {error:#}"
                        );
                    }
                    repository::mark_applied(pool, &event.event_id).await?;
                    if matches!(envelope.item.kind.as_str(), "files" | "image") {
                        let (aliases, _) = self
                            .equivalent_pending_files(
                                pool,
                                &event.event_id,
                                &envelope,
                                &key,
                                source,
                            )
                            .await?;
                        for alias in aliases {
                            repository::mark_applied(pool, &alias).await?;
                        }
                    }
                    made_content_progress = true;
                    return Ok(());
                }

                let (_, item_updated_at) =
                    synced_item_timestamps(&envelope.item, event.created_at_ms);
                let attachment_bytes = envelope
                    .blobs
                    .iter()
                    .fold(0_u64, |total, blob| total.saturating_add(blob.original_size));
                if envelope.item.kind == "image"
                    && attachments
                    && !quota
                        .admit_or_reject_historical(item_updated_at, attachment_bytes)
                        .await
                {
                    repository::mark_applied(pool, &event.event_id).await?;
                    let (aliases, _) = self
                        .equivalent_pending_files(pool, &event.event_id, &envelope, &key, source)
                        .await?;
                    for alias in aliases {
                        repository::mark_applied(pool, &alias).await?;
                    }
                    made_content_progress = true;
                    return Ok(());
                }

                let source_revision = envelope
                    .item
                    .source_revision
                    .as_deref()
                    .unwrap_or(&event.event_id);
                let local_file_available = envelope.item.kind == "files"
                    && available_materialized_file_item(
                        pool,
                        &resources,
                        &envelope.item.sync_key,
                        source_revision,
                        envelope.content_identity.as_deref(),
                    )
                    .await?
                    .is_some();
                if local_file_available {
                    deduplicated_content.push((event, envelope));
                } else if envelope.item.kind == "files" && !files {
                    let download_pending = complete_content_source_event_ids.contains(&event.event_id)
                        && !should_defer_remote_file_download(
                            &envelope,
                            sync_settings.auto_download_max_mb,
                            local_storage_limit_bytes,
                        )
                        && quota
                            .admit_or_reject_historical(item_updated_at, attachment_bytes)
                            .await;
                    metadata_only_content.push((event, envelope, download_pending));
                } else if envelope.item.kind == "files" {
                    let deferred_or_rejected =
                        !complete_content_source_event_ids.contains(&event.event_id)
                            || should_defer_remote_file_download(
                                &envelope,
                                sync_settings.auto_download_max_mb,
                                local_storage_limit_bytes,
                            )
                            || !quota
                                .admit_or_reject_historical(item_updated_at, attachment_bytes)
                                .await;
                    if deferred_or_rejected {
                        metadata_only_content.push((event, envelope, false));
                    } else {
                        let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
                        let (item_id, _, _) = self
                            .materialize_remote_file_metadata_card(
                                pool,
                                &event,
                                envelope.clone(),
                                group,
                            )
                            .await?;
                        source_assets_pending |= source_icon.is_some();
                        ready_content.push((event, envelope, Some(item_id)));
                    }
                } else {
                    ready_content.push((event, envelope, None));
                }
                Ok(())
            }
            .await;
            if let Err(error) = preparation {
                self.defer_card_apply(pool, &event_id, &error.to_string())
                    .await?;
            }
        }

        for (event, envelope, download_pending) in metadata_only_content {
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            let aliases = if !download_pending {
                self.equivalent_pending_files(pool, &event.event_id, &envelope, &key, source)
                    .await?
                    .0
            } else {
                Vec::new()
            };
            let applied: Result<()> = async {
                self.materialize_remote_file_metadata_card(pool, &event, envelope, group)
                    .await?;
                repository::record_inbound_outcome(
                    pool,
                    source.remote_batch_source(),
                    &event.event_id,
                    repository::InboundContentOutcome::Metadata,
                )
                .await?;
                if download_pending {
                    self.wake_attachments();
                } else {
                    repository::mark_applied(pool, &event.event_id).await?;
                    for alias in &aliases {
                        repository::record_inbound_outcome(
                            pool,
                            source.remote_batch_source(),
                            alias,
                            repository::InboundContentOutcome::Deduplicated,
                        )
                        .await?;
                        repository::mark_applied(pool, alias).await?;
                    }
                }
                Ok(())
            }
            .await;
            match applied {
                Ok(()) => {
                    made_content_progress = true;
                    source_assets_pending |= source_icon.is_some();
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                }
                Err(error) => {
                    self.defer_card_apply(pool, &event.event_id, &error.to_string())
                        .await?;
                }
            }
        }

        for (event, envelope) in deduplicated_content {
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            let applied: Result<()> = async {
                self.materialize_remote_file_metadata_card(pool, &event, envelope, group)
                    .await?;
                repository::record_inbound_outcome(
                    pool,
                    source.remote_batch_source(),
                    &event.event_id,
                    repository::InboundContentOutcome::Deduplicated,
                )
                .await?;
                repository::mark_applied(pool, &event.event_id).await?;
                Ok(())
            }
            .await;
            match applied {
                Ok(()) => {
                    made_content_progress = true;
                    source_assets_pending |= source_icon.is_some();
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                }
                Err(error) => {
                    self.defer_card_apply(pool, &event.event_id, &error.to_string())
                        .await?;
                }
            }
        }

        if attachments {
            drop(apply_guard.take());
        }
        let download_route = if source.is_cloud() {
            super::super::progress::TransferRoute::Cloud
        } else {
            super::super::progress::TransferRoute::Lan
        };
        let download_target = source.delivery_target().to_owned();
        let preparations = n0_future::stream::iter(ready_content)
            .map(|(event, envelope, download_item)| {
                let download_target = download_target.clone();
                async move {
                    let mut stored_blobs = Vec::new();
                    let mut blob_error = None;
                    let transferred = AtomicU64::new(0);
                    let download = if let Some(item_id) = download_item {
                        let claim = self.delivery.claim_remote_download(&item_id)?;
                        let total_bytes = envelope.blobs.iter().fold(0_u64, |total, manifest| {
                            total.saturating_add(manifest.encrypted_size)
                        });
                        let task_key = format!(
                            "{}\0{}\0{}\0auto-download",
                            file_transfer_key(&envelope),
                            download_target,
                            total_bytes
                        );
                        let handle = self.delivery.start(super::super::progress::TransferTask {
                            id: blake3::hash(task_key.as_bytes()).to_hex().to_string(),
                            revision: 0,
                            started_at_ms: chrono::Utc::now().timestamp_millis(),
                            operation_id: event.event_id.clone(),
                            item_id,
                            direction: super::super::progress::TransferDirection::Download,
                            route: download_route,
                            target: download_target,
                            file_name: (envelope.blobs.len() == 1)
                                .then(|| envelope.blobs[0].name.clone()),
                            phase: super::super::progress::TransferPhase::Queued,
                            transferred_bytes: 0,
                            total_bytes,
                            speed_bytes_per_second: 0,
                            elapsed_ms: 0,
                            eta_seconds: None,
                            error: None,
                            cancellable: true,
                        });
                        handle.connecting();
                        Some((handle, total_bytes, claim))
                    } else {
                        None
                    };
                    let mut completed_bytes = 0_u64;
                    for manifest in &envelope.blobs {
                        if download
                            .as_ref()
                            .is_some_and(|(handle, _, _)| handle.canceled())
                        {
                            blob_error = Some("transfer canceled".to_owned());
                            break;
                        }
                        let progress =
                            download
                                .as_ref()
                                .map(|(handle, _, _)| BlobDownloadProgress {
                                    completed_bytes,
                                    handle,
                                    transferred: &transferred,
                                });
                        match self
                            .ensure_blob(connection, group, manifest, progress)
                            .await
                        {
                            Ok(path) => stored_blobs.push(StoredBlob {
                                blob_id: manifest.blob_id.clone(),
                                encrypted_path: path.to_string_lossy().into_owned(),
                                size: manifest.encrypted_size,
                                source_path: None,
                                source_is_directory: false,
                                stream_nonce: None,
                            }),
                            Err(error) => {
                                log::debug!(
                                    "defer sync event {} until blob {} is available: {error}",
                                    event.event_id,
                                    manifest.blob_id
                                );
                                blob_error = Some(format!(
                                    "同步 Blob {} 暂时不可用: {error}",
                                    manifest.blob_id
                                ));
                                break;
                            }
                        }
                        completed_bytes = completed_bytes.saturating_add(manifest.encrypted_size);
                    }
                    let mut canceled = download
                        .as_ref()
                        .is_some_and(|(handle, _, _)| handle.canceled());
                    if blob_error.is_none()
                        && download
                            .as_ref()
                            .is_some_and(|(handle, _, _)| !handle.begin_finalizing(completed_bytes))
                    {
                        blob_error = Some("transfer canceled".to_owned());
                        canceled = true;
                    }
                    if let (Some(error), Some((handle, _, _))) = (&blob_error, &download) {
                        if !handle.canceled() {
                            handle.failed(transferred.load(Ordering::Acquire), error.clone());
                        }
                    }
                    Some((
                        event,
                        envelope,
                        stored_blobs,
                        blob_error,
                        download,
                        canceled,
                    ))
                }
            })
            .buffered_unordered(2);
        let mut preparations = std::pin::pin!(preparations);
        let mut consumed_staging = Vec::new();
        while let Some(prepared) = preparations.next().await {
            let Some(prepared) = prepared else {
                continue;
            };
            let (event, envelope, stored_blobs, blob_error, download, canceled) = prepared;
            let aliases = if matches!(envelope.item.kind.as_str(), "files" | "image") {
                self.equivalent_pending_files(pool, &event.event_id, &envelope, &key, source)
                    .await?
                    .0
            } else {
                Vec::new()
            };
            if let Some(error) = blob_error {
                if canceled {
                    repository::mark_applied(pool, &event.event_id).await?;
                    for alias in &aliases {
                        repository::mark_applied(pool, alias).await?;
                    }
                    consumed_staging.extend(
                        stored_blobs
                            .into_iter()
                            .map(|blob| PathBuf::from(blob.encrypted_path)),
                    );
                    made_content_progress = true;
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                    continue;
                }
                self.defer_card_apply(pool, &event.event_id, &error).await?;
                repository::copy_apply_retry(pool, &aliases, &event.event_id).await?;
                continue;
            }
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            let manifests = envelope.blobs.clone();
            let content_identity = envelope.content_identity.clone();
            let applied: Result<_> = async {
                let (item_id, item, fingerprint, deduplicated) =
                    self.apply_envelope(&event, envelope, group).await?;
                if !item_id.is_empty() {
                    let sources = self
                        .materialized_blob_sources(&item, &manifests, &stored_blobs)
                        .await?;
                    repository::register_event_blob_sources(pool, &event.event_id, &sources)
                        .await?;
                    repository::link_content_event_to_item(
                        pool,
                        &item_id,
                        &event.event_id,
                        "remote",
                        content_identity.as_deref(),
                    )
                    .await?;
                    self.reconcile_item_timestamps(pool, group, &item_id)
                        .await?;
                }
                repository::record_inbound_outcome(
                    pool,
                    source.remote_batch_source(),
                    &event.event_id,
                    repository::InboundContentOutcome::Received,
                )
                .await?;
                repository::mark_applied(pool, &event.event_id).await?;
                for alias in &aliases {
                    repository::record_inbound_outcome(
                        pool,
                        source.remote_batch_source(),
                        alias,
                        repository::InboundContentOutcome::Deduplicated,
                    )
                    .await?;
                    repository::mark_applied(pool, alias).await?;
                }
                Ok((item_id, item, fingerprint, deduplicated))
            }
            .await;
            match applied {
                Ok((item_id, item, fingerprint, deduplicated)) => {
                    if !item_id.is_empty()
                        && latest_clipboard_item.as_ref().is_none_or(
                            |(latest, _, _): &(
                                ClipboardItem,
                                Option<ClipboardFingerprint>,
                                bool,
                            )| item.updated_at >= latest.updated_at,
                        )
                    {
                        latest_clipboard_item = Some((item, fingerprint, deduplicated));
                    }
                    if let Some((handle, total_bytes, _)) = &download {
                        handle.completed(*total_bytes);
                    }
                    made_content_progress = true;
                    source_assets_pending |= source_icon.is_some();
                    consumed_staging.extend(
                        stored_blobs
                            .into_iter()
                            .map(|blob| PathBuf::from(blob.encrypted_path)),
                    );
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                }
                Err(error) => {
                    if let Some((handle, total_bytes, _)) = &download {
                        handle.failed(*total_bytes, error.to_string());
                    }
                    self.defer_card_apply(pool, &event.event_id, &error.to_string())
                        .await?;
                    repository::copy_apply_retry(pool, &aliases, &event.event_id).await?;
                }
            }
        }
        for path in consumed_staging {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    log::debug!(
                        "defer consumed sync staging cleanup {}: {error}",
                        path.display()
                    );
                }
            }
        }
        if let Some((item, fingerprint, deduplicated)) = latest_clipboard_item.as_ref() {
            self.write_latest_synced_item_to_clipboard(
                item,
                fingerprint.as_ref(),
                sync_settings.auto_write_clipboard
                    && self
                        .app
                        .state::<Arc<ClipboardFingerprintState>>()
                        .generation()
                        == clipboard_generation
                    && repository::is_latest_clipboard_item(pool, &item.id).await?,
            );
            if let Err(error) = self.app.emit(
                crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                serde_json::json!({
                    "id": item.id,
                    "kind": item.kind,
                    "deduplicated": deduplicated,
                    "reconciled": true,
                }),
            ) {
                log::warn!(
                    "emit {} failed: {error}",
                    crate::clipboard::CLIPBOARD_UPDATED_EVENT
                );
            }
        }
        drop(apply_guard.take());
        #[cfg(target_os = "android")]
        if applied_any {
            crate::commands::android::notify_overlay_clipboard_changed();
        }
        if source_assets_pending {
            self.runtime_state.source_asset_wake.notify_one();
        }
        // Attachment receipts are created after the record exchange has already finished.
        if attachments && made_content_progress {
            self.wake_lan_transfer();
        }
        Ok(made_content_progress
            && repository::has_unapplied_content_for_target(
                pool,
                source.delivery_target(),
                attachments,
                files,
            )
            .await?)
    }

    /// Includes copies received during an active download and preserves their shared backoff.
    async fn equivalent_pending_files(
        &self,
        pool: &SqlitePool,
        event_id: &str,
        envelope: &ClipboardEnvelope,
        key: &[u8; 32],
        source: &SyncConnectionTarget,
    ) -> Result<(Vec<String>, Option<(chrono::DateTime<Utc>, String)>)> {
        let mut ids = Vec::new();
        let mut retry: Option<(chrono::DateTime<Utc>, String)> = None;
        for (alias, retry_at) in
            repository::pending_file_events_for_item(pool, event_id, source.delivery_target())
                .await?
        {
            if alias.event.event_id == event_id {
                continue;
            }
            if let Ok(DecryptedSyncEvent::Clipboard(other)) =
                crypto::decrypt_sync_event(key, &alias.event)
            {
                if same_attachment_content(envelope, &other) {
                    if let Some(deadline) = retry_at.filter(|deadline| *deadline > Utc::now()) {
                        if retry
                            .as_ref()
                            .is_none_or(|(current, _)| deadline > *current)
                        {
                            retry = Some((deadline, alias.event.event_id.clone()));
                        }
                    }
                    ids.push(alias.event.event_id);
                }
            }
        }
        Ok((ids, retry))
    }

    /// Maps a materialized item back to its plaintext sources for future task-scoped staging.
    async fn materialized_blob_sources(
        &self,
        item: &ClipboardItem,
        manifests: &[BlobManifest],
        staged: &[StoredBlob],
    ) -> Result<Vec<StoredBlob>> {
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let source_paths = match item.kind {
            ClipboardKind::Text => Vec::new(),
            ClipboardKind::Image => vec![self.app.state::<ImageStore>().origin_path(&item.content)],
            ClipboardKind::Files => item
                .content
                .lines()
                .filter(|value| !value.is_empty())
                .map(|value| crate::clipboard::shared_files::resolve_path(&resources, value))
                .collect::<Result<Vec<_>>>()?,
        };
        let content_manifests = manifests
            .iter()
            .filter(|manifest| {
                manifest.role != BlobRole::Image || item.kind == ClipboardKind::Image
            })
            .collect::<Vec<_>>();
        if content_manifests.len() != source_paths.len() {
            bail!("materialized sync blob source count mismatch");
        }
        let mut sources = Vec::with_capacity(source_paths.len());
        for (manifest, source_path) in content_manifests.into_iter().zip(source_paths) {
            let staged = staged
                .iter()
                .find(|blob| blob.blob_id == manifest.blob_id)
                .context("materialized sync blob staging is missing")?;
            sources.push(StoredBlob {
                blob_id: staged.blob_id.clone(),
                encrypted_path: staged.encrypted_path.clone(),
                size: staged.size,
                source_path: Some(source_path.to_string_lossy().into_owned()),
                source_is_directory: manifest.is_directory_archive,
                stream_nonce: Some(crypto::blob_stream_nonce(Path::new(
                    &staged.encrypted_path,
                ))?),
            });
        }
        Ok(sources)
    }

    /// Merges a metadata or presence event even when its clipboard content has not arrived yet.
    async fn apply_item_state_event(
        &self,
        pool: &SqlitePool,
        event: &EncryptedEvent,
        patch: SyncedItemStatePatch,
    ) -> Result<()> {
        let sync_key = patch.sync_key.clone();
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let _resource_guard = crate::clipboard::shared_files::lock().await;
        let outcome = db::sync_item_state::apply_remote_patch(
            pool,
            &patch,
            &db::sync_item_state::SyncEventVersion {
                origin_sequence: event.origin_sequence,
                origin_device_id: event.origin_device_id.clone(),
            },
        )
        .await?;

        if let Some(item_id) = outcome.item_id.as_deref() {
            repository::link_event_to_item(pool, item_id, &event.event_id, "remote").await?;
            self.app.emit(
                crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                serde_json::json!({
                    "id": item_id,
                    "deduplicated": true,
                    "reconciled": true,
                }),
            )?;
        } else if outcome.canonical_presence == Some(false) {
            self.app.emit(
                crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                serde_json::json!({ "deletedHash": sync_key, "reconciled": true }),
            )?;
        }

        if !outcome.image_files.is_empty() {
            let store = self.app.state::<ImageStore>();
            if let Err(error) = crate::clipboard::cache::reclaim_deleted_images(
                pool,
                store.inner(),
                &outcome.image_files,
            )
            .await
            {
                log::warn!(
                    "reclaim deleted synchronized images after state apply failed: {error:#}"
                );
            }
        }

        if outcome.canonical_presence == Some(false) {
            if let Err(error) =
                crate::clipboard::shared_files::cleanup_resources_locked(pool, resources).await
            {
                log::warn!(
                    "reclaim deleted synchronized files after state apply failed: {error:#}"
                );
            }
        }
        repository::mark_applied(pool, &event.event_id).await?;
        Ok(())
    }

    async fn ensure_blob(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        manifest: &BlobManifest,
        progress: Option<BlobDownloadProgress<'_>>,
    ) -> Result<PathBuf> {
        let root = crate::core::paths::resources_dir(&self.app)?.join(SYNC_STAGING_DIR);
        self.ensure_blob_at(connection, group, manifest, &root, progress)
            .await
    }
    /// Downloads one encrypted blob into the caller-owned cache root.
    pub(super) async fn ensure_blob_at(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        manifest: &BlobManifest,
        root: &Path,
        progress: Option<BlobDownloadProgress<'_>>,
    ) -> Result<PathBuf> {
        validate_blob_manifest(manifest)?;
        let path = crypto::blob_path(root, &manifest.blob_id)?;
        let download_lock = {
            let mut locks = self.blob_download_locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(&path).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(path.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let _download_guard = download_lock.lock().await;
        if progress.is_some_and(|progress| progress.canceled()) {
            bail!("transfer canceled");
        }
        if path.is_file() && path.metadata()?.len() == manifest.encrypted_size {
            if let Some(progress) = progress {
                progress.transferred(manifest.encrypted_size);
            }
            return Ok(path);
        }
        download_blob(connection, group, manifest, &path, progress).await?;
        Ok(path)
    }

    /// Downloads and decrypts a cloud-only image into an ephemeral cache outside durable history.
    pub(super) async fn cloud_record_image_path(
        &self,
        connection: &Connection,
        group: &GroupSecrets,
        envelope: &ClipboardEnvelope,
    ) -> Result<Option<String>> {
        let Some(manifest) = envelope
            .blobs
            .iter()
            .find(|blob| blob.role == BlobRole::Image)
        else {
            return Ok(None);
        };
        let group_cache = self
            .app
            .path()
            .app_cache_dir()?
            .join(CLOUD_RECORD_CACHE_DIR)
            .join(blake3::hash(group.group_id.as_bytes()).to_hex().as_str());
        let encrypted = self
            .ensure_blob_at(
                connection,
                group,
                manifest,
                &group_cache.join("blobs"),
                None,
            )
            .await?;
        let destination = group_cache
            .join("images")
            .join(format!("{}.png", manifest.blob_id));
        if !destination.is_file() || destination.metadata()?.len() != manifest.original_size {
            fs::create_dir_all(
                destination
                    .parent()
                    .context("cloud record image cache has no parent")?,
            )?;
            let destination_for_task = destination.clone();
            let original_size = manifest.original_size;
            let key = group.content_key_bytes()?;
            tauri::async_runtime::spawn_blocking(move || {
                crypto::decrypt_blob(&encrypted, &destination_for_task, original_size, &key)
            })
            .await
            .context("cloud image decryption task failed")??;
        }

        Ok(Some(destination.to_string_lossy().into_owned()))
    }

    /// Creates or refreshes the remote card before an optional managed download starts.
    async fn materialize_remote_file_metadata_card(
        &self,
        pool: &SqlitePool,
        event: &EncryptedEvent,
        envelope: ClipboardEnvelope,
        group: &GroupSecrets,
    ) -> Result<(String, ClipboardItem, bool)> {
        let content_identity = envelope.content_identity.clone();
        let (item_id, item, deduplicated) = self
            .apply_remote_file_metadata(event, envelope, group)
            .await?;
        if !item_id.is_empty() {
            repository::link_content_event_to_item(
                pool,
                &item_id,
                &event.event_id,
                "remote",
                content_identity.as_deref(),
            )
            .await?;
            self.reconcile_item_timestamps(pool, group, &item_id)
                .await?;
            if let Err(error) = self.app.emit(
                crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                serde_json::json!({
                    "id": item.id,
                    "kind": item.kind,
                    "deduplicated": deduplicated,
                    "reconciled": true,
                }),
            ) {
                log::warn!("emit remote file metadata update failed: {error}");
            }
        }
        Ok((item_id, item, deduplicated))
    }

    /// Materializes a remote file card from its encrypted event without downloading file bytes.
    async fn apply_remote_file_metadata(
        &self,
        event: &EncryptedEvent,
        envelope: ClipboardEnvelope,
        group: &GroupSecrets,
    ) -> Result<(String, ClipboardItem, bool)> {
        if envelope.version != 1 || parse_kind(&envelope.item.kind)? != ClipboardKind::Files {
            bail!("不支持的远端文件元数据");
        }
        let pool = self.pool().await;
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let source_revision = envelope
            .item
            .source_revision
            .as_deref()
            .unwrap_or(&event.event_id);
        if let Some(existing) = available_materialized_file_item(
            &pool,
            &resources,
            &envelope.item.sync_key,
            source_revision,
            envelope.content_identity.as_deref(),
        )
        .await?
        {
            return Ok((existing.id.clone(), existing, true));
        }

        let mut manifests = envelope
            .blobs
            .iter()
            .filter(|blob| blob.role == BlobRole::File)
            .collect::<Vec<_>>();
        manifests.sort_by_key(|blob| blob.file_index.unwrap_or(u32::MAX));
        let content = manifests
            .iter()
            .map(|blob| sanitize_name(&blob.name))
            .collect::<Vec<_>>()
            .join("\n");
        if content.is_empty() {
            bail!("远端文件元数据为空");
        }

        let (created_at, updated_at) = synced_item_timestamps(&envelope.item, event.created_at_ms);
        let source_updated_at = envelope
            .item
            .source_updated_at_ms
            .and_then(|value| Utc.timestamp_millis_opt(value).single())
            .unwrap_or(updated_at);
        let source_revision = envelope
            .item
            .source_revision
            .clone()
            .unwrap_or_else(|| event.event_id.clone());
        let source_app_id = if let Some(source_app) = envelope.source_app.as_ref() {
            match self
                .resolve_received_source_app(
                    &pool,
                    group,
                    source_app,
                    source_updated_at,
                    &source_revision,
                )
                .await
            {
                Ok(app_id) => Some(app_id),
                Err(error) => {
                    log::warn!("ignore invalid synchronized source app: {error}");
                    None
                }
            }
        } else {
            None
        };
        let sync_key = envelope.item.sync_key.clone();
        let mut item = ClipboardItem {
            id: Uuid::new_v4().to_string(),
            kind: ClipboardKind::Files,
            sub_kind: None,
            group_id: None,
            source_app_id,
            source_revision,
            source_device_id: envelope.item.source_device_id.clone(),
            source_device_name: envelope.item.source_device_name.clone(),
            source_updated_at: Some(source_updated_at),
            source_device_is_local: false,
            content: content.clone(),
            content_hash: sync_key.clone(),
            sync_key: sync_key.clone(),
            search_text: Some(content),
            summary: envelope.item.summary,
            text_char_count: None,
            file_types: envelope.item.file_types,
            size: envelope.item.size,
            width: None,
            height: None,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: envelope.item.is_sensitive,
            platform: parse_platform(&envelope.item.source_platform),
            note: None,
            created_at,
            updated_at,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            remote_file_state: Some("available".to_owned()),
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        };
        let result = db::items::upsert_remote_file_item(
            &pool,
            &item,
            &event.event_id,
            &event.origin_device_id,
        )
        .await?;
        item.id.clone_from(&result.id);
        if let Some(materialized) = db::items::find_item_by_id(&pool, &result.id).await? {
            item = materialized;
        }
        Ok((result.id, item, result.deduplicated))
    }

    async fn apply_envelope(
        &self,
        event: &EncryptedEvent,
        envelope: ClipboardEnvelope,
        group: &GroupSecrets,
    ) -> Result<(String, ClipboardItem, Option<ClipboardFingerprint>, bool)> {
        let _sync_activity = SyncActivityGuard::acquire();
        if envelope.version != 1 {
            bail!("不支持的同步事件版本");
        }
        let kind = parse_kind(&envelope.item.kind)?;
        // Resource GC uses the same lock. Keep materialized files alive until their database
        // references commit, while preserving the global apply_lock -> resource lock order.
        let image_apply_guard = if kind == ClipboardKind::Image {
            Some(self.apply_lock.lock().await)
        } else {
            None
        };
        let mut resource_guard = if kind != ClipboardKind::Image {
            None
        } else {
            Some(crate::clipboard::shared_files::lock().await)
        };
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let blob_root = resources.join(SYNC_STAGING_DIR);
        let mut file_apply_guard = None;
        let (content, files_fingerprint) = match kind {
            ClipboardKind::Text => (envelope.item.content.clone(), None),
            ClipboardKind::Image => {
                let manifest = envelope
                    .blobs
                    .iter()
                    .find(|blob| blob.role == BlobRole::Image)
                    .context("同步图片缺少数据")?;
                let image_name = sanitize_name(&manifest.name);
                let destination = self.app.state::<ImageStore>().origin_path(&image_name);
                let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                let key = group.content_key_bytes()?;
                let original_size = manifest.original_size;
                tauri::async_runtime::spawn_blocking(move || {
                    crypto::decrypt_blob(&encrypted, &destination, original_size, &key)
                })
                .await
                .context("image decryption task failed")??;
                (image_name, None)
            }
            ClipboardKind::Files => {
                let temporary = tempfile::Builder::new()
                    .prefix("sync-receive-")
                    .tempdir_in(&resources)?;
                let directory = temporary.path().join("files");
                let final_directory = resources
                    .join("sync-files")
                    .join(Uuid::new_v4().to_string());
                let mut paths = Vec::new();
                let mut file_fingerprints = Vec::new();
                for manifest in &envelope.blobs {
                    if manifest.role != BlobRole::File {
                        continue;
                    }
                    // 不在逻辑文件名前加序号：系统剪贴板及 RustDesk 会继续
                    // 传播落盘后的 basename，改名会让同一内容在往返中产生不同指纹。
                    // 用独立序号目录处理同名根项，同时保留原始 basename。
                    let destination = synced_file_destination(
                        &directory,
                        manifest.file_index.unwrap_or(0),
                        &manifest.name,
                    );
                    let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                    let key = group.content_key_bytes()?;
                    let size = manifest.original_size;
                    let destination_for_task = destination.clone();
                    let is_directory = manifest.is_directory_archive;
                    let stream_commitment = manifest.stream_commitment;
                    let expected_blob_id = manifest.blob_id.clone();
                    let content_hash = tauri::async_runtime::spawn_blocking(move || {
                        if is_directory {
                            let archive_path =
                                destination_for_task.with_extension("ecopaste-dir.zip");
                            crypto::decrypt_blob(&encrypted, &archive_path, size, &key)?;
                            let fingerprint = crypto::extract_directory_archive(
                                &archive_path,
                                &destination_for_task,
                            )?;
                            fs::remove_file(archive_path).ok();
                            Ok::<_, anyhow::Error>(fingerprint)
                        } else {
                            let content_hash = crypto::decrypt_blob_with_fingerprint(
                                &encrypted,
                                &destination_for_task,
                                size,
                                &key,
                            )?;
                            if stream_commitment {
                                crypto::verify_stream_blob_id(
                                    &key,
                                    &encrypted,
                                    &content_hash,
                                    &expected_blob_id,
                                )?;
                            }
                            Ok(content_hash)
                        }
                    })
                    .await
                    .context("file decryption task failed")??;
                    file_fingerprints.push(FileEntryFingerprint {
                        name: manifest.name.clone(),
                        is_directory,
                        content_hash,
                    });
                    paths.push(destination.to_string_lossy().into_owned());
                }
                file_apply_guard = Some(self.apply_lock.lock().await);
                resource_guard = Some(crate::clipboard::shared_files::lock().await);
                tokio::fs::create_dir_all(resources.join("sync-files")).await?;
                tokio::fs::rename(&directory, &final_directory).await?;
                let physical_content = paths
                    .iter()
                    .map(|path| {
                        final_directory
                            .join(
                                Path::new(path)
                                    .strip_prefix(&directory)
                                    .expect("received file is inside temporary directory"),
                            )
                            .to_string_lossy()
                            .into_owned()
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let content = crate::clipboard::shared_files::normalize_owned_content(
                    &resources,
                    &physical_content,
                )?
                .context("materialized synchronized files are outside the managed root")?;
                (
                    content,
                    ClipboardFingerprint::from_file_entries(&file_fingerprints),
                )
            }
        };
        let (created_at, updated_at) = synced_item_timestamps(&envelope.item, event.created_at_ms);
        let source_updated_at = envelope
            .item
            .source_updated_at_ms
            .and_then(|value| Utc.timestamp_millis_opt(value).single())
            .unwrap_or(updated_at);
        let source_revision = envelope
            .item
            .source_revision
            .clone()
            .unwrap_or_else(|| event.event_id.clone());
        let pool = self.pool().await;
        let source_app_id = if let Some(source_app) = envelope.source_app.as_ref() {
            match self
                .resolve_received_source_app(
                    &pool,
                    group,
                    source_app,
                    source_updated_at,
                    &source_revision,
                )
                .await
            {
                Ok(app_id) => Some(app_id),
                Err(error) => {
                    log::warn!("ignore invalid synchronized source app: {error}");
                    None
                }
            }
        } else {
            None
        };
        let sync_key = envelope.item.sync_key.clone();

        let mut item = ClipboardItem {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            sub_kind: envelope
                .item
                .sub_kind
                .as_deref()
                .map(parse_sub_kind)
                .transpose()?,
            group_id: None,
            source_app_id,
            source_revision,
            source_device_id: envelope.item.source_device_id.clone(),
            source_device_name: envelope.item.source_device_name.clone(),
            source_updated_at: Some(source_updated_at),
            source_device_is_local: false,
            content_hash: if kind == ClipboardKind::Files {
                db::items::content_hash(kind, &content)
            } else {
                sync_key.clone()
            },
            sync_key: sync_key.clone(),
            content,
            search_text: envelope.item.search_text,
            summary: envelope.item.summary,
            text_char_count: None,
            file_types: envelope.item.file_types,
            size: envelope.item.size,
            width: envelope.item.width,
            height: envelope.item.height,
            use_count: 1,
            is_favorite: false,
            is_pinned: false,
            is_sensitive: envelope.item.is_sensitive,
            platform: parse_platform(&envelope.item.source_platform),
            note: None,
            created_at,
            updated_at,
            source_app_name: None,
            source_app_icon_file: None,
            source_app_icon_path: None,
            source_app_accent_start: None,
            source_app_accent_end: None,
            image_thumbnail_path: None,
            file_entries: None,
            files_preview_kind: None,
            remote_file_state: None,
            available_actions: Vec::new(),
            color_preview: None,
            display_created_at: String::new(),
        };
        let result = db::items::upsert_materialized_synced_item(&pool, &item).await?;
        drop(resource_guard);
        drop(file_apply_guard);
        drop(image_apply_guard);
        if result.id.is_empty() {
            return Ok((String::new(), item, None, result.deduplicated));
        }
        item.id.clone_from(&result.id);
        if let Some(materialized) = db::items::find_item_by_id(&pool, &result.id).await? {
            item = materialized;
        }
        let fingerprint = files_fingerprint.or_else(|| ClipboardFingerprint::from_text_item(&item));
        Ok((result.id, item, fingerprint, result.deduplicated))
    }

    /// A remote batch updates the system clipboard at most once. Clipboard ownership is
    /// independent from transport health, so a busy OS clipboard must never fail synchronization.
    fn write_latest_synced_item_to_clipboard(
        &self,
        item: &ClipboardItem,
        _fingerprint: Option<&ClipboardFingerprint>,
        auto_write_clipboard: bool,
    ) {
        if !auto_write_clipboard {
            return;
        }
        let mut resolved_item = item.clone();
        let resources = match crate::core::paths::resources_dir(&self.app) {
            Ok(resources) => resources,
            Err(error) => {
                log::warn!("resolve synchronized clipboard resources failed: {error:#}");
                return;
            }
        };
        if let Err(error) =
            crate::clipboard::shared_files::resolve_item_content(&resources, &mut resolved_item)
        {
            log::warn!("resolve synchronized clipboard item failed: {error:#}");
            return;
        }

        let fingerprints = self.app.state::<Arc<ClipboardFingerprintState>>();
        #[cfg(target_os = "android")]
        let result = if resolved_item.kind == ClipboardKind::Text {
            crate::clipboard::write_synced_item_to_clipboard_app(
                &self.app,
                &fingerprints,
                &resolved_item,
            )
        } else {
            Ok(false)
        };
        #[cfg(not(target_os = "android"))]
        let guard = self.app.state::<Arc<WritebackGuard>>();
        #[cfg(not(target_os = "android"))]
        let result = crate::clipboard::write_synced_item_to_clipboard(
            &self.app.state::<ImageStore>(),
            &guard,
            &fingerprints,
            &resolved_item,
            _fingerprint,
        );

        if let Err(error) = result {
            log::warn!(
                "synchronized item {} was persisted but clipboard writeback failed: {error}",
                item.id
            );
        }
    }
}

/// Applies authoritative Hub facts without treating removed metadata as complete inline content.
async fn apply_cloud_availability_updates(
    pool: &SqlitePool,
    key: &[u8; 32],
    updates: &[CloudAvailabilityUpdate],
    missing_event_ids: &[String],
    latest_cursor: u64,
) -> Result<bool> {
    let event_ids = updates
        .iter()
        .map(|update| update.event_id.clone())
        .collect::<Vec<_>>();
    let events = repository::events_by_ids(pool, &event_ids)
        .await?
        .into_iter()
        .map(|stored| (stored.event.event_id.clone(), stored.event))
        .collect::<HashMap<_, _>>();
    let availability = updates
        .iter()
        .filter_map(|update| {
            events.get(&update.event_id).and_then(|event| {
                cloud_event_content_availability(event, &update.available_blob_ids, key)
                    .map(|state| (update.event_id.clone(), state))
            })
        })
        .collect::<Vec<_>>();
    repository::record_cloud_availability_batch(
        pool,
        &availability,
        missing_event_ids,
        latest_cursor,
    )
    .await?;
    Ok(!availability.is_empty() || !missing_event_ids.is_empty())
}

/// Derives Hub content availability from its authoritative stored-blob list.
fn cloud_content_availability(
    received: &ReceivedSyncEvent,
    key: &[u8; 32],
) -> Option<repository::RouteContentAvailability> {
    let available_blob_ids = received.cloud_blob_ids.as_ref()?;
    cloud_event_content_availability(&received.event, available_blob_ids, key)
}

fn cloud_event_content_availability(
    event: &EncryptedEvent,
    available_blob_ids: &[String],
    key: &[u8; 32],
) -> Option<repository::RouteContentAvailability> {
    let DecryptedSyncEvent::Clipboard(envelope) = crypto::decrypt_sync_event(key, event).ok()?
    else {
        return None;
    };
    let available_blob_ids = available_blob_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let complete = envelope
        .blobs
        .iter()
        .all(|blob| available_blob_ids.contains(blob.blob_id.as_str()));
    Some(if complete {
        repository::RouteContentAvailability::Complete
    } else {
        repository::RouteContentAvailability::Metadata
    })
}

/// Identifies the authenticated file manifest, independent of copy events and retry attempts.
fn file_transfer_key(envelope: &ClipboardEnvelope) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(envelope.item.sync_key.as_bytes());
    for blob in &envelope.blobs {
        hasher.update(blob.blob_id.as_bytes());
        hasher.update(blob.name.as_bytes());
        hasher.update(&blob.file_index.unwrap_or(0).to_le_bytes());
        hasher.update(&blob.original_size.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Uses the existing content fingerprint, with manifest identity for older stored events.
fn same_file_content(left: &ClipboardEnvelope, right: &ClipboardEnvelope) -> bool {
    left.item.sync_key == right.item.sync_key
        && (left
            .content_identity
            .as_ref()
            .zip(right.content_identity.as_ref())
            .is_some_and(|(left, right)| left == right)
            || file_transfer_key(left) == file_transfer_key(right))
}

fn same_attachment_content(left: &ClipboardEnvelope, right: &ClipboardEnvelope) -> bool {
    if left.item.kind != right.item.kind {
        return false;
    }
    match left.item.kind.as_str() {
        "files" => same_file_content(left, right),
        "image" => !left.item.sync_key.is_empty() && left.item.sync_key == right.item.sync_key,
        _ => false,
    }
}
