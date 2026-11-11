use super::*;

impl SyncManager {
    /// Applies the Hub's independent, resumable blob-availability stream.
    pub(super) async fn sync_cloud_availability(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<bool> {
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
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("云端返回了无效的内容可用性响应");
        };
        let event_ids = updates
            .iter()
            .map(|update| update.event_id.clone())
            .collect::<Vec<_>>();
        let events = repository::events_by_ids(pool, &event_ids)
            .await?
            .into_iter()
            .map(|stored| (stored.event.event_id.clone(), stored.event))
            .collect::<HashMap<_, _>>();
        let key = group.content_key_bytes()?;
        let availability = updates
            .iter()
            .filter_map(|update| {
                events.get(&update.event_id).and_then(|event| {
                    cloud_event_content_availability(event, &update.available_blob_ids, &key)
                        .map(|state| (update.event_id.clone(), state))
                })
            })
            .collect::<Vec<_>>();
        repository::record_cloud_availability_batch(pool, &availability, latest_cursor).await?;
        if !availability.is_empty() {
            self.emit_updated();
        }
        Ok(has_more)
    }

    /// Stores one authenticated remote batch and applies all currently eligible events.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn accept_remote_events(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        sync_settings: &SyncSettings,
        device_id: &str,
        events: Vec<ReceivedSyncEvent>,
        group: &GroupSecrets,
        source: &SyncConnectionTarget,
        latest_cursor: u64,
        allow_content: bool,
    ) -> Result<bool> {
        let _sync_activity = SyncActivityGuard::acquire();
        let removed_peers = repository::removed_peer_ids(pool).await?;
        let events = events
            .into_iter()
            .filter(|received| !removed_peers.contains(&received.event.origin_device_id))
            .collect::<Vec<_>>();
        let cloud_availability = if source.is_cloud() {
            let key = group.content_key_bytes()?;
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
        let encrypted_events = events
            .into_iter()
            .map(|received| received.event)
            .collect::<Vec<_>>();
        let stored = repository::store_remote_batch_with_availability(
            pool,
            source.remote_batch_source(),
            Some(latest_cursor),
            &encrypted_events,
            device_id,
            &cloud_availability,
        )
        .await?;
        for (event_id, origin_device_id) in stored.conflicts {
            log::warn!(
                "ignore sync event {event_id} with a reused origin sequence from {origin_device_id}"
            );
        }

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
        if stored.received_new_events && source.is_cloud() {
            self.wake_lan_transfer();
        }
        Ok(content_has_more)
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
        // LAN 入站请求与本机主动拉取可能同时看到同一事件；从查询到标记完成必须串行，
        // 否则同一远端事件会被并发写入剪贴板历史两次。
        let _apply_guard = self.apply_lock.lock().await;
        let key = group.content_key_bytes()?;
        let mut source_assets_pending = false;
        let mut latest_clipboard_item = None;
        #[cfg(target_os = "android")]
        let mut applied_any = false;

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

        if !allow_content {
            drop(_apply_guard);
            #[cfg(target_os = "android")]
            if applied_any {
                crate::commands::android::notify_overlay_clipboard_changed();
            }
            return Ok(false);
        }

        // Replaying still-unapplied content makes a download/apply interruption recoverable even
        // when the Hub no longer returns the duplicate event.
        let pending_apply = repository::unapplied_events_by_kind(
            pool,
            SyncEventKind::Content,
            MAX_EVENTS_PER_BATCH,
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

        // SQLite 写入按游标顺序串行完成，避免并发事务互相触发 SQLITE_BUSY；
        // 只有不持有数据库事务的 Blob 网络与文件 I/O 才并发执行。
        let mut ready_content = Vec::new();
        let mut metadata_only_content = Vec::new();
        let mut deduplicated_content = Vec::new();
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let local_storage_limit_bytes = self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .clipboard
            .history
            .max_storage_bytes;
        for (event, envelope) in pending_content {
            let presence = SyncedItemStatePatch {
                sync_key: envelope.item.sync_key.clone(),
                is_favorite: None,
                is_pinned: None,
                note: None,
                note_changed: false,
                is_present: Some(true),
            };
            let _resource_guard = crate::clipboard::shared_files::lock().await;
            let outcome = match db::sync_item_state::apply_remote_patch(
                pool,
                &presence,
                &db::sync_item_state::SyncEventVersion {
                    origin_sequence: event.origin_sequence,
                    origin_device_id: event.origin_device_id.clone(),
                },
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    log::warn!(
                        "defer synchronized content presence {}: {error:#}",
                        event.event_id
                    );
                    let delay =
                        repository::mark_apply_failed(pool, &event.event_id, &error.to_string())
                            .await?;
                    self.schedule_transfer_wake(delay);
                    continue;
                }
            };

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
                made_content_progress = true;
                continue;
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
            } else if should_defer_remote_file_download(
                &envelope,
                sync_settings.auto_download_max_mb,
                local_storage_limit_bytes,
            ) {
                metadata_only_content.push((event, envelope));
            } else {
                let item_id = if envelope.item.kind == "files" {
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
                    Some(item_id)
                } else {
                    None
                };
                ready_content.push((event, envelope, item_id));
            }
        }

        for (event, envelope) in metadata_only_content {
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            match self
                .materialize_remote_file_metadata_card(pool, &event, envelope, group)
                .await
            {
                Ok(_) => {
                    repository::record_inbound_outcome(
                        pool,
                        source.remote_batch_source(),
                        &event.event_id,
                        repository::InboundContentOutcome::Metadata,
                    )
                    .await?;
                    repository::mark_applied(pool, &event.event_id).await?;
                    made_content_progress = true;
                    source_assets_pending |= source_icon.is_some();
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                }
                Err(error) => {
                    log::warn!(
                        "defer metadata-only sync content event {}: {error:#}",
                        event.event_id
                    );
                    let delay =
                        repository::mark_apply_failed(pool, &event.event_id, &error.to_string())
                            .await?;
                    self.schedule_transfer_wake(delay);
                }
            }
        }

        for (event, envelope) in deduplicated_content {
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            match self
                .materialize_remote_file_metadata_card(pool, &event, envelope, group)
                .await
            {
                Ok(_) => {
                    repository::record_inbound_outcome(
                        pool,
                        source.remote_batch_source(),
                        &event.event_id,
                        repository::InboundContentOutcome::Deduplicated,
                    )
                    .await?;
                    repository::mark_applied(pool, &event.event_id).await?;
                    made_content_progress = true;
                    source_assets_pending |= source_icon.is_some();
                    #[cfg(target_os = "android")]
                    {
                        applied_any = true;
                    }
                }
                Err(error) => {
                    log::warn!(
                        "defer deduplicated sync content event {}: {error:#}",
                        event.event_id
                    );
                    let delay =
                        repository::mark_apply_failed(pool, &event.event_id, &error.to_string())
                            .await?;
                    self.schedule_transfer_wake(delay);
                }
            }
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
                            event.event_id, download_target, total_bytes
                        );
                        let handle = self.delivery.start(super::super::progress::TransferTask {
                            id: blake3::hash(task_key.as_bytes()).to_hex().to_string(),
                            revision: 0,
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
            .buffered_ordered(2);
        let mut preparations = std::pin::pin!(preparations);
        let mut consumed_staging = Vec::new();
        while let Some(prepared) = preparations.next().await {
            let Some(prepared) = prepared else {
                continue;
            };
            let (event, envelope, stored_blobs, blob_error, download, canceled) = prepared;
            if let Some(error) = blob_error {
                if canceled {
                    repository::mark_applied(pool, &event.event_id).await?;
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
                let delay = repository::mark_apply_failed(pool, &event.event_id, &error).await?;
                self.schedule_transfer_wake(delay);
                continue;
            }
            let source_icon = envelope.source_app.as_ref().and_then(valid_source_icon);
            let manifests = envelope.blobs.clone();
            let content_identity = envelope.content_identity.clone();
            match self.apply_envelope(&event, envelope, group).await {
                Ok((item_id, item, fingerprint, deduplicated)) => {
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
                        latest_clipboard_item = Some((item, fingerprint, deduplicated));
                    }
                    repository::record_inbound_outcome(
                        pool,
                        source.remote_batch_source(),
                        &event.event_id,
                        repository::InboundContentOutcome::Received,
                    )
                    .await?;
                    repository::mark_applied(pool, &event.event_id).await?;
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
                    log::warn!(
                        "defer unapplyable sync content event {}: {error:#}",
                        event.event_id
                    );
                    let delay =
                        repository::mark_apply_failed(pool, &event.event_id, &error.to_string())
                            .await?;
                    self.schedule_transfer_wake(delay);
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
                sync_settings.auto_write_clipboard,
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
        drop(_apply_guard);
        #[cfg(target_os = "android")]
        if applied_any {
            crate::commands::android::notify_overlay_clipboard_changed();
        }
        if source_assets_pending {
            self.runtime_state.source_asset_wake.notify_one();
        }
        Ok(made_content_progress
            && repository::has_unapplied_events_by_kind(pool, SyncEventKind::Content).await?)
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
        let source_revision = envelope
            .item
            .source_revision
            .clone()
            .unwrap_or_else(|| event.event_id.clone());
        let source_app_id = if let Some(source_app) = envelope.source_app.as_ref() {
            match self
                .resolve_received_source_app(&pool, group, source_app, updated_at, &source_revision)
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
        let _resource_guard = if kind == ClipboardKind::Text {
            None
        } else {
            Some(crate::clipboard::shared_files::lock().await)
        };
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let blob_root = resources.join(SYNC_STAGING_DIR);
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
                let directory = resources.join("sync-files").join(&event.event_id);
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
                let physical_content = paths.join("\n");
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
        let source_revision = envelope
            .item
            .source_revision
            .clone()
            .unwrap_or_else(|| event.event_id.clone());
        let pool = self.pool().await;
        let source_app_id = if let Some(source_app) = envelope.source_app.as_ref() {
            match self
                .resolve_received_source_app(&pool, group, source_app, updated_at, &source_revision)
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
        .filter(|blob| matches!(blob.role, BlobRole::Image | BlobRole::File))
        .all(|blob| available_blob_ids.contains(blob.blob_id.as_str()));
    Some(if complete {
        repository::RouteContentAvailability::Complete
    } else {
        repository::RouteContentAvailability::Metadata
    })
}
