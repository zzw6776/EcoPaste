use super::*;

impl SyncManager {
    /// Materializes durable local operations while keeping blob work inside an active connection.
    pub(super) async fn materialize_local_operations(
        &self,
        allow_content: bool,
        max_content: usize,
        preparation_target: Option<&PreparationTarget>,
        cycle: Option<&SyncCycleContext>,
    ) -> Result<MaterializationRun> {
        let mut staging = self.delivery.new_staging_cleanup();
        let settings = cycle
            .map(|cycle| cycle.settings.clone())
            .unwrap_or_else(|| self.app.state::<SettingsStore>().snapshot().sync);
        if !settings.enabled {
            return Ok(MaterializationRun {
                processed: 0,
                deferred_content: false,
                staging,
            });
        }
        let pool = if let Some(cycle) = cycle {
            cycle.pool.clone()
        } else {
            self.pool().await
        };
        let _enqueue_guard = self.delivery.lock_enqueue().await;
        let (device_id, group) = if let Some(cycle) = cycle {
            (cycle.device_id.clone(), cycle.group.clone())
        } else {
            let identity = self.identity.snapshot();
            let Some(group) = identity.group else {
                return Ok(MaterializationRun {
                    processed: 0,
                    deferred_content: false,
                    staging,
                });
            };
            (identity.device_id, group)
        };
        let mut processed = 0;
        let mut stored_any = false;
        let mut deferred_content = false;
        let mut materialized_content = 0_usize;
        let max_operations = usize::from(MAX_EVENTS_PER_BATCH) * MAX_SYNC_BATCHES_PER_CONNECTION;

        let materialize_result: Result<()> = async {
            for _ in 0..max_operations {
                let content =
                    db::sync_queue::next_ready_content(&pool, settings.sync_sensitive).await?;
                let state = db::sync_item_state::dirty_states(&pool, 1, settings.sync_sensitive)
                    .await?
                    .into_iter()
                    .next();
                let materialize_content = match (&content, &state) {
                    (Some(content), Some(state)) => content.operation_order <= state.pending_order,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => break,
                };

                if materialize_content {
                    let operation = content.expect("content operation selected above");
                    match self
                        .materialize_content_operation(
                            &pool,
                            &operation,
                            ContentMaterializationContext {
                                settings: &settings,
                                device_id: &device_id,
                                group: &group,
                                allow_content,
                                preparation_target,
                            },
                        )
                        .await
                    {
                        Ok(ContentMaterialization::Completed) => {}
                        Ok(ContentMaterialization::Deferred) => {
                            deferred_content = true;
                            break;
                        }
                        Ok(ContentMaterialization::Stored(content_staging)) => {
                            stored_any = true;
                            staging.extend(content_staging);
                            materialized_content += 1;
                        }
                        Err(error) => {
                            let retry_delay = db::sync_queue::fail_content(
                                &pool,
                                operation.operation_order,
                                &error.to_string(),
                            )
                            .await?;
                            log::warn!(
                                "defer local sync content preparation: operationOrder={} retryInSecs={} error={error:#}",
                                operation.operation_order,
                                retry_delay.num_seconds(),
                            );
                            self.schedule_transfer_wake(retry_delay);
                        }
                    }
                    if materialized_content >= max_content {
                        processed += 1;
                        break;
                    }
                } else {
                    let state = state.expect("item-state operation selected above");
                    let sequence = repository::next_origin_sequence(&pool).await?;
                    let event = crypto::encrypt_item_state(
                        &group.content_key_bytes()?,
                        Uuid::new_v4().simple().to_string(),
                        device_id.clone(),
                        sequence,
                        Utc::now().timestamp_millis(),
                        &state.patch(),
                    )?;
                    repository::commit_item_state_event(&pool, &event, &state).await?;
                    stored_any = true;
                }
                processed += 1;
            }
            Ok(())
        }
        .await;

        if stored_any {
            self.wake_transfer();
            self.emit_updated();
        }
        materialize_result?;
        Ok(MaterializationRun {
            processed,
            deferred_content,
            staging,
        })
    }

    async fn materialize_content_operation(
        &self,
        pool: &SqlitePool,
        operation: &db::sync_queue::ContentOutboxOperation,
        context: ContentMaterializationContext<'_>,
    ) -> Result<ContentMaterialization> {
        let ContentMaterializationContext {
            settings,
            device_id,
            group,
            allow_content,
            preparation_target,
        } = context;
        let Some(item) = db::items::find_item_by_id(pool, &operation.item_id).await? else {
            db::sync_queue::complete_content(pool, operation.operation_order).await?;
            self.discard_pending_observation(&operation.item_id, &operation.source_revision);
            return Ok(ContentMaterialization::Completed);
        };
        if item.source_revision != operation.source_revision || item.sync_key != operation.sync_key
        {
            db::sync_queue::complete_content(pool, operation.operation_order).await?;
            self.discard_pending_observation(&operation.item_id, &operation.source_revision);
            return Ok(ContentMaterialization::Completed);
        }
        if let Some(stored) = repository::latest_local_event_for_item(pool, &item.id).await? {
            if !operation.manual_share
                && event_matches_item_timestamp(&stored.event, group.content_key_bytes()?, &item)
            {
                db::sync_queue::complete_content(pool, operation.operation_order).await?;
                self.discard_pending_observation(&operation.item_id, &operation.source_revision);
                return Ok(ContentMaterialization::Completed);
            }
        } else if operation.history_backfill
            && repository::event_for_item(pool, &item.id).await?.is_some()
        {
            db::sync_queue::complete_content(pool, operation.operation_order).await?;
            self.discard_pending_observation(&operation.item_id, &operation.source_revision);
            return Ok(ContentMaterialization::Completed);
        }
        if !allow_content {
            return Ok(ContentMaterialization::Deferred);
        }
        let event_id = uuid::Uuid::new_v4().simple().to_string();
        let preparation = if item.kind == ClipboardKind::Files {
            match preparation_target {
                Some(target)
                    if target.matches_delivery_route(operation.delivery_route.as_deref()) =>
                {
                    self.start_preparation_transfer(
                        pool,
                        operation,
                        &event_id,
                        &item,
                        target,
                        settings.auto_upload_max_mb,
                    )
                    .await?
                    .map(|handle| PreparationProgress::new(handle, target.clone()))
                }
                Some(_) | None => None,
            }
        } else {
            None
        };
        let envelope_result = self
            .build_envelope(pool, &event_id, &item, group, preparation.as_ref())
            .await;
        let (envelope, blobs, content_objects, fingerprint, staging) = match envelope_result {
            Err(error) => {
                if let Some(preparation) = &preparation {
                    if preparation.handle.canceled() {
                        db::sync_queue::complete_content(pool, operation.operation_order).await?;
                        self.discard_pending_observation(
                            &operation.item_id,
                            &operation.source_revision,
                        );
                        preparation.handle.mark_canceled(preparation.processed());
                        return Ok(ContentMaterialization::Completed);
                    } else {
                        preparation
                            .handle
                            .failed(preparation.processed(), error.to_string());
                    }
                }
                return Err(error);
            }
            Ok(value) => value,
        };
        let preparation_failure = crate::i18n::commands::label(
            crate::i18n::current_language(&self.app),
            crate::i18n::commands::Key::SyncPreparationFailed,
        );
        let mut preparation_guard = PreparationCommitGuard::new(preparation, preparation_failure);
        if item.kind == ClipboardKind::Files {
            if let Some(size) = envelope.item.size {
                if item.size != Some(size)
                    && db::items::update_file_item_size(pool, &item.id, &item.source_revision, size)
                        .await?
                {
                    if let Err(error) = self.app.emit(
                        crate::clipboard::CLIPBOARD_UPDATED_EVENT,
                        serde_json::json!({
                            "id": item.id,
                            "kind": item.kind,
                            "deduplicated": true,
                        }),
                    ) {
                        log::warn!("emit clipboard file size update failed: {error}");
                    }
                    #[cfg(target_os = "android")]
                    crate::commands::android::notify_overlay_clipboard_changed();
                }
            }
        }
        let sequence = repository::next_origin_sequence(pool).await?;
        let created_at_ms = Utc::now().timestamp_millis();
        let key = group.content_key_bytes()?;
        let event = crypto::encrypt_event(
            &key,
            event_id,
            device_id.to_owned(),
            sequence,
            created_at_ms,
            &envelope,
        )?;
        if !preparation_guard.begin_commit() {
            db::sync_queue::complete_content(pool, operation.operation_order).await?;
            self.discard_pending_observation(&operation.item_id, &operation.source_revision);
            if let Some(preparation) = &preparation_guard.progress {
                preparation.handle.mark_canceled(preparation.processed());
            }
            return Ok(ContentMaterialization::Completed);
        }
        repository::commit_content_outbox_event(
            pool,
            repository::ContentOutboxCommit {
                operation_order: operation.operation_order,
                item_id: &item.id,
                event: &event,
                blobs: &blobs,
                sync_key: &item.sync_key,
                is_sensitive: item.is_sensitive,
                delivery_route: operation.delivery_route.as_deref(),
                group_id: &group.group_id,
                content_objects: &content_objects,
                content_identity: envelope
                    .content_identity
                    .as_deref()
                    .context("missing synchronized content identity")?,
            },
        )
        .await?;
        preparation_guard.commit();
        let observation = self
            .pending_observations
            .lock()
            .expect("pending clipboard observations poisoned")
            .remove(&(operation.item_id.clone(), operation.source_revision.clone()));
        if let (Some(observation), Some(fingerprint)) = (observation.as_ref(), fingerprint) {
            self.app
                .state::<Arc<ClipboardFingerprintState>>()
                .commit_observation(observation, fingerprint);
        }
        #[cfg(not(target_os = "android"))]
        if let Some(fingerprint) = observation
            .as_ref()
            .and_then(ClipboardObservation::outbound_image)
        {
            self.app
                .state::<Arc<ClipboardFingerprintState>>()
                .remember_synced_image(fingerprint);
        }
        Ok(ContentMaterialization::Stored(staging))
    }

    pub(super) fn discard_pending_observation(&self, item_id: &str, source_revision: &str) {
        self.pending_observations
            .lock()
            .expect("pending clipboard observations poisoned")
            .remove(&(item_id.to_owned(), source_revision.to_owned()));
    }

    async fn build_envelope(
        &self,
        pool: &SqlitePool,
        _event_id: &str,
        item: &ClipboardItem,
        group: &GroupSecrets,
        preparation: Option<&PreparationProgress>,
    ) -> Result<(
        ClipboardEnvelope,
        Vec<StoredBlob>,
        Vec<StoredContentObject>,
        Option<ClipboardFingerprint>,
        TaskStagingCleanup,
    )> {
        let _sync_activity = SyncActivityGuard::acquire();
        let _resource_guard = if item.kind == ClipboardKind::Text {
            None
        } else {
            Some(crate::clipboard::shared_files::lock().await)
        };
        let key = group.content_key_bytes()?;
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let blob_root = resources.join(SYNC_STAGING_DIR);
        let mut manifests = Vec::new();
        let mut stored_blobs = Vec::new();
        let mut content_objects = Vec::new();
        let mut staging = self.delivery.new_staging_cleanup();
        let mut envelope_size = item.size;
        let mut clipboard_fingerprint = None;
        match item.kind {
            ClipboardKind::Text => {}
            ClipboardKind::Image => {
                let source = self.app.state::<ImageStore>().origin_path(&item.content);
                let source_for_task = source.clone();
                let root_for_task = blob_root.clone();
                let mut blob = tauri::async_runtime::spawn_blocking(move || {
                    crypto::encrypt_blob(&source_for_task, &root_for_task, &key)
                })
                .await
                .context("image encryption task failed")??;
                staging.track(&blob);
                blob.source_path = Some(source.to_string_lossy().into_owned());
                manifests.push(BlobManifest {
                    blob_id: blob.blob_id.clone(),
                    name: item.content.clone(),
                    original_size: source.metadata()?.len(),
                    encrypted_size: blob.size,
                    role: BlobRole::Image,
                    file_index: None,
                    is_directory_archive: false,
                    stream_commitment: false,
                });
                stored_blobs.push(blob);
            }
            ClipboardKind::Files => {
                let paths = item
                    .content
                    .lines()
                    .filter(|value| !value.is_empty())
                    .map(|value| crate::clipboard::shared_files::resolve_path(&resources, value))
                    .collect::<Result<Vec<_>>>()?;
                if paths.is_empty() {
                    bail!("没有可同步的文件");
                }
                let mut packed_total_size = 0_u64;
                let mut file_fingerprints = Vec::with_capacity(paths.len());
                for (index, source) in paths.into_iter().enumerate() {
                    let name = safe_file_name(&source);
                    let source_for_task = source.clone();
                    let is_directory = source.is_dir();
                    let progress_base = preparation
                        .map(PreparationProgress::processed)
                        .unwrap_or(packed_total_size);
                    let (mut blob, original_size, content_size, content_hash, content_object) =
                        if is_directory {
                            let root_for_task = blob_root.clone();
                            let key_for_task = key;
                            let preparation_for_task = preparation.cloned();
                            tauri::async_runtime::spawn_blocking(move || {
                                let temporary = tempfile::tempdir()?;
                                let archive_path = temporary.path().join("directory.zip");
                                let archived = crypto::archive_directory_with_progress(
                                    &source_for_task,
                                    &archive_path,
                                    || {
                                        preparation_for_task
                                            .as_ref()
                                            .is_some_and(|progress| progress.handle.canceled())
                                    },
                                    |processed| {
                                        if let Some(progress) = &preparation_for_task {
                                            progress
                                                .report(progress_base.saturating_add(processed));
                                        }
                                    },
                                )?;
                                let archive_size = archive_path.metadata()?.len();
                                let blob = crypto::encrypt_blob_with_cancel(
                                    &archive_path,
                                    &root_for_task,
                                    &key_for_task,
                                    || {
                                        preparation_for_task
                                            .as_ref()
                                            .is_some_and(|progress| progress.handle.canceled())
                                    },
                                )?;
                                Ok::<_, anyhow::Error>((
                                    blob,
                                    archive_size,
                                    archived.content_size,
                                    archived.fingerprint,
                                    None,
                                ))
                            })
                            .await
                            .context("directory encryption task failed")??
                        } else {
                            let preparation_for_task = preparation.cloned();
                            let (size, content_hash) =
                                tauri::async_runtime::spawn_blocking(move || {
                                    let size = source_for_task.metadata()?.len();
                                    let content_hash = crypto::hash_file_with_progress(
                                        &source_for_task,
                                        || {
                                            preparation_for_task
                                                .as_ref()
                                                .is_some_and(|progress| progress.handle.canceled())
                                        },
                                        |processed| {
                                            if let Some(progress) = &preparation_for_task {
                                                progress.report(
                                                    progress_base.saturating_add(processed),
                                                );
                                            }
                                        },
                                    )?;
                                    Ok::<_, anyhow::Error>((size, content_hash))
                                })
                                .await
                                .context("file hashing task failed")??;
                            let encrypted_size = crypto::encrypted_blob_size(size)?;
                            let reusable = repository::reusable_content_object(
                                pool,
                                &group.group_id,
                                &content_hash,
                                size,
                            )
                            .await?
                            .filter(|object| {
                                object.encrypted_size == encrypted_size
                                    && crypto::stream_blob_id(
                                        &key,
                                        &object.stream_nonce,
                                        &content_hash,
                                    )
                                    .is_ok_and(|blob_id| blob_id == object.blob_id)
                            });
                            let object = match reusable {
                                Some(object) => object,
                                None => {
                                    let stream_nonce = crypto::new_stream_nonce();
                                    StoredContentObject {
                                        blob_id: crypto::stream_blob_id(
                                            &key,
                                            &stream_nonce,
                                            &content_hash,
                                        )?,
                                        plaintext_hash: content_hash.clone(),
                                        stream_nonce,
                                        original_size: size,
                                        encrypted_size,
                                    }
                                }
                            };
                            (
                                StoredBlob {
                                    blob_id: object.blob_id.clone(),
                                    encrypted_path: String::new(),
                                    size: object.encrypted_size,
                                    source_path: Some(source.to_string_lossy().into_owned()),
                                    source_is_directory: false,
                                    stream_nonce: Some(object.stream_nonce.clone()),
                                },
                                size,
                                size,
                                content_hash,
                                Some(object),
                            )
                        };
                    staging.track(&blob);
                    if preparation.is_some_and(|progress| progress.handle.canceled()) {
                        bail!("transfer canceled");
                    }
                    blob.source_path = Some(source.to_string_lossy().into_owned());
                    blob.source_is_directory = is_directory;
                    packed_total_size = packed_total_size
                        .checked_add(content_size)
                        .context("clipboard file size overflow")?;
                    if is_directory {
                        if let Some(preparation) = preparation {
                            preparation.report(packed_total_size);
                        }
                    }
                    file_fingerprints.push(FileEntryFingerprint {
                        name: name.clone(),
                        is_directory,
                        content_hash,
                    });
                    if let Some(content_object) = content_object {
                        content_objects.push(content_object);
                    }
                    manifests.push(BlobManifest {
                        blob_id: blob.blob_id.clone(),
                        name,
                        original_size,
                        encrypted_size: blob.size,
                        role: BlobRole::File,
                        file_index: Some(index as u32),
                        is_directory_archive: is_directory,
                        stream_commitment: !is_directory,
                    });
                    stored_blobs.push(blob);
                }
                envelope_size = Some(
                    i64::try_from(packed_total_size)
                        .context("clipboard file size exceeds supported range")?,
                );
                clipboard_fingerprint = ClipboardFingerprint::from_file_entries(&file_fingerprints);
            }
        }
        let source_blob_start = stored_blobs.len();
        let source_app = self
            .build_source_app_ref(item, group, &blob_root, &mut stored_blobs)
            .await?;
        for blob in &stored_blobs[source_blob_start..] {
            staging.track(blob);
        }
        let content_identity = match item.kind {
            ClipboardKind::Files => clipboard_fingerprint
                .as_ref()
                .map(|fingerprint| fingerprint.as_str().to_owned())
                .context("missing synchronized file content identity")?,
            ClipboardKind::Text | ClipboardKind::Image => item.content_hash.clone(),
        };
        Ok((
            ClipboardEnvelope {
                version: 1,
                item: SyncedClipboardItem {
                    kind: kind_string(item.kind).into(),
                    sub_kind: item.sub_kind.map(sub_kind_string).map(str::to_owned),
                    content: item.content.clone(),
                    search_text: item.search_text.clone(),
                    summary: item.summary.clone(),
                    file_types: item.file_types.clone(),
                    size: envelope_size,
                    width: item.width,
                    height: item.height,
                    is_sensitive: item.is_sensitive,
                    source_platform: platform_string(item.platform).into(),
                    created_at_ms: item.created_at.timestamp_millis(),
                    sync_key: item.sync_key.clone(),
                    updated_at_ms: Some(item.updated_at.timestamp_millis()),
                    source_revision: Some(item.source_revision.clone()),
                },
                blobs: manifests,
                source_app,
                content_identity: Some(content_identity),
            },
            stored_blobs,
            content_objects,
            clipboard_fingerprint,
            staging,
        ))
    }

    /// Builds a privacy-preserving source reference and reuses the existing blob transport.
    async fn build_source_app_ref(
        &self,
        item: &ClipboardItem,
        group: &GroupSecrets,
        blob_root: &Path,
        stored_blobs: &mut Vec<StoredBlob>,
    ) -> Result<Option<SourceAppRef>> {
        let Some(app_id) = item.source_app_id.as_deref() else {
            return Ok(None);
        };
        let pool = self.pool().await;
        let Some(mut app) = db::apps::find_app_by_id(&pool, app_id).await? else {
            return Ok(None);
        };
        let key = group.content_key_bytes()?;
        let source_key = source_app_key(&key, app.platform, &app.id);
        let mut icon_ref = None;
        if let Some(icon_file) = app.icon_file.clone() {
            let store = self.app.state::<AppIconStore>();
            match store.refresh_metadata(&icon_file) {
                Ok(metadata) => {
                    app.icon_file = Some(metadata.file_name.clone());
                    app.icon_hash = Some(metadata.icon_hash.clone());
                    app.accent_start = Some(metadata.accent_start.clone());
                    app.accent_end = Some(metadata.accent_end.clone());
                    if let Err(error) = db::apps::upsert_app(&pool, &app).await {
                        log::warn!("persist normalized source app icon metadata failed: {error}");
                    }
                    let source = store.icon_path(&metadata.file_name);
                    let source_for_task = source.clone();
                    let root = blob_root.to_path_buf();
                    let hash = metadata.icon_hash.clone();
                    let encrypted = tauri::async_runtime::spawn_blocking(move || {
                        crypto::encrypt_stable_blob(&source_for_task, &root, &key, &hash)
                    })
                    .await;
                    match encrypted {
                        Ok(Ok(mut blob)) => {
                            blob.source_path = Some(source.to_string_lossy().into_owned());
                            icon_ref = Some(SourceIconRef {
                                icon_hash: metadata.icon_hash,
                                blob_id: blob.blob_id.clone(),
                                original_size: metadata.original_size,
                                encrypted_size: blob.size,
                            });
                            stored_blobs.push(blob);
                        }
                        Ok(Err(error)) => {
                            log::warn!("encrypt source app icon failed: {error}");
                        }
                        Err(error) => {
                            log::warn!("source app icon encryption task failed: {error}");
                        }
                    }
                }
                Err(error) => {
                    log::warn!("normalize source app icon {icon_file} failed: {error}");
                }
            }
        }
        let (fallback_start, fallback_end) = fallback_accent_colors(&source_key);
        Ok(Some(SourceAppRef {
            version: 1,
            source_key,
            platform: platform_string(app.platform).to_owned(),
            display_name: sanitize_source_app_name(&app.name),
            icon: icon_ref,
            accent_start: Some(app.accent_start.unwrap_or(fallback_start)),
            accent_end: Some(app.accent_end.unwrap_or(fallback_end)),
        }))
    }

    /// Creates sync events for the latest local history once per sync space.
    pub(super) async fn backfill_recent_history(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<()> {
        if repository::history_backfill_completed(pool, &group.group_id).await? {
            return Ok(());
        }

        let sync_sensitive = self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .sync
            .sync_sensitive;
        let item_ids = db::items::recent_item_ids_for_sync(pool, HISTORY_BACKFILL_LIMIT).await?;
        for item_id in item_ids.into_iter().rev() {
            let Some(item) = db::items::find_item_by_id(pool, &item_id).await? else {
                continue;
            };
            if item.is_sensitive && !sync_sensitive {
                continue;
            }
            db::sync_queue::stage_existing_content(
                pool,
                &item.id,
                &item.sync_key,
                &item.source_revision,
                db::sync_queue::ContentSyncOptions {
                    history_backfill: true,
                    ..Default::default()
                },
            )
            .await?;
        }
        repository::mark_history_backfill_completed(pool, &group.group_id).await?;
        self.runtime_state.local_outbox_wake.notify_one();
        self.emit_updated();
        Ok(())
    }

    /// Repairs records already applied by older clients from retained versioned events.
    pub(super) async fn repair_history_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<()> {
        if repository::history_timestamp_repair_completed(pool, &group.group_id).await? {
            return Ok(());
        }

        let linked = repository::linked_item_events(pool).await?;
        let reconciled = self
            .reconcile_linked_timestamps(pool, group, linked)
            .await?;
        repository::mark_history_timestamp_repair_completed(pool, &group.group_id).await?;
        if reconciled > 0 {
            self.emit_clipboard_reconciled();
        }
        Ok(())
    }

    pub(super) async fn reconcile_item_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        item_id: &str,
    ) -> Result<()> {
        let linked = repository::linked_events_for_item(pool, item_id)
            .await?
            .into_iter()
            .map(|stored| LinkedSyncEvent {
                item_id: item_id.to_owned(),
                stored,
            })
            .collect();
        let reconciled = self
            .reconcile_linked_timestamps(pool, group, linked)
            .await?;
        if reconciled > 0 {
            self.emit_clipboard_reconciled();
        }
        Ok(())
    }

    async fn reconcile_linked_timestamps(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        linked: Vec<LinkedSyncEvent>,
    ) -> Result<usize> {
        let key = group.content_key_bytes()?;
        let mut timestamps = HashMap::new();
        for linked_event in linked {
            let Ok(envelope) = crypto::decrypt_event(&key, &linked_event.stored.event) else {
                continue;
            };
            if envelope.item.updated_at_ms.is_none() {
                continue;
            }
            let (created_at, updated_at) =
                synced_item_timestamps(&envelope.item, linked_event.stored.event.created_at_ms);
            timestamps
                .entry(linked_event.item_id)
                .and_modify(
                    |current: &mut (chrono::DateTime<Utc>, chrono::DateTime<Utc>)| {
                        current.0 = current.0.min(created_at);
                        current.1 = current.1.max(updated_at);
                    },
                )
                .or_insert((created_at, updated_at));
        }
        let reconciled = timestamps.len();
        for (item_id, (created_at, updated_at)) in timestamps {
            db::items::set_synced_item_timestamps(pool, &item_id, created_at, updated_at).await?;
        }
        Ok(reconciled)
    }

    /// Notifies the list to reload after synchronized history or metadata changes.
    pub(super) fn emit_clipboard_reconciled(&self) {
        if let Err(error) = self.app.emit(
            crate::clipboard::CLIPBOARD_UPDATED_EVENT,
            serde_json::json!({ "reconciled": true }),
        ) {
            log::warn!("emit clipboard reconciliation update failed: {error}");
        }
    }
}
