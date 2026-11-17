use super::*;

fn is_missing_local_source_error(error: &anyhow::Error) -> bool {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
            || cause
                .downcast_ref::<walkdir::Error>()
                .and_then(|walk| walk.io_error())
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    }) {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("does not exist")
        || message.contains("no such file")
        || message.contains("cannot find the path")
        || message.contains("系统找不到")
        || message.contains("不存在")
        || message.contains("没有可同步的文件")
}

#[derive(Clone)]
enum FileUploadLocation {
    Path(PathBuf),
    #[cfg(target_os = "android")]
    AndroidUri(String),
}

#[derive(Clone)]
struct FileUploadSource {
    location: FileUploadLocation,
    name: String,
    is_directory: bool,
    #[cfg(target_os = "android")]
    expected_size: Option<u64>,
}

impl FileUploadSource {
    fn locator(&self) -> String {
        match &self.location {
            FileUploadLocation::Path(path) => path.to_string_lossy().into_owned(),
            #[cfg(target_os = "android")]
            FileUploadLocation::AndroidUri(uri) => uri.clone(),
        }
    }

    fn path(&self) -> Option<&Path> {
        match &self.location {
            FileUploadLocation::Path(path) => Some(path),
            #[cfg(target_os = "android")]
            FileUploadLocation::AndroidUri(_) => None,
        }
    }
}

impl SyncManager {
    async fn resolve_materialization_scope(
        &self,
        cycle: Option<&SyncCycleContext>,
    ) -> Option<(SyncSettings, SqlitePool, String, GroupSecrets)> {
        let settings = cycle
            .map(|cycle| cycle.settings.clone())
            .unwrap_or_else(|| self.app.state::<SettingsStore>().snapshot().sync);
        if !settings.enabled {
            return None;
        }
        let pool = if let Some(cycle) = cycle {
            cycle.pool.clone()
        } else {
            self.pool().await
        };
        let (device_id, group) = if let Some(cycle) = cycle {
            (cycle.device_id.clone(), cycle.group.clone())
        } else {
            let identity = self.identity.snapshot();
            let group = identity.group?;
            (identity.device_id, group)
        };
        Some((settings, pool, device_id, group))
    }

    /// Materializes dirty per-item metadata state patches independently of attachment backlogs.
    pub(super) async fn materialize_dirty_states(
        &self,
        cycle: Option<&SyncCycleContext>,
    ) -> Result<usize> {
        let Some((settings, pool, device_id, group)) =
            self.resolve_materialization_scope(cycle).await
        else {
            return Ok(0);
        };
        let _enqueue_guard = self.delivery.lock_materialization(None).await;
        let max_operations = usize::from(MAX_EVENTS_PER_BATCH) * MAX_SYNC_BATCHES_PER_CONNECTION;
        let mut processed = 0_usize;

        let materialize_result: Result<()> = async {
            for _ in 0..max_operations {
                let Some(state) =
                    db::sync_item_state::dirty_states(&pool, 1, settings.sync_sensitive)
                        .await?
                        .into_iter()
                        .next()
                else {
                    break;
                };
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
                processed += 1;
            }
            Ok(())
        }
        .await;

        if processed > 0 {
            self.wake_transfer();
            self.emit_updated();
        }
        materialize_result?;
        Ok(processed)
    }

    /// Materializes durable content outbox operations for a single payload lane.
    pub(super) async fn materialize_content_lane(
        &self,
        lane: db::sync_queue::OutboxContentLane,
        max_content: usize,
        preparation_target: Option<&PreparationTarget>,
        cycle: Option<&SyncCycleContext>,
    ) -> Result<MaterializationRun> {
        let mut staging = self.delivery.new_staging_cleanup();
        if max_content == 0 {
            return Ok(MaterializationRun {
                processed: 0,
                hit_limit: false,
                staging,
            });
        }
        let Some((settings, pool, device_id, group)) =
            self.resolve_materialization_scope(cycle).await
        else {
            return Ok(MaterializationRun {
                processed: 0,
                hit_limit: false,
                staging,
            });
        };
        let _enqueue_guard = self.delivery.lock_materialization(Some(lane)).await;
        let mut processed = 0_usize;
        let mut stored_any = false;
        let mut hit_limit = false;
        let mut materialized_content = 0_usize;
        let max_operations = usize::from(MAX_EVENTS_PER_BATCH) * MAX_SYNC_BATCHES_PER_CONNECTION;

        let materialize_result: Result<()> = async {
            for _ in 0..max_operations {
                let Some(operation) =
                    db::sync_queue::next_ready_content(&pool, settings.sync_sensitive, Some(lane))
                        .await?
                else {
                    break;
                };
                processed += 1;
                match self
                    .materialize_content_operation(
                        &pool,
                        &operation,
                        ContentMaterializationContext {
                            settings: &settings,
                            device_id: &device_id,
                            group: &group,
                            preparation_target,
                        },
                    )
                    .await
                {
                    Ok(ContentMaterialization::Completed) => {}
                    Ok(ContentMaterialization::Stored {
                        staging: content_staging,
                        has_attachments,
                    }) => {
                        stored_any = true;
                        staging.extend(content_staging);
                        materialized_content += 1;
                        // Attachment preparation yields after one record so a backlog never
                        // starves active exchange or pre-builds staging copies out of order.
                        if has_attachments || materialized_content >= max_content {
                            hit_limit = true;
                            break;
                        }
                    }
                    Err(error) => {
                        if operation.history_backfill
                            || (!operation.manual_share && is_missing_local_source_error(&error))
                        {
                            log::warn!(
                                "skip unavailable local sync content preparation: operationOrder={} backfill={} error={error:#}",
                                operation.operation_order,
                                operation.history_backfill,
                            );
                            db::sync_queue::complete_content(&pool, operation.operation_order)
                                .await?;
                            self.discard_pending_observation(
                                &operation.item_id,
                                &operation.source_revision,
                            );
                        } else {
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
                            self.emit_updated();
                        }
                    }
                }
            }
            if !hit_limit && processed >= max_operations {
                hit_limit = true;
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
            hit_limit,
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
        let item_updated_at_ms = item.updated_at.timestamp_millis();
        let created_at_ms = item_updated_at_ms.min(Utc::now().timestamp_millis());
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
                payload_kind: kind_string(item.kind),
                sync_key: &item.sync_key,
                payload_updated_at_ms: item_updated_at_ms,
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
        self.delivery.complete_local_import_for_item(&item.id);
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
        Ok(ContentMaterialization::Stored {
            staging,
            has_attachments: !envelope.blobs.is_empty(),
        })
    }

    pub(super) fn discard_pending_observation(&self, item_id: &str, source_revision: &str) {
        self.pending_observations
            .lock()
            .expect("pending clipboard observations poisoned")
            .remove(&(item_id.to_owned(), source_revision.to_owned()));
    }

    #[cfg(not(target_os = "android"))]
    async fn file_upload_sources(
        &self,
        pool: &SqlitePool,
        item: &ClipboardItem,
        resources: &Path,
    ) -> Result<Vec<FileUploadSource>> {
        if item.remote_file_state.is_none() {
            let paths = item
                .content
                .lines()
                .filter(|value| !value.is_empty())
                .map(|value| crate::clipboard::shared_files::resolve_path(resources, value))
                .collect::<Result<Vec<_>>>()?;
            return Ok(paths
                .into_iter()
                .map(|path| FileUploadSource {
                    name: safe_file_name(&path),
                    is_directory: path.is_dir(),
                    location: FileUploadLocation::Path(path),
                })
                .collect());
        }

        let unavailable = crate::i18n::commands::label(
            crate::i18n::current_language(&self.app),
            crate::i18n::commands::Key::RemoteFileNotDownloaded,
        );
        let session = repository::external_download_sessions(pool, std::slice::from_ref(&item.id))
            .await?
            .into_iter()
            .next()
            .context(unavailable)?;
        let mut sources = Vec::with_capacity(session.targets.len());
        for target in session.targets {
            if target.destination_kind != "path"
                || !super::external_download_target_available(&target).await
            {
                bail!("{unavailable}");
            }
            let path = PathBuf::from(target.destination);
            sources.push(FileUploadSource {
                name: safe_file_name(&path),
                is_directory: target.is_directory,
                location: FileUploadLocation::Path(path),
            });
        }
        Ok(sources)
    }

    #[cfg(target_os = "android")]
    async fn file_upload_sources(
        &self,
        pool: &SqlitePool,
        item: &ClipboardItem,
        resources: &Path,
    ) -> Result<Vec<FileUploadSource>> {
        if item.remote_file_state.is_none() {
            let paths = item
                .content
                .lines()
                .filter(|value| !value.is_empty())
                .map(|value| crate::clipboard::shared_files::resolve_path(resources, value))
                .collect::<Result<Vec<_>>>()?;
            return Ok(paths
                .into_iter()
                .map(|path| FileUploadSource {
                    name: safe_file_name(&path),
                    is_directory: path.is_dir(),
                    location: FileUploadLocation::Path(path),
                    expected_size: None,
                })
                .collect());
        }

        let unavailable = crate::i18n::commands::label(
            crate::i18n::current_language(&self.app),
            crate::i18n::commands::Key::RemoteFileNotDownloaded,
        );
        let names = item
            .content
            .lines()
            .filter(|value| !value.is_empty())
            .map(sanitize_name)
            .collect::<Vec<_>>();
        let session = repository::external_download_sessions(pool, std::slice::from_ref(&item.id))
            .await?
            .into_iter()
            .next()
            .context(unavailable)?;
        if names.len() != session.targets.len() {
            bail!("{unavailable}");
        }
        let mut sources = Vec::with_capacity(session.targets.len());
        for (name, target) in names.into_iter().zip(session.targets) {
            if target.destination_kind != "uri"
                || target.is_directory
                || !super::external_download_target_available(&target).await
            {
                bail!("{unavailable}");
            }
            sources.push(FileUploadSource {
                location: FileUploadLocation::AndroidUri(target.destination),
                name,
                is_directory: false,
                expected_size: Some(target.expected_size),
            });
        }
        Ok(sources)
    }

    async fn fingerprint_upload_source(
        &self,
        source: &FileUploadSource,
        preparation: Option<&PreparationProgress>,
        progress_base: u64,
    ) -> Result<(u64, String)> {
        let preparation = preparation.cloned();
        match &source.location {
            FileUploadLocation::Path(path) => {
                let path = path.clone();
                tauri::async_runtime::spawn_blocking(move || {
                    let size = path.metadata()?.len();
                    let content_hash = crypto::hash_file_with_progress(
                        &path,
                        || {
                            preparation
                                .as_ref()
                                .is_some_and(|progress| progress.handle.canceled())
                        },
                        |processed| {
                            if let Some(progress) = &preparation {
                                progress.report(progress_base.saturating_add(processed));
                            }
                        },
                    )?;
                    Ok::<_, anyhow::Error>((size, content_hash))
                })
                .await
                .context("file hashing task failed")?
            }
            #[cfg(target_os = "android")]
            FileUploadLocation::AndroidUri(uri) => {
                let uri = uri.clone();
                let expected_size = source.expected_size;
                tauri::async_runtime::spawn_blocking(move || {
                    let mut file =
                        crate::commands::android::open_android_remote_download_source(&uri)?;
                    let (size, content_hash) = crypto::fingerprint_reader_with_progress(
                        &mut file,
                        || {
                            preparation
                                .as_ref()
                                .is_some_and(|progress| progress.handle.canceled())
                        },
                        |processed| {
                            if let Some(progress) = &preparation {
                                progress.report(progress_base.saturating_add(processed));
                            }
                        },
                    )?;
                    if expected_size.is_some_and(|expected| expected != size) {
                        bail!("Android upload source size changed");
                    }
                    Ok::<_, anyhow::Error>((size, content_hash))
                })
                .await
                .context("Android file hashing task failed")?
            }
        }
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
        let _resource_guard = if item.kind != ClipboardKind::Image {
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
                let original_size = tokio::fs::metadata(&source)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to stat local sync image source {}",
                            source.display()
                        )
                    })?
                    .len();
                let expected_encrypted_size = crypto::encrypted_blob_size(original_size)?;
                let reusable = repository::reusable_content_object(
                    pool,
                    &group.group_id,
                    &item.content_hash,
                    original_size,
                )
                .await?
                .filter(|object| object.encrypted_size == expected_encrypted_size)
                .map(|object| (object.blob_id, object.stream_nonce));
                let source_for_task = source.clone();
                let root_for_task = blob_root.clone();
                let mut blob =
                    tauri::async_runtime::spawn_blocking(move || -> Result<StoredBlob> {
                        if let Some((expected_blob_id, stream_nonce)) = reusable {
                            if let Ok(rebuilt) = crypto::encrypt_blob_with_stream_nonce(
                                &source_for_task,
                                &root_for_task,
                                &key,
                                &stream_nonce,
                            ) {
                                if rebuilt.blob_id == expected_blob_id
                                    && rebuilt.size == expected_encrypted_size
                                {
                                    return Ok(rebuilt);
                                }
                                if rebuilt.blob_id != expected_blob_id {
                                    let _ = fs::remove_file(&rebuilt.encrypted_path);
                                }
                            }
                        }
                        crypto::encrypt_blob(&source_for_task, &root_for_task, &key)
                    })
                    .await
                    .context("image encryption task failed")??;
                staging.track(&blob);
                blob.source_path = Some(source.to_string_lossy().into_owned());
                if let Some(stream_nonce) = blob.stream_nonce.clone() {
                    content_objects.push(StoredContentObject {
                        plaintext_hash: item.content_hash.clone(),
                        blob_id: blob.blob_id.clone(),
                        stream_nonce,
                        original_size,
                        encrypted_size: blob.size,
                    });
                }
                manifests.push(BlobManifest {
                    blob_id: blob.blob_id.clone(),
                    name: item.content.clone(),
                    original_size,
                    encrypted_size: blob.size,
                    role: BlobRole::Image,
                    file_index: None,
                    is_directory_archive: false,
                    stream_commitment: false,
                });
                stored_blobs.push(blob);
            }
            ClipboardKind::Files => {
                let paths = self.file_upload_sources(pool, item, &resources).await?;
                if paths.is_empty() {
                    bail!("没有可同步的文件");
                }
                let mut packed_total_size = 0_u64;
                let mut file_fingerprints = Vec::with_capacity(paths.len());
                for (index, source) in paths.into_iter().enumerate() {
                    let name = source.name.clone();
                    let is_directory = source.is_directory;
                    let progress_base = preparation
                        .map(PreparationProgress::processed)
                        .unwrap_or(packed_total_size);
                    let (mut blob, original_size, content_size, content_hash, content_object) =
                        if is_directory {
                            let source_for_task = source
                                .path()
                                .context("directory upload source is unavailable")?
                                .to_path_buf();
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
                            let (size, content_hash) = self
                                .fingerprint_upload_source(&source, preparation, progress_base)
                                .await?;
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
                                    source_path: Some(source.locator()),
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
                    blob.source_path = Some(source.locator());
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
                    source_device_id: item.source_device_id.clone(),
                    source_device_name: item.source_device_name.clone(),
                    source_updated_at_ms: item
                        .source_updated_at
                        .map(|value| value.timestamp_millis()),
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

    /// Computes total byte size for a local file entry and rejects items with missing paths.
    async fn backfill_file_item_size(
        &self,
        pool: &SqlitePool,
        item: &ClipboardItem,
        sources: &[FileUploadSource],
        resources: &Path,
    ) -> Result<Option<u64>> {
        for source in sources {
            if let Some(path) = source.path() {
                if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                    return Ok(None);
                }
            }
        }
        if let Some(size) = item.size.and_then(|value| u64::try_from(value).ok()) {
            return Ok(Some(size));
        }
        #[cfg(target_os = "android")]
        if item.remote_file_state.is_some() {
            let total = sources
                .iter()
                .filter_map(|source| source.expected_size)
                .fold(0_u64, u64::saturating_add);
            return Ok(Some(total));
        }
        match calculate_current_file_item_size(pool, item, resources).await {
            Ok(Some(size)) => Ok(u64::try_from(size).ok()),
            Ok(None) | Err(_) => Ok(None),
        }
    }

    /// Recovers local history omitted before captures were retained while synchronization paused.
    pub(super) async fn backfill_local_history(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
    ) -> Result<()> {
        if repository::history_backfill_completed(pool, &group.group_id).await? {
            return Ok(());
        }

        struct BackfillCandidate {
            id: String,
            sync_key: String,
            source_revision: String,
        }

        let snapshot = self.app.state::<SettingsStore>().snapshot();
        let sync_sensitive = snapshot.sync.sync_sensitive;
        let auto_upload_max_mb = snapshot.sync.auto_upload_max_mb;
        let history = snapshot.clipboard.history;
        let cutoff = crate::clipboard::cleanup::retention_cutoff(&history.retention, Utc::now());
        let max_count = (history.max_count > 0).then_some(history.max_count as usize);
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let image_store = self.app.state::<ImageStore>();

        let item_ids = db::items::local_item_ids_for_sync(pool).await?;
        let mut selected = Vec::new();
        let mut valid_count = 0_usize;
        for item_id in item_ids {
            let Some(item) = db::items::find_item_by_id(pool, &item_id).await? else {
                continue;
            };
            if !item.is_favorite && !item.is_pinned {
                if cutoff.is_some_and(|cutoff| item.created_at < cutoff)
                    || max_count.is_some_and(|limit| valid_count >= limit)
                {
                    continue;
                }
                valid_count += 1;
            }
            if item.is_sensitive && !sync_sensitive {
                continue;
            }
            match item.kind {
                ClipboardKind::Text => selected.push(BackfillCandidate {
                    id: item.id,
                    sync_key: item.sync_key,
                    source_revision: item.source_revision,
                }),
                ClipboardKind::Image => {
                    let path = image_store.origin_path(&item.content);
                    let Ok(metadata) = tokio::fs::metadata(&path).await else {
                        continue;
                    };
                    if !metadata.is_file() {
                        continue;
                    }
                    selected.push(BackfillCandidate {
                        id: item.id,
                        sync_key: item.sync_key,
                        source_revision: item.source_revision,
                    });
                }
                ClipboardKind::Files => {
                    let Ok(sources) = self.file_upload_sources(pool, &item, &resources).await
                    else {
                        continue;
                    };
                    if sources.is_empty() {
                        continue;
                    }
                    let Some(size) = self
                        .backfill_file_item_size(pool, &item, &sources, &resources)
                        .await?
                    else {
                        continue;
                    };
                    if permits_automatic_file_upload(size, auto_upload_max_mb)
                        || sources.iter().all(|source| !source.is_directory)
                    {
                        selected.push(BackfillCandidate {
                            id: item.id,
                            sync_key: item.sync_key,
                            source_revision: item.source_revision,
                        });
                    }
                }
            }
        }
        for candidate in selected.into_iter().rev() {
            db::sync_queue::stage_existing_content(
                pool,
                &candidate.id,
                &candidate.sync_key,
                &candidate.source_revision,
                db::sync_queue::ContentSyncOptions {
                    history_backfill: true,
                    ..Default::default()
                },
            )
            .await?;
        }
        repository::mark_history_backfill_completed(pool, &group.group_id).await?;
        self.notify_local_outbox();
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
