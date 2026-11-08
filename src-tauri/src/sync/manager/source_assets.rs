use super::*;

impl SyncManager {
    /// Creates or refreshes the local alias before the clipboard row references it.
    pub(super) async fn resolve_received_source_app(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        source: &SourceAppRef,
        source_updated_at: chrono::DateTime<Utc>,
        source_revision: &str,
    ) -> Result<String> {
        validate_source_app_ref(source)?;
        let icon = source
            .icon
            .as_ref()
            .filter(|icon| validate_source_icon_ref(icon).is_ok());
        let icon_file = icon.and_then(|icon| {
            self.app
                .state::<AppIconStore>()
                .icon_file_for_hash(&icon.icon_hash)
        });
        let icon_hash = icon_file
            .as_ref()
            .and_then(|_| icon.map(|icon| icon.icon_hash.clone()));
        let now = Utc::now();
        let app = ClipboardApp {
            id: String::new(),
            name: sanitize_source_app_name(&source.display_name),
            icon_file,
            icon_hash,
            accent_start: source.accent_start.clone(),
            accent_end: source.accent_end.clone(),
            platform: parse_platform(&source.platform),
            created_at: now,
            updated_at: now,
        };
        let icon = icon.map(|icon| {
            (
                icon.icon_hash.as_str(),
                icon.blob_id.as_str(),
                icon.original_size,
                icon.encrypted_size,
            )
        });
        let app = db::apps::resolve_synced_app(
            pool,
            &group.group_id,
            &source.source_key,
            app,
            icon,
            source_updated_at,
            source_revision,
        )
        .await?;
        Ok(app.id)
    }

    /// Retries all missing source icons through the currently available peer or Hub connection.
    pub(super) async fn refresh_source_app_assets(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
    ) -> Result<usize> {
        let pool = self.pool().await;
        let store = self.app.state::<AppIconStore>();
        let mut pending_count = 0;
        for asset in db::apps::source_app_assets(&pool, &group.group_id).await? {
            let icon = SourceIconRef {
                icon_hash: asset.icon_hash.clone(),
                blob_id: asset.blob_id.clone(),
                original_size: asset.original_size,
                encrypted_size: asset.encrypted_size,
            };
            let encrypted_available =
                self.source_icon_blob_record(&icon)
                    .ok()
                    .is_some_and(|encrypted| {
                        Path::new(&encrypted.encrypted_path)
                            .metadata()
                            .is_ok_and(|metadata| metadata.len() == encrypted.size)
                    });
            if encrypted_available
                && asset.is_attached
                && store.icon_file_for_hash(&asset.icon_hash).is_some()
            {
                continue;
            }
            pending_count += 1;
            if let Err(error) = self
                .refresh_source_app_asset(connection, group, &pool, &asset)
                .await
            {
                log::debug!(
                    "source app icon {} remains pending: {error}",
                    asset.icon_hash
                );
            }
        }
        Ok(pending_count)
    }

    /// Publishes queued local source icons and removes them only after the Hub acknowledges them.
    pub(super) async fn publish_pending_source_icons(
        self: &Arc<Self>,
        connection: Option<&Connection>,
        group: &GroupSecrets,
    ) -> Result<usize> {
        let Some(connection) = connection else {
            return Ok(0);
        };
        if !self.cloud_async_source_icons_supported(connection, &group.group_id) {
            return Ok(0);
        }

        let pool = self.pool().await;
        let blobs = repository::pending_source_icon_uploads(
            &pool,
            &group.group_id,
            MAX_SOURCE_ICON_UPLOADS_PER_BATCH,
        )
        .await?;
        if blobs.is_empty() {
            return Ok(0);
        }

        let blob_ids = blobs
            .iter()
            .map(|blob| blob.blob_id.clone())
            .collect::<Vec<_>>();
        let blob_paths = blobs
            .iter()
            .map(|blob| PathBuf::from(&blob.encrypted_path))
            .collect::<Vec<_>>();
        upload_blobs(
            self.clone(),
            connection,
            group,
            blobs,
            BlobUploadOptions {
                kind: BlobUploadKind::PublishedSourceIcon,
                managed_hub_blob: self.bounded_hub_supported(connection, &group.group_id),
                is_cloud: true,
                language: crate::i18n::current_language(&self.app),
                started: None,
                transfer: None,
            },
        )
        .await?;
        repository::complete_source_icon_uploads(&pool, &group.group_id, &blob_ids).await?;
        for (blob_id, path) in blob_ids.iter().zip(blob_paths) {
            self.delivery.request_staging_release(blob_id, path);
        }
        self.runtime_state
            .confirmed_cloud_source_icons
            .write()
            .expect("confirmed cloud source icons poisoned")
            .confirm(connection.stable_id(), &group.group_id, &blob_ids);
        Ok(blob_ids.len())
    }

    pub(super) async fn refresh_source_app_asset(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
        pool: &SqlitePool,
        asset: &db::apps::SourceAppAsset,
    ) -> Result<()> {
        let icon = SourceIconRef {
            icon_hash: asset.icon_hash.clone(),
            blob_id: asset.blob_id.clone(),
            original_size: asset.original_size,
            encrypted_size: asset.encrypted_size,
        };
        let blob = self
            .ensure_source_icon_blob(connection, group, &icon)
            .await?;
        let encrypted = PathBuf::from(&blob.encrypted_path);
        let _staging_cleanup = TemporaryBlobCleanup {
            path: encrypted.clone(),
            armed: true,
        };

        let store = self.app.state::<AppIconStore>();
        let metadata = match store.synced_metadata_for_hash(&asset.icon_hash) {
            Ok(metadata) => metadata,
            Err(_) => {
                let temporary = tempfile::tempdir()?;
                let destination = temporary.path().join("source-icon.png");
                let encrypted_for_task = encrypted.clone();
                let destination_for_task = destination.clone();
                let key = group.content_key_bytes()?;
                let original_size = asset.original_size;
                tauri::async_runtime::spawn_blocking(move || {
                    crypto::decrypt_blob(
                        &encrypted_for_task,
                        &destination_for_task,
                        original_size,
                        &key,
                    )
                })
                .await
                .context("source app icon decryption task failed")??;
                let bytes = fs::read(destination)?;
                store.store_synced_png(&bytes, &asset.icon_hash)?
            }
        };
        let updated = db::apps::update_synced_app_icon(
            pool,
            &asset.app_id,
            &metadata.file_name,
            &metadata.icon_hash,
            &metadata.accent_start,
            &metadata.accent_end,
        )
        .await?;
        if !updated {
            return Ok(());
        }
        if let Err(error) = self.app.emit(
            crate::clipboard::SOURCE_APP_UPDATED_EVENT,
            serde_json::json!({ "appId": asset.app_id }),
        ) {
            log::warn!("emit source app update failed: {error}");
        }
        #[cfg(target_os = "android")]
        crate::commands::android::notify_overlay_clipboard_changed();
        Ok(())
    }

    pub(super) async fn ensure_source_icon_blob(
        &self,
        connection: Option<&Connection>,
        group: &GroupSecrets,
        icon: &SourceIconRef,
    ) -> Result<StoredBlob> {
        let expected = self.source_icon_blob_record(icon)?;
        let manifest = BlobManifest {
            blob_id: icon.blob_id.clone(),
            name: format!("{}.png", icon.icon_hash),
            original_size: icon.original_size,
            encrypted_size: icon.encrypted_size,
            role: BlobRole::Image,
            file_index: None,
            is_directory_archive: false,
            stream_commitment: false,
        };
        let encrypted = PathBuf::from(&expected.encrypted_path);
        if encrypted.is_file() {
            let is_valid = encrypted.metadata()?.len() == icon.encrypted_size
                && crypto::hash_file(&encrypted)? == icon.blob_id;
            if is_valid {
                return Ok(expected);
            }
            fs::remove_file(&encrypted)
                .with_context(|| format!("remove invalid source icon blob {encrypted:?}"))?;
        }

        let store = self.app.state::<AppIconStore>();
        if let Some(file_name) = store.icon_file_for_hash(&icon.icon_hash) {
            let source = store.icon_path(&file_name);
            let root = crate::core::paths::resources_dir(&self.app)?.join(SYNC_STAGING_DIR);
            let root_for_task = root.clone();
            let key = group.content_key_bytes()?;
            let icon_hash = icon.icon_hash.clone();
            let blob = tauri::async_runtime::spawn_blocking(move || {
                crypto::encrypt_stable_blob(&source, &root_for_task, &key, &icon_hash)
            })
            .await
            .context("cached source app icon encryption task failed")??;
            if blob.blob_id == icon.blob_id && blob.size == icon.encrypted_size {
                return Ok(blob);
            }
            log::warn!("cached source app icon does not match synchronized blob reference");
        }

        let connection = connection.context("source app icon blob is unavailable")?;
        download_blob(connection, group, &manifest, &encrypted, None).await?;
        Ok(expected)
    }

    pub(super) fn source_icon_blob_record(&self, icon: &SourceIconRef) -> Result<StoredBlob> {
        validate_source_icon_ref(icon)?;
        let root = crate::core::paths::resources_dir(&self.app)?.join(SYNC_STAGING_DIR);
        let encrypted = crypto::blob_path(&root, &icon.blob_id)?;
        Ok(StoredBlob {
            blob_id: icon.blob_id.clone(),
            encrypted_path: encrypted.to_string_lossy().into_owned(),
            size: icon.encrypted_size,
            source_path: None,
            source_is_directory: false,
            stream_nonce: None,
        })
    }
}
