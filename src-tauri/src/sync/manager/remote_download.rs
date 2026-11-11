use super::*;

struct RemoteFileDownloadResult {
    completed_routes: HashSet<super::super::progress::TransferRoute>,
    external_destinations: Vec<repository::ExternalDownloadTarget>,
}

impl SyncManager {
    /// Prompts for an external destination, then downloads a metadata-only file card in background.
    pub async fn download_remote_files(
        self: &Arc<Self>,
        item_id: String,
        preferred_route: Option<super::super::progress::TransferRoute>,
    ) -> Result<Option<super::super::progress::TransferTask>> {
        let pool = self.pool().await;
        let item = db::items::find_item_by_id(&pool, &item_id)
            .await?
            .context("剪贴板记录不存在")?;
        if item.kind != ClipboardKind::Files
            || item.remote_file_state.as_deref() != Some("available")
        {
            bail!("该文件记录不需要下载");
        }
        let stored = repository::remote_file_event(&pool, &item_id)
            .await?
            .context("远端文件事件已不可用")?;
        let group = self
            .identity
            .snapshot()
            .group
            .context("请先创建或加入同步设备组")?;
        let decrypted = crypto::decrypt_sync_event(&group.content_key_bytes()?, &stored.event)?;
        let DecryptedSyncEvent::Clipboard(envelope) = decrypted else {
            bail!("远端文件事件类型无效");
        };
        validate_envelope(&envelope)?;
        let mut manifests = envelope
            .blobs
            .iter()
            .filter(|blob| blob.role == BlobRole::File)
            .cloned()
            .collect::<Vec<_>>();
        manifests.sort_by_key(|blob| blob.file_index.unwrap_or(u32::MAX));
        if manifests.is_empty() {
            bail!("远端文件元数据为空");
        }

        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let origin_device_id = &stored.event.origin_device_id;
        let status = repository::item_statuses(&pool, std::slice::from_ref(&item_id))
            .await?
            .into_iter()
            .next()
            .context("远端文件状态不存在")?;
        let cloud_content_available =
            status.cloud.availability == SyncContentAvailability::Complete;
        let active_lan_connection = self.cached_lan_connection(origin_device_id).is_some();
        let discovered_origin = repository::list_peers(&pool)
            .await?
            .into_iter()
            .find(|peer| peer.announcement.device_id == origin_device_id.as_str())
            .is_some_and(|peer| peer_endpoint_addr(&peer.announcement).is_ok());
        let route = preferred_route.unwrap_or_else(|| {
            if settings.lan_enabled && active_lan_connection {
                super::super::progress::TransferRoute::Lan
            } else if cloud_content_available && server_is_configured(&settings) {
                super::super::progress::TransferRoute::Cloud
            } else if settings.lan_enabled
                && (discovered_origin || !server_is_configured(&settings))
            {
                super::super::progress::TransferRoute::Lan
            } else {
                super::super::progress::TransferRoute::Cloud
            }
        });
        if route == super::super::progress::TransferRoute::Cloud && !server_is_configured(&settings)
        {
            bail!("云端同步未配置");
        }
        if route == super::super::progress::TransferRoute::Lan && !settings.lan_enabled {
            bail!(crate::i18n::commands::label(
                crate::i18n::current_language(&self.app),
                crate::i18n::commands::Key::SyncLanDisabled,
            ));
        }
        let Some(claim) = self.delivery.claim_remote_download(&item_id) else {
            return Ok(None);
        };
        let destination = match self.pick_remote_download_destination(&manifests).await {
            Ok(Some(destination)) => destination,
            Ok(None) => {
                drop(claim);
                self.wake_transfer();
                return Ok(None);
            }
            Err(error) => {
                drop(claim);
                self.wake_transfer();
                return Err(error);
            }
        };
        if let Err(error) = repository::mark_applied(&pool, &stored.event.event_id).await {
            drop(claim);
            self.wake_transfer();
            return Err(error);
        }
        let use_lan = route == super::super::progress::TransferRoute::Lan;
        let target = if use_lan {
            stored.event.origin_device_id.clone()
        } else {
            CLOUD_TARGET.to_owned()
        };
        let total_bytes = manifests.iter().fold(0_u64, |total, manifest| {
            total.saturating_add(manifest.encrypted_size)
        });
        let task_id = Uuid::new_v4().simple().to_string();
        let task = super::super::progress::TransferTask {
            id: task_id,
            revision: 0,
            operation_id: stored.event.event_id.clone(),
            item_id: item_id.clone(),
            direction: super::super::progress::TransferDirection::Download,
            route,
            target,
            file_name: (manifests.len() == 1).then(|| manifests[0].name.clone()),
            phase: super::super::progress::TransferPhase::Queued,
            transferred_bytes: 0,
            total_bytes,
            speed_bytes_per_second: 0,
            elapsed_ms: 0,
            eta_seconds: None,
            error: None,
            cancellable: true,
        };
        repository::lock_transfer_item(&pool, &task.id, &item_id).await?;
        let handle = self.delivery.start(task.clone());
        let cleanup_task_id = task.id.clone();
        let manager = self.clone();
        tauri::async_runtime::spawn(async move {
            let _claim = claim;
            let transferred = Arc::new(AtomicU64::new(0));
            let result = manager
                .run_remote_file_download(
                    RemoteDownloadRequest {
                        event: &stored.event,
                        group: &group,
                        manifests: &manifests,
                        destination,
                        route,
                    },
                    &handle,
                    &transferred,
                )
                .await;
            let transferred = transferred.load(Ordering::Acquire);
            let pool = manager.pool().await;
            match result {
                Ok(download) => {
                    let mut lan_receipt_recorded = false;
                    for completed_route in &download.completed_routes {
                        let delivered_by_lan =
                            *completed_route == super::super::progress::TransferRoute::Lan;
                        if let Err(error) = repository::complete_remote_file_download(
                            &pool,
                            &stored.event.origin_device_id,
                            &stored.event.event_id,
                            delivered_by_lan,
                        )
                        .await
                        {
                            log::warn!("record completed remote file download failed: {error:#}");
                        } else if delivered_by_lan {
                            lan_receipt_recorded = true;
                        }
                    }
                    if let Err(error) = repository::record_external_download_session(
                        &pool,
                        &stored.event.event_id,
                        &download.external_destinations,
                    )
                    .await
                    {
                        log::warn!("record external download destinations failed: {error:#}");
                    }
                    if lan_receipt_recorded {
                        manager.wake_lan_transfer();
                    }
                    manager.emit_updated();
                    #[cfg(target_os = "android")]
                    crate::commands::android::notify_overlay_clipboard_changed();
                    handle.completed(total_bytes);
                }
                Err(_error) if handle.canceled() => handle.mark_canceled(transferred),
                Err(error) => handle.failed(transferred, error.to_string()),
            }
            if let Err(error) = repository::unlock_transfer_item(&pool, &cleanup_task_id).await {
                log::warn!("release remote file transfer protection failed: {error:#}");
            }
        });
        Ok(Some(task))
    }

    #[cfg(not(target_os = "android"))]
    async fn pick_remote_download_destination(
        &self,
        manifests: &[BlobManifest],
    ) -> Result<Option<RemoteDownloadDestination>> {
        use crate::i18n::commands::{label, Key};

        let app = self.app.clone();
        let language = crate::i18n::current_language(&self.app);
        let single_name = (manifests.len() == 1 && !manifests[0].is_directory_archive)
            .then(|| sanitize_name(&manifests[0].name));
        tauri::async_runtime::spawn_blocking(move || {
            crate::window::set_clipboard_window_auto_hide_suspended(true);
            let result = if let Some(single_name) = single_name {
                let mut dialog = app
                    .dialog()
                    .file()
                    .set_can_create_directories(true)
                    .set_file_name(single_name)
                    .set_title(label(language, Key::RemoteFileSave));
                if let Ok(download_dir) = app.path().download_dir() {
                    dialog = dialog.set_directory(download_dir);
                }
                dialog
                    .blocking_save_file()
                    .map(|path| path.into_path())
                    .transpose()
                    .map(|path| path.map(RemoteDownloadDestination::Single))
                    .with_context(|| label(language, Key::RemoteFileInvalidDestination))
            } else {
                let mut dialog = app
                    .dialog()
                    .file()
                    .set_can_create_directories(true)
                    .set_title(label(language, Key::RemoteFileSaveDirectory));
                if let Ok(download_dir) = app.path().download_dir() {
                    dialog = dialog.set_directory(download_dir);
                }
                dialog
                    .blocking_pick_folder()
                    .map(|path| path.into_path())
                    .transpose()
                    .map(|path| path.map(RemoteDownloadDestination::Directory))
                    .with_context(|| label(language, Key::RemoteFileInvalidDirectory))
            };
            crate::window::set_clipboard_window_auto_hide_suspended(false);
            result
        })
        .await
        .context("remote file destination picker task failed")?
    }

    #[cfg(target_os = "android")]
    async fn pick_remote_download_destination(
        &self,
        _manifests: &[BlobManifest],
    ) -> Result<Option<RemoteDownloadDestination>> {
        Ok(tauri::async_runtime::spawn_blocking(move || {
            crate::commands::android::pick_android_remote_download_target()
                .map(|target| target.map(RemoteDownloadDestination::Android))
        })
        .await
        .context("Android remote file destination picker task failed")??)
    }

    async fn run_remote_file_download(
        self: &Arc<Self>,
        request: RemoteDownloadRequest<'_>,
        handle: &super::super::progress::TransferHandle,
        transferred: &AtomicU64,
    ) -> Result<RemoteFileDownloadResult> {
        let RemoteDownloadRequest {
            event,
            group,
            manifests,
            destination,
            route,
        } = request;
        handle.connecting();
        let (mut active_route, mut connection) =
            match self.remote_file_connection(event, route).await {
                Ok(connection) => (route, connection),
                Err(error) => {
                    let Some(fallback) = self
                        .fallback_remote_file_connection(event, route, handle, 0)
                        .await?
                    else {
                        return Err(error);
                    };
                    fallback
                }
            };
        let key = group.content_key_bytes()?;
        let mut completed_bytes = 0_u64;
        let mut completed_route_counts = HashMap::new();
        let mut external_destinations = Vec::new();
        #[cfg(not(target_os = "android"))]
        let mut committed_downloads = Vec::new();
        #[cfg(target_os = "android")]
        let mut committed_uris = Vec::new();
        let result = async {
            for manifest in manifests {
                if handle.canceled() {
                    bail!("transfer canceled");
                }
                #[cfg(not(target_os = "android"))]
                {
                    let target = match &destination {
                        RemoteDownloadDestination::Single(path) => path.clone(),
                        RemoteDownloadDestination::Directory(root) => {
                            non_conflicting_external_path(root, &sanitize_name(&manifest.name))
                        }
                    };
                    let mut retried = false;
                    let mut only_unavailable = true;
                    let commit = loop {
                        let attempt = download_blob_to_external_path(
                            manifest,
                            &target,
                            RemoteBlobTransferContext {
                                connection: &connection,
                                group,
                                key: &key,
                                completed_bytes,
                                handle,
                                transferred,
                            },
                        )
                        .await;
                        match attempt {
                            Ok(commit) => break commit,
                            Err(error)
                                if !retried
                                    && !handle.canceled()
                                    && error
                                        .downcast_ref::<RemoteBlobTransportFailure>()
                                        .is_some() =>
                            {
                                only_unavailable &= error
                                    .downcast_ref::<RemoteBlobTransportFailure>()
                                    .is_some_and(|failure| failure.unavailable);
                                transferred.store(completed_bytes, Ordering::Release);
                                let Some((fallback_route, fallback_connection)) = self
                                    .fallback_remote_file_connection(
                                        event,
                                        active_route,
                                        handle,
                                        completed_bytes,
                                    )
                                    .await?
                                else {
                                    return Err(conclude_remote_blob_failure(
                                        error,
                                        only_unavailable,
                                    ));
                                };
                                active_route = fallback_route;
                                connection = fallback_connection;
                                retried = true;
                            }
                            Err(error) => {
                                return Err(conclude_remote_blob_failure(error, only_unavailable));
                            }
                        }
                    };
                    external_destinations.push(repository::ExternalDownloadTarget {
                        destination_kind: "path".to_owned(),
                        destination: commit.destination().to_string_lossy().into_owned(),
                        expected_size: manifest.original_size,
                        is_directory: manifest.is_directory_archive,
                    });
                    committed_downloads.push(commit);
                    if handle.canceled() {
                        bail!("transfer canceled");
                    }
                }
                #[cfg(target_os = "android")]
                {
                    let RemoteDownloadDestination::Android(target) = &destination;
                    let mut retried = false;
                    let mut only_unavailable = true;
                    let committed_uri = loop {
                        let attempt = download_blob_to_android_destination(
                            manifest,
                            target,
                            RemoteBlobTransferContext {
                                connection: &connection,
                                group,
                                key: &key,
                                completed_bytes,
                                handle,
                                transferred,
                            },
                        )
                        .await;
                        match attempt {
                            Ok(committed_uri) => break committed_uri,
                            Err(error)
                                if !retried
                                    && !handle.canceled()
                                    && error
                                        .downcast_ref::<RemoteBlobTransportFailure>()
                                        .is_some() =>
                            {
                                only_unavailable &= error
                                    .downcast_ref::<RemoteBlobTransportFailure>()
                                    .is_some_and(|failure| failure.unavailable);
                                transferred.store(completed_bytes, Ordering::Release);
                                let Some((fallback_route, fallback_connection)) = self
                                    .fallback_remote_file_connection(
                                        event,
                                        active_route,
                                        handle,
                                        completed_bytes,
                                    )
                                    .await?
                                else {
                                    return Err(conclude_remote_blob_failure(
                                        error,
                                        only_unavailable,
                                    ));
                                };
                                active_route = fallback_route;
                                connection = fallback_connection;
                                retried = true;
                            }
                            Err(error) => {
                                return Err(conclude_remote_blob_failure(error, only_unavailable));
                            }
                        }
                    };
                    external_destinations.push(repository::ExternalDownloadTarget {
                        destination_kind: "uri".to_owned(),
                        destination: committed_uri.clone(),
                        expected_size: manifest.original_size,
                        is_directory: manifest.is_directory_archive,
                    });
                    committed_uris.push(committed_uri);
                    if handle.canceled() {
                        bail!("transfer canceled");
                    }
                }
                completed_bytes = completed_bytes.saturating_add(manifest.encrypted_size);
                transferred.store(completed_bytes, Ordering::Release);
                let completed = completed_route_counts
                    .entry(active_route)
                    .or_insert(0_usize);
                *completed = completed.saturating_add(1);
            }
            if handle.canceled() {
                bail!("transfer canceled");
            }
            if !handle.begin_finalizing(completed_bytes) {
                bail!("transfer canceled");
            }
            let completed_routes = completed_route_counts
                .into_iter()
                .filter_map(|(route, count)| (count == manifests.len()).then_some(route))
                .collect();
            Ok(RemoteFileDownloadResult {
                completed_routes,
                external_destinations,
            })
        }
        .await;

        match result {
            Ok(completed_routes) => {
                #[cfg(not(target_os = "android"))]
                finalize_external_downloads(committed_downloads).await;
                Ok(completed_routes)
            }
            Err(error) => {
                #[cfg(not(target_os = "android"))]
                rollback_external_downloads(committed_downloads).await;
                #[cfg(target_os = "android")]
                for uri in committed_uris.into_iter().rev() {
                    let result = tauri::async_runtime::spawn_blocking(move || {
                        crate::commands::android::discard_android_remote_download_document(&uri)
                    })
                    .await
                    .context("Android remote batch rollback task failed");
                    if let Err(rollback_error) =
                        result.and_then(|value| value.map_err(anyhow::Error::from))
                    {
                        log::warn!(
                            "rollback partial Android remote download failed: {rollback_error:#}"
                        );
                    }
                }
                Err(error)
            }
        }
    }

    /// Tries the other configured transport while keeping one logical download task.
    async fn fallback_remote_file_connection(
        self: &Arc<Self>,
        event: &EncryptedEvent,
        failed_route: super::super::progress::TransferRoute,
        handle: &super::super::progress::TransferHandle,
        transferred_bytes: u64,
    ) -> Result<Option<(super::super::progress::TransferRoute, Connection)>> {
        if handle.canceled() {
            bail!("transfer canceled");
        }
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        let fallback_route = match failed_route {
            super::super::progress::TransferRoute::Lan if server_is_configured(&settings) => {
                super::super::progress::TransferRoute::Cloud
            }
            super::super::progress::TransferRoute::Cloud if settings.lan_enabled => {
                super::super::progress::TransferRoute::Lan
            }
            _ => return Ok(None),
        };
        let target = match fallback_route {
            super::super::progress::TransferRoute::Lan => event.origin_device_id.clone(),
            super::super::progress::TransferRoute::Cloud => CLOUD_TARGET.to_owned(),
        };
        handle.reroute(fallback_route, target, transferred_bytes);
        let connection = self.remote_file_connection(event, fallback_route).await?;
        if handle.canceled() {
            bail!("transfer canceled");
        }
        Ok(Some((fallback_route, connection)))
    }

    async fn remote_file_connection(
        self: &Arc<Self>,
        event: &EncryptedEvent,
        route: super::super::progress::TransferRoute,
    ) -> Result<Connection> {
        let settings = self.app.state::<SettingsStore>().snapshot().sync;
        if route == super::super::progress::TransferRoute::Cloud {
            if !server_is_configured(&settings) {
                bail!("云端同步未配置");
            }
            return self.request_cloud_connection(Duration::from_secs(8)).await;
        }
        if !settings.lan_enabled {
            bail!(crate::i18n::commands::label(
                crate::i18n::current_language(&self.app),
                crate::i18n::commands::Key::SyncLanDisabled,
            ));
        }
        let pool = self.pool().await;
        let peer = repository::list_peers(&pool)
            .await?
            .into_iter()
            .find(|peer| peer.announcement.device_id == event.origin_device_id)
            .context("来源设备当前不可达")?;
        let address = peer_endpoint_addr(&peer.announcement)?;
        if let Some(connection) = self.cached_lan_connection(&event.origin_device_id) {
            if lan_connection_matches_discovery(&connection, &address) {
                return Ok(connection);
            }
            self.invalidate_lan_connection(&event.origin_device_id, connection.stable_id());
            connection.close(0_u32.into(), b"LAN route no longer discovered");
        }
        let endpoint = self.ensure_endpoint(&settings).await?;
        let connection = connect_peer(&endpoint, address, Duration::from_secs(8)).await?;
        super::protocol_handlers::spawn_peer_connection_service(self.clone(), connection.clone());
        self.store_lan_connection(&event.origin_device_id, connection.clone());
        Ok(connection)
    }
}
