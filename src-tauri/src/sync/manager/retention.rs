use super::*;

impl SyncManager {
    /// Applies local history retention while transfers are paused, then reclaims encrypted content
    /// that no current route can still need.
    pub(crate) async fn run_local_retention_cleanup(&self, trigger: CleanupTrigger) -> bool {
        let _cloud_guard = if trigger == CleanupTrigger::CloudBootstrap {
            None
        } else {
            Some(self.runtime_state.cloud_cycle_lock.lock().await)
        };
        let _lan_guard = self.runtime_state.lan_cycle_lock.write().await;
        let _enqueue_guard = self.delivery.lock_enqueue().await;
        let _apply_guard = self.apply_lock.lock().await;
        let _files_guard = crate::clipboard::shared_files::lock().await;
        let pool = self.pool().await;
        match crate::clipboard::cleanup::apply_retention(&self.app, self, &pool, trigger).await {
            Ok(()) => true,
            Err(error) => {
                log::warn!("local retention cleanup failed: {error:#}");
                false
            }
        }
    }

    /// Reclaims encrypted events and blobs after their last local reference disappears.
    pub(crate) async fn reclaim_released_sync_content(&self, pool: &SqlitePool) -> Result<()> {
        let mut removed_events = 0_u64;
        loop {
            let removed =
                repository::prune_released_content_events(pool, LOCAL_SYNC_GC_BATCH_SIZE).await?;
            removed_events = removed_events.saturating_add(removed);
            if removed < u64::from(LOCAL_SYNC_GC_BATCH_SIZE) {
                break;
            }
        }

        let live_blob_ids = repository::live_sync_blob_ids(pool).await?;
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let blob_root = resources.join(SYNC_STAGING_DIR);
        let removed_blobs = tauri::async_runtime::spawn_blocking(move || {
            remove_unreferenced_sync_staging(&blob_root, &live_blob_ids)
        })
        .await??;
        if removed_events > 0 || removed_blobs > 0 {
            log::info!(
                "local sync cleanup removed {removed_events} event(s) and {removed_blobs} blob(s)"
            );
        }
        Ok(())
    }

    /// Removes reproducible ciphertext after all currently known deliveries finish.
    pub(super) async fn release_staging_for_events(
        &self,
        pool: &SqlitePool,
        event_ids: &[String],
    ) -> Result<()> {
        for (blob_id, path) in repository::releasable_staging_for_events(pool, event_ids).await? {
            self.delivery.request_staging_release(&blob_id, path);
        }
        Ok(())
    }

    /// Publishes current protection before uploads and repairs old Hub metadata in bounded pages.
    pub(super) async fn refresh_cloud_retention(
        &self,
        connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        announcement: &DeviceAnnouncement,
        max_pages: usize,
    ) -> Result<bool> {
        let key = group.content_key_bytes()?;
        let device = self.cloud_device_announcement_for(announcement);
        let language = crate::i18n::current_language(&self.app);
        for _ in 0..max_pages {
            let response = call_cloud(
                connection,
                Request::RetentionPlan {
                    group_id: group.group_id.clone(),
                    access_token: group.access_token_bytes()?,
                    decoder_revision: Some(crypto::RETENTION_DECODER_REVISION),
                },
                language,
            )
            .await?;
            let Response::RetentionPlan {
                epoch,
                events,
                has_more,
            } = response
            else {
                if let Response::Error { message, .. } = response {
                    bail!(message);
                }
                bail!("invalid cloud retention plan");
            };
            let mut mappings = Vec::with_capacity(events.len());
            let mut deferred_event_ids = Vec::new();
            let mut first_decode_error = None;
            for stored in &events {
                match crypto::retention_mapping(&key, &stored.event) {
                    Ok(mapping) => {
                        let patch = match crypto::decrypt_sync_event(&key, &stored.event)? {
                            DecryptedSyncEvent::ItemState(patch) => patch,
                            DecryptedSyncEvent::Clipboard(envelope) => SyncedItemStatePatch {
                                sync_key: envelope.item.sync_key,
                                is_favorite: None,
                                is_pinned: None,
                                note: None,
                                note_changed: false,
                                is_present: Some(true),
                            },
                        };
                        // Do not learn a newer tombstone halfway through an older content download.
                        let _apply_guard = self.apply_lock.lock().await;
                        db::sync_item_state::merge_retention_state(
                            pool,
                            &patch,
                            &db::sync_item_state::SyncEventVersion {
                                origin_sequence: stored.event.origin_sequence,
                                origin_device_id: stored.event.origin_device_id.clone(),
                            },
                        )
                        .await?;
                        mappings.push(mapping);
                    }
                    Err(error) => {
                        deferred_event_ids.push(stored.event.event_id.clone());
                        first_decode_error.get_or_insert_with(|| error.to_string());
                    }
                }
            }
            let made_progress = !mappings.is_empty() || !deferred_event_ids.is_empty();
            let deferred_count = deferred_event_ids.len();
            let mut protected_keys = repository::protected_sync_keys(pool)
                .await?
                .iter()
                .map(|sync_key| crypto::retention_key(&key, sync_key))
                .collect::<Vec<_>>();
            protected_keys.sort_unstable();
            protected_keys.dedup();
            let fingerprint = blake3::hash(&minicbor::to_vec((
                &epoch,
                &device.device_id,
                &protected_keys,
            ))?)
            .to_hex()
            .to_string();
            let changed = repository::cloud_retention_snapshot(pool).await?.as_deref()
                != Some(fingerprint.as_str());
            if changed || made_progress {
                let generation = if changed {
                    repository::next_origin_sequence(pool).await?
                } else {
                    0
                };
                let response = call_cloud(
                    connection,
                    Request::SetRetention {
                        group_id: group.group_id.clone(),
                        access_token: group.access_token_bytes()?,
                        device: device.clone(),
                        generation,
                        protected_keys: changed.then_some(protected_keys),
                        mappings,
                        deferred: (!deferred_event_ids.is_empty()).then_some(
                            DeferredRetentionEvents {
                                decoder_revision: crypto::RETENTION_DECODER_REVISION,
                                event_ids: deferred_event_ids,
                            },
                        ),
                    },
                    language,
                )
                .await?;
                match response {
                    Response::RetentionUpdated => {
                        if let Some(error) = first_decode_error {
                            log::warn!("deferred unsupported cloud history: count={deferred_count} decoderRevision={} firstError={error}", crypto::RETENTION_DECODER_REVISION);
                        }
                        if changed {
                            repository::set_cloud_retention_snapshot(pool, &fingerprint).await?;
                        }
                    }
                    Response::Error { message, .. } => bail!(message),
                    _ => bail!("invalid cloud retention acknowledgement"),
                }
            }
            if !has_more || !made_progress {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Rechecks one bounded page of legacy delivery claims without pulling attachments or resetting sync.
    pub(super) async fn reconcile_legacy_cloud_deliveries(
        self: &Arc<Self>,
        connection: &Connection,
        pool: &SqlitePool,
        device_id: &str,
        group: &GroupSecrets,
    ) -> Result<bool> {
        let Some(before_cursor) =
            repository::cloud_confirmation_repair_cursor(pool, device_id).await?
        else {
            return Ok(false);
        };
        let response = call_cloud(
            connection,
            Request::ListEvents {
                group_id: group.group_id.clone(),
                access_token: group.access_token_bytes()?,
                before_cursor,
                limit: MAX_EVENTS_PER_BATCH,
                include_states: Some(true),
            },
            crate::i18n::current_language(&self.app),
        )
        .await?;
        let Response::EventsPage {
            events,
            next_before_cursor,
            ..
        } = response
        else {
            if let Response::Error { message, .. } = response {
                bail!(message);
            }
            bail!("invalid cloud confirmation history response");
        };
        let key = group.content_key_bytes()?;
        let mut verified = Vec::new();
        for stored in events {
            if let Err(error) = crypto::decrypt_sync_event(&key, &stored.event) {
                log::warn!("skip invalid cloud confirmation event: error={error}");
                continue;
            }
            verified.push(stored.event);
        }
        repository::repair_cloud_confirmations(pool, &verified, next_before_cursor).await?;
        self.emit_updated();
        Ok(next_before_cursor.is_some())
    }
}
