use ecopaste_sync_protocol::{LanContentLevel, LanItemAck, SyncItemRecord, SyncItemWrite};
use sqlx::Row;

use super::*;

/// Freshness grace window for the dedicated VIP realtime download worker. Allows modest clock skew
/// between paired devices while keeping older historical catch-up strictly on the General worker.
const VIP_REALTIME_RECENCY_MS: i64 = 120_000;

#[derive(Default)]
pub(super) struct IngestBatchSummary {
    pub(super) needs_apply: bool,
    pub(super) needs_blob_pull: bool,
    pub(super) needs_repush: bool,
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
            sqlx::query_scalar::<_, Option<chrono::DateTime<Utc>>>(
                "SELECT MAX(updated_at) FROM clipboard_items",
            )
            .fetch_one(pool)
            .await
            .context("read latest local clipboard timestamp")?
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

    pub(super) fn is_historical(&self, item_updated_at: chrono::DateTime<Utc>) -> bool {
        self.latest_local_updated_at
            .is_some_and(|latest| item_updated_at < latest)
    }

    pub(super) async fn admit_or_reject_historical(
        &mut self,
        item_updated_at: chrono::DateTime<Utc>,
        attachment_bytes: u64,
    ) -> bool {
        if self.target_bytes == 0 || !self.is_historical(item_updated_at) {
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

struct RemotePayloadInput<'a> {
    item_key: &'a str,
    origin_device_id: &'a str,
    hub_rev: Option<u64>,
    remote_seq: u64,
    deleted: bool,
    protected: bool,
    nonce: &'a [u8],
    ciphertext: &'a [u8],
    available_blob_ids: &'a [String],
}

impl SyncManager {
    fn decrypt_local_or_fallback_snapshot(
        &self,
        meta: &repository::StoredSyncItemMeta,
        key: &[u8; 32],
        local_device_id: &str,
    ) -> SyncedItemSnapshot {
        if let (Some(item_key), Some(nonce), Some(ciphertext)) = (
            meta.item_key.as_deref(),
            meta.nonce.as_deref(),
            meta.ciphertext.as_deref(),
        ) {
            if let Ok(snapshot) = crypto::decrypt_item_snapshot(
                key,
                item_key,
                meta.deleted,
                meta.protected,
                nonce,
                ciphertext,
            ) {
                return snapshot;
            }
        }
        repository::snapshot_from_meta(meta, local_device_id)
    }

    /// Decrypts, validates, CRDT-merges, and persists one incoming remote snapshot.
    async fn ingest_single_remote_payload(
        &self,
        pool: &SqlitePool,
        key: &[u8; 32],
        local_device_id: &str,
        source_target_id: &str,
        removed_peers: &HashSet<String>,
        input: RemotePayloadInput<'_>,
    ) -> Result<Option<(SyncedItemSnapshot, bool, bool, bool)>> {
        if removed_peers.contains(input.origin_device_id) {
            return Ok(None);
        }
        let remote_snapshot = match crypto::decrypt_item_snapshot(
            key,
            input.item_key,
            input.deleted,
            input.protected,
            input.nonce,
            input.ciphertext,
        )
        .and_then(|snapshot| {
            validate_snapshot(&snapshot)?;
            Ok(snapshot)
        }) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::warn!(
                    "ignore invalid remote sync item {}: {error:#}",
                    &input.item_key[..input.item_key.len().min(12)]
                );
                return Ok(None);
            }
        };
        let expected_item_key = crypto::derive_item_key(key, &remote_snapshot.sync_key);
        if expected_item_key != input.item_key {
            log::warn!(
                "ignore remote sync item {} with mismatched derived item_key",
                &input.item_key[..input.item_key.len().min(12)]
            );
            return Ok(None);
        }

        let existing = repository::load_sync_item_meta(pool, &remote_snapshot.sync_key).await?;
        if let Some(existing_meta) = &existing {
            // If a newer local content capture is still encrypting in the background, only a
            // strictly newer remote deletion tombstone may supersede it before prep finishes.
            let local_preparing_newer_content = existing_meta.nonce.is_none()
                && !existing_meta.deleted
                && existing_meta.content_version > remote_snapshot.content_version;
            let remote_deletes = !remote_snapshot.is_present
                && remote_snapshot.presence_version > existing_meta.presence_version;
            if local_preparing_newer_content && !remote_deletes {
                return Ok(None);
            }
        }

        let (
            final_snapshot,
            final_nonce,
            final_ciphertext,
            needs_apply,
            needs_blob_wake,
            re_push_required,
        ) = if let Some(existing_meta) = &existing {
            let local_snapshot =
                self.decrypt_local_or_fallback_snapshot(existing_meta, key, local_device_id);
            let merged = super::super::model::merge_snapshots(&local_snapshot, &remote_snapshot);
            let newly_available_blobs = !merged.snapshot.deleted()
                && !merged.snapshot.blobs.is_empty()
                && existing_meta.local_state != "managed"
                && !input.available_blob_ids.is_empty();
            let needs_apply = merged.local_changed;
            let needs_blob_wake = !merged.snapshot.deleted()
                && (existing_meta.needs_blob_pull || newly_available_blobs);
            if merged.remote_changed {
                let (nonce, ciphertext) = crypto::encrypt_item_snapshot(
                    key,
                    input.item_key,
                    merged.snapshot.deleted(),
                    merged.snapshot.protected(),
                    &merged.snapshot,
                )?;
                (
                    merged.snapshot,
                    nonce,
                    ciphertext,
                    needs_apply,
                    needs_blob_wake,
                    true,
                )
            } else {
                (
                    merged.snapshot,
                    input.nonce.to_vec(),
                    input.ciphertext.to_vec(),
                    needs_apply,
                    needs_blob_wake,
                    false,
                )
            }
        } else {
            let has_blobs = !remote_snapshot.deleted() && !remote_snapshot.blobs.is_empty();
            (
                remote_snapshot,
                input.nonce.to_vec(),
                input.ciphertext.to_vec(),
                true,
                has_blobs,
                false,
            )
        };

        let sync_key = final_snapshot.sync_key.clone();
        repository::store_merged_remote_item(
            pool,
            repository::StoreMergedRemoteItem {
                sync_key: &sync_key,
                item_key: input.item_key,
                origin_device_id: input.origin_device_id,
                hub_rev: input.hub_rev,
                remote_seq: input.remote_seq,
                source_target_id,
                snapshot: &final_snapshot,
                nonce: &final_nonce,
                ciphertext: &final_ciphertext,
                available_blob_ids: input.available_blob_ids,
                needs_apply,
                re_push_required,
            },
        )
        .await?;

        Ok(Some((
            final_snapshot,
            needs_apply,
            needs_blob_wake,
            re_push_required,
        )))
    }

    /// Merges a batch of `SyncItemRecord` rows pulled from Cloud (`items` + `conflicts`) or LAN.
    pub(super) async fn ingest_remote_records(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        source_target_id: &str,
        records: &[SyncItemRecord],
        is_cloud: bool,
    ) -> Result<IngestBatchSummary> {
        let mut summary = IngestBatchSummary::default();
        if records.is_empty() {
            return Ok(summary);
        }
        let _sync_activity = SyncActivityGuard::acquire();
        let removed_peers = repository::removed_peer_ids(pool).await?;
        let key = group.content_key_bytes()?;
        let local_device_id = self.identity.snapshot().device_id;

        for record in records {
            let Some((snapshot, needs_apply, needs_blob_wake, re_push)) = self
                .ingest_single_remote_payload(
                    pool,
                    &key,
                    &local_device_id,
                    source_target_id,
                    &removed_peers,
                    RemotePayloadInput {
                        item_key: &record.item_key,
                        origin_device_id: &record.origin_device_id,
                        hub_rev: is_cloud.then_some(record.hub_rev),
                        remote_seq: if is_cloud { 0 } else { record.seq },
                        deleted: record.deleted,
                        protected: record.protected,
                        nonce: &record.nonce,
                        ciphertext: &record.ciphertext,
                        available_blob_ids: &record.available_blob_ids,
                    },
                )
                .await?
            else {
                continue;
            };
            summary.needs_apply |= needs_apply;
            summary.needs_repush |= re_push;
            if needs_blob_wake || (needs_apply && !snapshot.deleted() && !snapshot.blobs.is_empty())
            {
                summary.needs_blob_pull = true;
            }
        }
        Ok(summary)
    }

    /// Merges incoming LAN `SyncItemWrite` pushes, immediately applies inline items & file cards
    /// within the same 1-RTT frame, and returns per-item `LanItemAck` receipts.
    pub(super) async fn ingest_lan_writes(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_settings: &SyncSettings,
        peer_device_id: &str,
        writes: &[SyncItemWrite],
    ) -> Result<(Vec<LanItemAck>, bool, bool)> {
        if writes.is_empty() {
            return Ok((Vec::new(), false, false));
        }
        let _sync_activity = SyncActivityGuard::acquire();
        let removed_peers = repository::removed_peer_ids(pool).await?;
        let key = group.content_key_bytes()?;
        let local_device_id = self.identity.snapshot().device_id;
        let source_target_id = format!("peer:{peer_device_id}");
        let mut staged_acks = Vec::with_capacity(writes.len());
        let mut any_repush = false;

        for write in writes {
            let Some((snapshot, _needs_apply, _needs_blob_wake, re_push)) = self
                .ingest_single_remote_payload(
                    pool,
                    &key,
                    &local_device_id,
                    &source_target_id,
                    &removed_peers,
                    RemotePayloadInput {
                        item_key: &write.item_key,
                        origin_device_id: &write.origin_device_id,
                        hub_rev: None,
                        remote_seq: write.local_seq,
                        deleted: write.deleted,
                        protected: write.protected,
                        nonce: &write.nonce,
                        ciphertext: &write.ciphertext,
                        available_blob_ids: &write.blob_ids,
                    },
                )
                .await?
            else {
                continue;
            };
            any_repush |= re_push;
            staged_acks.push((write.item_key.clone(), write.local_seq, snapshot));
        }

        // Apply all inline text, tombstones, metadata patches, and file metadata cards BEFORE
        // replying so the sender's WiFi badge turns solid green in 1 RTT!
        let has_pending_blobs = self
            .apply_pending_inline_items(pool, group, sync_settings)
            .await?;

        let mut acks = Vec::with_capacity(staged_acks.len());
        let mut needs_blob_pull = has_pending_blobs;
        for (item_key, local_seq, snapshot) in staged_acks {
            let level = if snapshot.deleted() || snapshot.blobs.is_empty() {
                LanContentLevel::Complete
            } else {
                let meta = repository::load_sync_item_meta(pool, &snapshot.sync_key).await?;
                if meta
                    .as_ref()
                    .is_some_and(|row| row.local_state == "managed" && row.item_id.is_some())
                {
                    LanContentLevel::Complete
                } else {
                    if meta.as_ref().is_some_and(|row| row.needs_blob_pull) {
                        needs_blob_pull = true;
                    }
                    LanContentLevel::Metadata
                }
            };
            acks.push(LanItemAck {
                item_key,
                local_seq,
                level,
            });
        }

        Ok((acks, needs_blob_pull, any_repush))
    }

    /// Sends `AckLanItems` with each peer's recorded `remote_seq` once blob materialization or
    /// manual download completes, covering both direct LAN pulls and dual-channel Cloud pulls.
    pub(super) async fn send_lan_acks_for_completed_item(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_key: &str,
        item_key: &str,
    ) {
        let peer_seqs = match repository::lan_remote_seqs_for_item(pool, sync_key).await {
            Ok(seqs) => seqs,
            Err(error) => {
                log::debug!("read LAN remote sequences for ack failed: {error:#}");
                return;
            }
        };
        for (peer_device_id, remote_seq) in peer_seqs {
            let Some(control_conn) = self.cached_lan_connection(&peer_device_id) else {
                continue;
            };
            if let Err(error) = self
                .send_lan_acks(
                    &control_conn,
                    group,
                    vec![LanItemAck {
                        item_key: item_key.to_owned(),
                        local_seq: remote_seq,
                        level: LanContentLevel::Complete,
                    }],
                )
                .await
            {
                log::debug!(
                    "send LAN attachment completion ack to {peer_device_id} failed: {error:#}"
                );
            }
        }
    }

    /// Applies all pending items that require zero network blob I/O (tombstones, metadata updates,
    /// inline text, deduplicated local files/images, and deferred remote file metadata cards).
    /// Returns `true` when at least one remaining item needs Data-Plane blob downloads.
    pub(super) async fn apply_pending_inline_items(
        &self,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_settings: &SyncSettings,
    ) -> Result<bool> {
        let key = group.content_key_bytes()?;
        let history = self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .clipboard
            .history;
        let history_target_bytes = crate::clipboard::cleanup::history_target_bytes(&history);
        let clipboard_generation = self
            .app
            .state::<Arc<ClipboardFingerprintState>>()
            .generation();
        let mut latest_clipboard_item: Option<(ClipboardItem, Option<ClipboardFingerprint>)> = None;
        let mut source_assets_pending = false;
        let mut applied_any = false;
        let mut has_pending_blobs = false;

        let pending = repository::list_items_needing_apply(pool, 64).await?;
        if pending.is_empty() {
            return Ok(false);
        }

        for chunk in pending.chunks(16) {
            let _apply_guard = self.apply_lock.lock().await;
            for meta in chunk {
                let (Some(item_key), Some(nonce), Some(ciphertext)) = (
                    meta.item_key.as_deref(),
                    meta.nonce.as_deref(),
                    meta.ciphertext.as_deref(),
                ) else {
                    repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
                    continue;
                };
                let snapshot = match crypto::decrypt_item_snapshot(
                    &key,
                    item_key,
                    meta.deleted,
                    meta.protected,
                    nonce,
                    ciphertext,
                )
                .and_then(|snapshot| {
                    validate_snapshot(&snapshot)?;
                    Ok(snapshot)
                }) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        log::warn!(
                            "discard invalid pending sync item {}: {error:#}",
                            meta.sync_key
                        );
                        repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
                        continue;
                    }
                };

                if !snapshot.is_present {
                    self.apply_tombstone_snapshot(pool, &snapshot.sync_key)
                        .await?;
                    repository::mark_item_applied(pool, &meta.sync_key, None, "managed", None)
                        .await?;
                    applied_any = true;
                    continue;
                }

                let Some(synced_item) = snapshot.item.as_ref() else {
                    let existing_id = self.apply_metadata_only_snapshot(pool, &snapshot).await?;
                    repository::mark_item_applied(
                        pool,
                        &meta.sync_key,
                        existing_id.as_deref(),
                        "managed",
                        None,
                    )
                    .await?;
                    applied_any |= existing_id.is_some();
                    continue;
                };

                source_assets_pending |= snapshot
                    .source_app
                    .as_ref()
                    .and_then(valid_source_icon)
                    .is_some();

                let kind = parse_kind(&synced_item.kind)?;
                match kind {
                    ClipboardKind::Text => {
                        let (item_id, item, fingerprint, deduplicated) = self
                            .apply_materialized_snapshot(pool, &snapshot, group, false)
                            .await?;
                        repository::mark_item_applied(
                            pool,
                            &meta.sync_key,
                            (!item_id.is_empty()).then_some(item_id.as_str()),
                            "managed",
                            None,
                        )
                        .await?;
                        if !item_id.is_empty() {
                            self.emit_clipboard_item_updated(&item, deduplicated);
                            if latest_clipboard_item
                                .as_ref()
                                .is_none_or(|(latest, _)| item.updated_at >= latest.updated_at)
                            {
                                latest_clipboard_item = Some((item, fingerprint));
                            }
                            applied_any = true;
                        }
                    }
                    ClipboardKind::Image => {
                        // Reuse local image immediately if already present on disk (e.g. metadata update).
                        let already_local = if let Some(existing) =
                            db::items::find_item_by_sync_key(pool, &snapshot.sync_key).await?
                        {
                            existing.kind == ClipboardKind::Image
                                && self
                                    .app
                                    .state::<ImageStore>()
                                    .origin_path(&existing.content)
                                    .is_file()
                        } else {
                            false
                        };
                        if already_local {
                            let (item_id, item, _, deduplicated) = self
                                .apply_materialized_snapshot(pool, &snapshot, group, true)
                                .await?;
                            repository::mark_item_applied(
                                pool,
                                &meta.sync_key,
                                (!item_id.is_empty()).then_some(item_id.as_str()),
                                "managed",
                                None,
                            )
                            .await?;
                            if !item_id.is_empty() {
                                self.emit_clipboard_item_updated(&item, deduplicated);
                                applied_any = true;
                            }
                        } else {
                            repository::mark_item_needs_blob_pull(
                                pool,
                                &meta.sync_key,
                                None,
                                "metadata",
                                None,
                            )
                            .await?;
                            has_pending_blobs = true;
                        }
                    }
                    ClipboardKind::Files => {
                        let resources = crate::core::paths::resources_dir(&self.app)?;
                        let source_revision = synced_item
                            .source_revision
                            .as_deref()
                            .unwrap_or(&snapshot.sync_key);
                        if let Some(existing) = available_materialized_file_item(
                            pool,
                            &resources,
                            &snapshot.sync_key,
                            source_revision,
                            snapshot.content_identity.as_deref(),
                        )
                        .await?
                        {
                            let _ = self.apply_metadata_only_snapshot(pool, &snapshot).await?;
                            repository::mark_item_applied(
                                pool,
                                &meta.sync_key,
                                Some(&existing.id),
                                "managed",
                                None,
                            )
                            .await?;
                            self.emit_clipboard_item_updated(&existing, true);
                            applied_any = true;
                            continue;
                        }

                        // Always materialize or refresh the remote file metadata card immediately.
                        let origin_device_id = meta.origin_device_id.as_deref().unwrap_or("");
                        let (card_id, card_item, deduplicated) = self
                            .apply_remote_file_metadata_snapshot(
                                pool,
                                &snapshot,
                                origin_device_id,
                                group,
                            )
                            .await?;
                        if !card_id.is_empty() {
                            self.emit_clipboard_item_updated(&card_item, deduplicated);
                            applied_any = true;
                        }
                        if should_defer_remote_file_download(
                            &snapshot,
                            sync_settings.auto_download_max_mb,
                            history_target_bytes,
                        ) {
                            repository::mark_item_applied(
                                pool,
                                &meta.sync_key,
                                (!card_id.is_empty()).then_some(card_id.as_str()),
                                "metadata",
                                Some("available"),
                            )
                            .await?;
                        } else {
                            repository::mark_item_needs_blob_pull(
                                pool,
                                &meta.sync_key,
                                (!card_id.is_empty()).then_some(card_id.as_str()),
                                "metadata",
                                Some("available"),
                            )
                            .await?;
                            has_pending_blobs = true;
                        }
                    }
                }
            }
            drop(_apply_guard);
            tokio::task::yield_now().await;
        }

        if let Some((item, fingerprint)) = latest_clipboard_item.as_ref() {
            let can_write = sync_settings.auto_write_clipboard
                && self
                    .app
                    .state::<Arc<ClipboardFingerprintState>>()
                    .generation()
                    == clipboard_generation
                && repository::is_latest_clipboard_item(pool, &item.id).await?;
            self.write_latest_synced_item_to_clipboard(item, fingerprint.as_ref(), can_write);
        }
        #[cfg(target_os = "android")]
        if applied_any {
            crate::commands::android::notify_overlay_clipboard_changed();
        }
        if source_assets_pending {
            self.runtime_state.source_asset_wake.notify_one();
        }
        if applied_any {
            self.emit_updated();
        }
        Ok(has_pending_blobs)
    }

    /// Pulls and materializes the single highest-priority remote attachment item available on
    /// `source` over `data_connection`.
    ///
    /// Returns `Ok(true)` when an item was processed/transitioned so the worker re-queries fresh
    /// priority order, `Ok(false)` when no eligible unclaimed item exists so the worker sleeps
    /// cleanly, and `Err` on retryable transport failure so the worker applies backoff.
    pub(super) async fn pull_one_remote_blob_item(
        self: &Arc<Self>,
        data_connection: &Connection,
        pool: &SqlitePool,
        group: &GroupSecrets,
        sync_settings: &SyncSettings,
        source: &SyncConnectionTarget,
        realtime_only: bool,
    ) -> Result<bool> {
        let target_id = source.delivery_target();
        let min_updated_at_ms = realtime_only.then(|| {
            Utc::now()
                .timestamp_millis()
                .saturating_sub(VIP_REALTIME_RECENCY_MS)
        });
        let candidates =
            repository::list_blob_pull_items_for_source(pool, target_id, min_updated_at_ms, 4)
                .await?;
        if candidates.is_empty() {
            return Ok(false);
        }

        let Some((meta, download_claim)) = candidates.into_iter().find_map(|candidate| {
            let claim_id = candidate.item_id.as_deref().unwrap_or(&candidate.sync_key);
            self.delivery
                .claim_remote_download(claim_id)
                .map(|claim| (candidate, claim))
        }) else {
            return Ok(false);
        };

        let key = group.content_key_bytes()?;
        let history = self
            .app
            .state::<SettingsStore>()
            .snapshot()
            .clipboard
            .history;
        let history_target_bytes = crate::clipboard::cleanup::history_target_bytes(&history);
        let mut quota = AttachmentQuotaInspector::for_history(&self.app, pool, &history).await?;
        let clipboard_generation = self
            .app
            .state::<Arc<ClipboardFingerprintState>>()
            .generation();
        let route = if source.is_cloud() {
            super::super::progress::TransferRoute::Cloud
        } else {
            super::super::progress::TransferRoute::Lan
        };

        let (Some(item_key), Some(nonce), Some(ciphertext)) = (
            meta.item_key.as_deref(),
            meta.nonce.as_deref(),
            meta.ciphertext.as_deref(),
        ) else {
            repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
            return Ok(true);
        };
        let Ok(snapshot) = crypto::decrypt_item_snapshot(
            &key,
            item_key,
            meta.deleted,
            meta.protected,
            nonce,
            ciphertext,
        )
        .and_then(|s| {
            validate_snapshot(&s)?;
            Ok(s)
        }) else {
            repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
            return Ok(true);
        };
        if !snapshot.is_present || snapshot.blobs.is_empty() {
            repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
            return Ok(true);
        }
        let Some(synced_item) = snapshot.item.as_ref() else {
            repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
            return Ok(true);
        };
        if synced_item.kind == "files"
            && should_defer_remote_file_download(
                &snapshot,
                sync_settings.auto_download_max_mb,
                history_target_bytes,
            )
        {
            repository::mark_item_applied(
                pool,
                &meta.sync_key,
                meta.item_id.as_deref(),
                "metadata",
                Some("available"),
            )
            .await?;
            return Ok(true);
        }

        let (_, updated_at) = synced_item_timestamps(synced_item, meta.payload_updated_at_ms);
        let total_original = snapshot
            .blobs
            .iter()
            .fold(0_u64, |acc, b| acc.saturating_add(b.original_size));
        let total_encrypted = snapshot
            .blobs
            .iter()
            .fold(0_u64, |acc, b| acc.saturating_add(b.encrypted_size));
        if !quota
            .admit_or_reject_historical(updated_at, total_original)
            .await
        {
            if synced_item.kind == "files" {
                repository::mark_item_applied(
                    pool,
                    &meta.sync_key,
                    meta.item_id.as_deref(),
                    "metadata",
                    Some("available"),
                )
                .await?;
            } else {
                repository::clear_item_needs_apply(pool, &meta.sync_key).await?;
            }
            return Ok(true);
        }

        let is_files = synced_item.kind == "files";
        let transfer_handle = if is_files {
            if let Some(item_id) = meta.item_id.as_deref() {
                let task_id = Uuid::new_v4().simple().to_string();
                repository::lock_transfer_item(pool, &task_id, item_id).await?;
                let handle = self.delivery.start(super::super::progress::TransferTask {
                    id: task_id.clone(),
                    revision: 0,
                    started_at_ms: Utc::now().timestamp_millis(),
                    operation_id: meta.sync_key.clone(),
                    item_id: item_id.to_owned(),
                    direction: super::super::progress::TransferDirection::Download,
                    route,
                    target: target_id.to_owned(),
                    file_name: (snapshot.blobs.len() == 1).then(|| snapshot.blobs[0].name.clone()),
                    phase: super::super::progress::TransferPhase::Transferring,
                    transferred_bytes: 0,
                    total_bytes: total_encrypted,
                    speed_bytes_per_second: 0,
                    elapsed_ms: 0,
                    eta_seconds: None,
                    error: None,
                    cancellable: true,
                });
                Some((task_id, handle))
            } else {
                None
            }
        } else {
            None
        };

        let transferred = AtomicU64::new(0);
        let mut completed_bytes = 0_u64;
        let mut staged_blobs = Vec::with_capacity(snapshot.blobs.len());
        let mut pull_error: Option<anyhow::Error> = None;

        for manifest in &snapshot.blobs {
            let progress = transfer_handle
                .as_ref()
                .map(|(_, handle)| BlobDownloadProgress {
                    handle,
                    transferred: &transferred,
                    completed_bytes,
                });
            match self
                .ensure_blob(data_connection, group, manifest, progress)
                .await
            {
                Ok(path) => {
                    completed_bytes = completed_bytes.saturating_add(manifest.encrypted_size);
                    staged_blobs.push(StoredBlob {
                        blob_id: manifest.blob_id.clone(),
                        encrypted_path: path.to_string_lossy().into_owned(),
                        size: manifest.encrypted_size,
                        source_path: None,
                        source_is_directory: manifest.is_directory_archive,
                        stream_nonce: None,
                    });
                }
                Err(error) => {
                    pull_error = Some(error);
                    break;
                }
            }
        }

        if let Some(error) = pull_error {
            let canceled = transfer_handle
                .as_ref()
                .is_some_and(|(_, handle)| handle.canceled());
            if let Some((task_id, handle)) = &transfer_handle {
                let bytes = transferred.load(Ordering::Acquire);
                if canceled {
                    handle.mark_canceled(bytes);
                } else {
                    handle.failed(bytes, error.to_string());
                }
                let _ = repository::unlock_transfer_item(pool, task_id).await;
            }
            drop(download_claim);
            if canceled {
                repository::mark_item_applied(
                    pool,
                    &meta.sync_key,
                    meta.item_id.as_deref(),
                    "metadata",
                    Some("available"),
                )
                .await?;
                for blob in staged_blobs {
                    let _ = tokio::fs::remove_file(&blob.encrypted_path).await;
                }
                return Ok(true);
            }
            if source.is_cloud() && should_invalidate_cloud_connection(data_connection, &error) {
                self.invalidate_cloud_data_connection(data_connection.stable_id());
            }
            return Err(error);
        }

        if let Some((_, handle)) = &transfer_handle {
            if !handle.begin_finalizing(completed_bytes) {
                handle.mark_canceled(completed_bytes);
                if let Some((task_id, _)) = &transfer_handle {
                    let _ = repository::unlock_transfer_item(pool, task_id).await;
                }
                drop(download_claim);
                repository::mark_item_applied(
                    pool,
                    &meta.sync_key,
                    meta.item_id.as_deref(),
                    "metadata",
                    Some("available"),
                )
                .await?;
                return Ok(true);
            }
        }

        let apply_res = self
            .apply_materialized_snapshot(pool, &snapshot, group, false)
            .await;
        match apply_res {
            Ok((item_id, item, fingerprint, deduplicated)) => {
                if !item_id.is_empty() {
                    if let Ok(sources) = self
                        .materialized_blob_sources(&item, &snapshot.blobs, &staged_blobs)
                        .await
                    {
                        for source_blob in sources {
                            let _ = repository::update_blob_local_source(
                                pool,
                                &meta.sync_key,
                                &source_blob.blob_id,
                                "",
                                source_blob.source_path.as_deref(),
                                source_blob.source_is_directory,
                                source_blob.stream_nonce.as_deref(),
                            )
                            .await;
                        }
                    }
                }
                repository::mark_item_applied(
                    pool,
                    &meta.sync_key,
                    (!item_id.is_empty()).then_some(item_id.as_str()),
                    "managed",
                    None,
                )
                .await?;
                for blob in staged_blobs {
                    let _ = tokio::fs::remove_file(&blob.encrypted_path).await;
                }
                if let Some((task_id, handle)) = &transfer_handle {
                    handle.completed(total_encrypted);
                    let _ = repository::unlock_transfer_item(pool, task_id).await;
                }
                drop(download_claim);

                if !item_id.is_empty() {
                    self.emit_clipboard_item_updated(&item, deduplicated);
                    let can_write = sync_settings.auto_write_clipboard
                        && self
                            .app
                            .state::<Arc<ClipboardFingerprintState>>()
                            .generation()
                            == clipboard_generation
                        && repository::is_latest_clipboard_item(pool, &item.id).await?;
                    self.write_latest_synced_item_to_clipboard(
                        &item,
                        fingerprint.as_ref(),
                        can_write,
                    );
                }
                self.send_lan_acks_for_completed_item(pool, group, &meta.sync_key, item_key)
                    .await;
                #[cfg(target_os = "android")]
                crate::commands::android::notify_overlay_clipboard_changed();
                self.runtime_state.source_asset_wake.notify_one();
                self.emit_updated();
                Ok(true)
            }
            Err(error) => {
                if let Some((task_id, handle)) = &transfer_handle {
                    handle.failed(total_encrypted, error.to_string());
                    let _ = repository::unlock_transfer_item(pool, task_id).await;
                }
                drop(download_claim);
                Err(error)
            }
        }
    }

    fn emit_clipboard_item_updated(&self, item: &ClipboardItem, deduplicated: bool) {
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

    async fn apply_tombstone_snapshot(&self, pool: &SqlitePool, sync_key: &str) -> Result<()> {
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let _resource_guard = crate::clipboard::shared_files::lock().await;
        let rows = sqlx::query("SELECT kind, content FROM clipboard_items WHERE sync_key = ?")
            .bind(sync_key)
            .fetch_all(pool)
            .await
            .context("query deleted synchronized clipboard items")?;
        if rows.is_empty() {
            return Ok(());
        }
        let mut deleted_images = Vec::new();
        for row in rows {
            let kind: ClipboardKind = row.try_get("kind")?;
            let content: String = row.try_get("content")?;
            if kind == ClipboardKind::Image && !content.is_empty() {
                deleted_images.push(content);
            }
        }
        sqlx::query("DELETE FROM clipboard_items WHERE sync_key = ?")
            .bind(sync_key)
            .execute(pool)
            .await
            .context("delete synchronized clipboard item")?;

        if let Err(error) = self.app.emit(
            crate::clipboard::CLIPBOARD_UPDATED_EVENT,
            serde_json::json!({ "deletedHash": sync_key, "reconciled": true }),
        ) {
            log::warn!("emit synchronized deletion failed: {error}");
        }
        if !deleted_images.is_empty() {
            let store = self.app.state::<ImageStore>();
            if let Err(error) = crate::clipboard::cache::reclaim_deleted_images(
                pool,
                store.inner(),
                &deleted_images,
            )
            .await
            {
                log::warn!("reclaim deleted synchronized images failed: {error:#}");
            }
        }
        if let Err(error) =
            crate::clipboard::shared_files::cleanup_resources_locked(pool, resources).await
        {
            log::warn!("reclaim deleted synchronized files failed: {error:#}");
        }
        Ok(())
    }

    async fn apply_metadata_only_snapshot(
        &self,
        pool: &SqlitePool,
        snapshot: &SyncedItemSnapshot,
    ) -> Result<Option<String>> {
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT id FROM clipboard_items WHERE sync_key = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&snapshot.sync_key)
        .fetch_optional(pool)
        .await
        .context("find clipboard item for metadata snapshot")?;
        let Some(item_id) = existing else {
            return Ok(None);
        };
        sqlx::query(
            r#"
            UPDATE clipboard_items
            SET is_favorite = CASE WHEN ? > 0 THEN ? ELSE is_favorite END,
                is_pinned = CASE WHEN ? > 0 THEN ? ELSE is_pinned END,
                note = CASE WHEN ? > 0 THEN ? ELSE note END
            WHERE id = ?
            "#,
        )
        .bind(i64::try_from(snapshot.favorite_version.seq).unwrap_or(i64::MAX))
        .bind(snapshot.is_favorite)
        .bind(i64::try_from(snapshot.pinned_version.seq).unwrap_or(i64::MAX))
        .bind(snapshot.is_pinned)
        .bind(i64::try_from(snapshot.note_version.seq).unwrap_or(i64::MAX))
        .bind(snapshot.note.as_deref())
        .bind(&item_id)
        .execute(pool)
        .await
        .context("apply metadata-only sync snapshot")?;

        let _ = self.app.emit(
            crate::clipboard::CLIPBOARD_UPDATED_EVENT,
            serde_json::json!({
                "id": item_id,
                "deduplicated": true,
                "reconciled": true,
            }),
        );
        Ok(Some(item_id))
    }

    async fn apply_remote_file_metadata_snapshot(
        &self,
        pool: &SqlitePool,
        snapshot: &SyncedItemSnapshot,
        origin_device_id: &str,
        group: &GroupSecrets,
    ) -> Result<(String, ClipboardItem, bool)> {
        let synced = snapshot
            .item
            .as_ref()
            .context("missing item in remote file snapshot")?;
        if parse_kind(&synced.kind)? != ClipboardKind::Files {
            bail!("不支持的远端文件元数据");
        }
        let mut manifests = snapshot
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

        let (created_at, updated_at) = synced_item_timestamps(synced, synced.created_at_ms);
        let source_updated_at = synced
            .source_updated_at_ms
            .and_then(|value| Utc.timestamp_millis_opt(value).single())
            .unwrap_or(updated_at);
        let source_revision = synced
            .source_revision
            .clone()
            .unwrap_or_else(|| snapshot.sync_key.clone());
        let source_app_id = if let Some(source_app) = snapshot.source_app.as_ref() {
            match self
                .resolve_received_source_app(
                    pool,
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
        let sync_key = snapshot.sync_key.clone();
        let mut item = ClipboardItem {
            id: Uuid::new_v4().to_string(),
            kind: ClipboardKind::Files,
            sub_kind: None,
            group_id: None,
            source_app_id,
            source_revision,
            source_device_id: synced.source_device_id.clone(),
            source_device_name: synced.source_device_name.clone(),
            source_updated_at: Some(source_updated_at),
            source_device_is_local: false,
            content: content.clone(),
            content_hash: sync_key.clone(),
            sync_key,
            search_text: Some(content),
            summary: synced.summary.clone(),
            text_char_count: None,
            file_types: synced.file_types.clone(),
            size: synced.size,
            width: None,
            height: None,
            use_count: 1,
            is_favorite: snapshot.is_favorite,
            is_pinned: snapshot.is_pinned,
            is_sensitive: synced.is_sensitive,
            platform: parse_platform(&synced.source_platform),
            note: snapshot.note.clone(),
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
        let result = db::items::upsert_remote_file_item(pool, &item, origin_device_id).await?;
        item.id.clone_from(&result.id);
        if let Some(materialized) = db::items::find_item_by_id(pool, &result.id).await? {
            item = materialized;
        }
        Ok((result.id, item, result.deduplicated))
    }

    async fn apply_materialized_snapshot(
        &self,
        pool: &SqlitePool,
        snapshot: &SyncedItemSnapshot,
        group: &GroupSecrets,
        skip_existing_image_decrypt: bool,
    ) -> Result<(String, ClipboardItem, Option<ClipboardFingerprint>, bool)> {
        let _sync_activity = SyncActivityGuard::acquire();
        let synced = snapshot
            .item
            .as_ref()
            .context("missing item in materialized snapshot")?;
        let kind = parse_kind(&synced.kind)?;
        let image_apply_guard = if kind == ClipboardKind::Image && !skip_existing_image_decrypt {
            Some(self.apply_lock.lock().await)
        } else {
            None
        };
        let mut resource_guard = if kind == ClipboardKind::Image {
            Some(crate::clipboard::shared_files::lock().await)
        } else {
            None
        };
        let resources = crate::core::paths::resources_dir(&self.app)?;
        let blob_root = resources.join(SYNC_STAGING_DIR);
        let mut file_apply_guard = None;
        let (content, files_fingerprint) = match kind {
            ClipboardKind::Text => (synced.content.clone(), None),
            ClipboardKind::Image => {
                let manifest = snapshot
                    .blobs
                    .iter()
                    .find(|blob| blob.role == BlobRole::Image)
                    .context("同步图片缺少数据")?;
                let image_name = sanitize_name(&manifest.name);
                let destination = self.app.state::<ImageStore>().origin_path(&image_name);
                if !(skip_existing_image_decrypt && destination.is_file()) {
                    let encrypted = crypto::blob_path(&blob_root, &manifest.blob_id)?;
                    let key = group.content_key_bytes()?;
                    let original_size = manifest.original_size;
                    tauri::async_runtime::spawn_blocking(move || {
                        crypto::decrypt_blob(&encrypted, &destination, original_size, &key)
                    })
                    .await
                    .context("image decryption task failed")??;
                }
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
                for manifest in &snapshot.blobs {
                    if manifest.role != BlobRole::File {
                        continue;
                    }
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
                if kind == ClipboardKind::Files {
                    file_apply_guard = Some(self.apply_lock.lock().await);
                    resource_guard = Some(crate::clipboard::shared_files::lock().await);
                }
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

        let (created_at, updated_at) = synced_item_timestamps(synced, synced.created_at_ms);
        let source_updated_at = synced
            .source_updated_at_ms
            .and_then(|value| Utc.timestamp_millis_opt(value).single())
            .unwrap_or(updated_at);
        let source_revision = synced
            .source_revision
            .clone()
            .unwrap_or_else(|| snapshot.sync_key.clone());
        let source_app_id = if let Some(source_app) = snapshot.source_app.as_ref() {
            match self
                .resolve_received_source_app(
                    pool,
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
        let sync_key = snapshot.sync_key.clone();
        let mut item = ClipboardItem {
            id: Uuid::new_v4().to_string(),
            kind,
            sub_kind: synced.sub_kind.as_deref().map(parse_sub_kind).transpose()?,
            group_id: None,
            source_app_id,
            source_revision,
            source_device_id: synced.source_device_id.clone(),
            source_device_name: synced.source_device_name.clone(),
            source_updated_at: Some(source_updated_at),
            source_device_is_local: false,
            content_hash: if kind == ClipboardKind::Files {
                db::items::content_hash(kind, &content)
            } else {
                sync_key.clone()
            },
            sync_key,
            content,
            search_text: synced.search_text.clone(),
            summary: synced.summary.clone(),
            text_char_count: None,
            file_types: synced.file_types.clone(),
            size: synced.size,
            width: synced.width,
            height: synced.height,
            use_count: 1,
            is_favorite: snapshot.is_favorite,
            is_pinned: snapshot.is_pinned,
            is_sensitive: synced.is_sensitive,
            platform: parse_platform(&synced.source_platform),
            note: snapshot.note.clone(),
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
        let result = db::items::upsert_materialized_synced_item(pool, &item).await?;
        drop(resource_guard);
        drop(file_apply_guard);
        drop(image_apply_guard);
        if result.id.is_empty() {
            return Ok((String::new(), item, None, result.deduplicated));
        }
        item.id.clone_from(&result.id);
        if let Some(materialized) = db::items::find_item_by_id(pool, &result.id).await? {
            item = materialized;
        }
        let fingerprint = files_fingerprint.or_else(|| ClipboardFingerprint::from_text_item(&item));
        Ok((result.id, item, fingerprint, result.deduplicated))
    }

    /// Maps a materialized item back to its plaintext sources so staging ciphertext can be deleted.
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
        snapshot: &SyncedItemSnapshot,
    ) -> Result<Option<String>> {
        let Some(manifest) = snapshot
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
        let result = crate::clipboard::write_synced_item_to_clipboard(
            &self.app.state::<ImageStore>(),
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
