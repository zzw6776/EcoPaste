PRAGMA foreign_keys = ON;

-- Remove un-downloaded remote file placeholder cards before retiring legacy sync tables.
-- Materialized local clipboard items, groups, apps, and paired device identities are preserved.
DELETE FROM clipboard_items
WHERE id IN (SELECT item_id FROM sync_remote_file_items);

DROP TABLE IF EXISTS sync_delivery_targets;
DROP TABLE IF EXISTS sync_local_content;
DROP TABLE IF EXISTS sync_event_ingress;
DROP TABLE IF EXISTS sync_local_replicas;
DROP TABLE IF EXISTS sync_target_replicas;
DROP TABLE IF EXISTS sync_forced_content_deliveries;
DROP TABLE IF EXISTS sync_peer_receipts;
DROP TABLE IF EXISTS sync_remote_file_items;
DROP TABLE IF EXISTS sync_canceled_deliveries;
DROP TABLE IF EXISTS sync_content_outbox;
DROP TABLE IF EXISTS sync_item_states;
DROP TABLE IF EXISTS sync_item_events;
DROP TABLE IF EXISTS sync_delivery_states;
DROP TABLE IF EXISTS sync_deliveries;
DROP TABLE IF EXISTS sync_event_blobs;
DROP TABLE IF EXISTS sync_events;
DROP TABLE IF EXISTS sync_pending_items;
DROP TABLE IF EXISTS sync_external_download_targets;
DROP TABLE IF EXISTS sync_external_download_sessions;

-- Clear stale transport/cursor state while preserving monotonic sequence counters.
DELETE FROM sync_state
WHERE key NOT IN ('origin_sequence');

UPDATE sync_peers
SET pull_cursor = 0;

ALTER TABLE sync_peers ADD COLUMN catchup_before_seq INTEGER;
ALTER TABLE sync_peers ADD COLUMN window_high_seq INTEGER NOT NULL DEFAULT 0;

CREATE TABLE sync_items (
    sync_key TEXT PRIMARY KEY NOT NULL,
    item_key TEXT,
    item_id TEXT REFERENCES clipboard_items(id) ON DELETE SET NULL,
    origin TEXT NOT NULL DEFAULT 'local',
    origin_device_id TEXT,
    source_revision TEXT,
    content_identity TEXT,
    kind TEXT NOT NULL DEFAULT 'text',
    is_sensitive INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    protected INTEGER NOT NULL DEFAULT 0,
    content_seq INTEGER NOT NULL DEFAULT 0,
    content_device_id TEXT NOT NULL DEFAULT '',
    presence_seq INTEGER NOT NULL DEFAULT 0,
    presence_device_id TEXT NOT NULL DEFAULT '',
    favorite_seq INTEGER NOT NULL DEFAULT 0,
    favorite_device_id TEXT NOT NULL DEFAULT '',
    pinned_seq INTEGER NOT NULL DEFAULT 0,
    pinned_device_id TEXT NOT NULL DEFAULT '',
    note_seq INTEGER NOT NULL DEFAULT 0,
    note_device_id TEXT NOT NULL DEFAULT '',
    is_present INTEGER NOT NULL DEFAULT 1,
    is_favorite INTEGER NOT NULL DEFAULT 0,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    local_seq INTEGER NOT NULL DEFAULT 0,
    hub_rev INTEGER NOT NULL DEFAULT 0,
    payload_updated_at_ms INTEGER NOT NULL DEFAULT 0,
    prep_state TEXT NOT NULL DEFAULT 'ready',
    prep_error TEXT,
    prep_attempts INTEGER NOT NULL DEFAULT 0,
    next_prep_at TEXT,
    history_backfill INTEGER NOT NULL DEFAULT 0,
    manual_share INTEGER NOT NULL DEFAULT 0,
    delivery_route TEXT,
    nonce BLOB,
    ciphertext BLOB,
    local_state TEXT NOT NULL DEFAULT 'managed',
    remote_file_state TEXT,
    external_destinations_json TEXT,
    needs_apply INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_sync_items_item_key
ON sync_items(item_key)
WHERE item_key IS NOT NULL;

CREATE INDEX idx_sync_items_item_id
ON sync_items(item_id)
WHERE item_id IS NOT NULL;

CREATE INDEX idx_sync_items_local_seq
ON sync_items(local_seq DESC)
WHERE local_seq > 0;

CREATE INDEX idx_sync_items_prep_queue
ON sync_items(prep_state, kind, history_backfill ASC, payload_updated_at_ms DESC)
WHERE prep_state <> 'ready';

CREATE INDEX idx_sync_items_needs_apply
ON sync_items(needs_apply, payload_updated_at_ms DESC)
WHERE needs_apply = 1;

CREATE TABLE sync_item_blobs (
    sync_key TEXT NOT NULL REFERENCES sync_items(sync_key) ON DELETE CASCADE,
    blob_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'file',
    optional INTEGER NOT NULL DEFAULT 0,
    size INTEGER NOT NULL DEFAULT 0,
    original_size INTEGER NOT NULL DEFAULT 0,
    encrypted_path TEXT NOT NULL DEFAULT '',
    source_path TEXT,
    source_is_directory INTEGER NOT NULL DEFAULT 0,
    stream_nonce BLOB,
    stream_commitment INTEGER NOT NULL DEFAULT 0,
    cloud_available INTEGER NOT NULL DEFAULT 0,
    local_available INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (sync_key, blob_id)
);

CREATE INDEX idx_sync_item_blobs_blob_id
ON sync_item_blobs(blob_id);

CREATE TABLE sync_item_targets (
    sync_key TEXT NOT NULL REFERENCES sync_items(sync_key) ON DELETE CASCADE,
    target_id TEXT NOT NULL,
    desired_content TEXT NOT NULL DEFAULT 'complete',
    confirmed_content TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL DEFAULT 'pending',
    pushed_local_seq INTEGER NOT NULL DEFAULT 0,
    acked_local_seq INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (sync_key, target_id)
);

CREATE INDEX idx_sync_item_targets_pending
ON sync_item_targets(target_id, status, pushed_local_seq);
