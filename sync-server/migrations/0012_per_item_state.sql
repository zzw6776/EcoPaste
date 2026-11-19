PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS retention_deferred_events;
DROP TABLE IF EXISTS retention_protections;
DROP TABLE IF EXISTS retention_devices;
DROP TABLE IF EXISTS event_blob_refs;
DROP TABLE IF EXISTS events;
DELETE FROM blobs;

ALTER TABLE sync_groups ADD COLUMN latest_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_groups ADD COLUMN pruned_through_seq INTEGER NOT NULL DEFAULT 0;

UPDATE sync_groups
SET pruned_through_cursor = 0,
    event_bytes = 0,
    event_count = 0,
    blob_bytes = 0,
    blob_count = 0,
    availability_cursor = 0,
    pruned_availability_cursor = 0;

CREATE TABLE items (
    group_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    seq INTEGER NOT NULL,
    hub_rev INTEGER NOT NULL,
    origin_device_id TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    protected INTEGER NOT NULL DEFAULT 0,
    nonce BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    PRIMARY KEY (group_id, item_key),
    FOREIGN KEY (group_id) REFERENCES sync_groups(group_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_items_group_seq
ON items(group_id, seq);

CREATE INDEX idx_items_group_cleanup
ON items(group_id, protected, deleted, updated_at_ms, seq);

CREATE TABLE item_blob_refs (
    group_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    optional INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (group_id, item_key, blob_id),
    FOREIGN KEY (group_id, item_key)
        REFERENCES items(group_id, item_key) ON DELETE CASCADE
);

CREATE INDEX idx_item_blob_refs_group_blob
ON item_blob_refs(group_id, blob_id);
