CREATE TABLE sync_remote_file_items_next (
    item_id          TEXT PRIMARY KEY NOT NULL REFERENCES clipboard_items(id) ON DELETE CASCADE,
    event_id         TEXT REFERENCES sync_events(event_id) ON DELETE SET NULL,
    origin_device_id TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'available'
                     CHECK (state IN ('available', 'expired')),
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

INSERT INTO sync_remote_file_items_next (
    item_id, event_id, origin_device_id, state, created_at, updated_at
)
SELECT item_id, event_id, origin_device_id, state, created_at, updated_at
FROM sync_remote_file_items;

DROP TABLE sync_remote_file_items;
ALTER TABLE sync_remote_file_items_next RENAME TO sync_remote_file_items;

CREATE INDEX idx_sync_remote_file_items_event_id
ON sync_remote_file_items(event_id);

UPDATE sync_remote_file_items
SET state = 'expired',
    event_id = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id IN (
    SELECT event_id
    FROM sync_event_blobs
    WHERE instr(replace(encrypted_path, '\', '/'), '/sync-blobs/') > 0
);

DELETE FROM sync_events
WHERE event_id IN (
    SELECT event_id
    FROM sync_event_blobs
    WHERE instr(replace(encrypted_path, '\', '/'), '/sync-blobs/') > 0
);

DELETE FROM sync_source_icon_uploads
WHERE instr(replace(encrypted_path, '\', '/'), '/sync-blobs/') > 0;
