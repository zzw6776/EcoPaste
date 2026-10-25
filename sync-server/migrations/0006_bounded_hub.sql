ALTER TABLE sync_groups ADD COLUMN pruned_through_cursor INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_groups ADD COLUMN event_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_groups ADD COLUMN event_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_groups ADD COLUMN blob_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_groups ADD COLUMN blob_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE events ADD COLUMN received_at_ms INTEGER NOT NULL DEFAULT 0;
UPDATE events
SET received_at_ms = CAST(strftime('%s', 'now') AS INTEGER) * 1000
WHERE received_at_ms = 0;

ALTER TABLE blobs ADD COLUMN gc_protected INTEGER NOT NULL DEFAULT 0;
UPDATE blobs
SET gc_protected = 1,
    created_at_ms = CAST(strftime('%s', 'now') AS INTEGER) * 1000;

UPDATE sync_groups
SET event_bytes = COALESCE((
        SELECT SUM(length(nonce) + length(ciphertext))
        FROM events WHERE events.group_id = sync_groups.group_id
    ), 0),
    event_count = COALESCE((
        SELECT COUNT(*) FROM events WHERE events.group_id = sync_groups.group_id
    ), 0),
    blob_bytes = COALESCE((
        SELECT SUM(size) FROM blobs WHERE blobs.group_id = sync_groups.group_id
    ), 0),
    blob_count = COALESCE((
        SELECT COUNT(*) FROM blobs WHERE blobs.group_id = sync_groups.group_id
    ), 0);

CREATE TABLE event_blob_refs (
    group_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (group_id, event_id, blob_id),
    FOREIGN KEY (group_id, event_id)
        REFERENCES events(group_id, event_id) ON DELETE CASCADE
);

CREATE INDEX idx_events_group_received_cursor
ON events(group_id, received_at_ms, cursor);

CREATE INDEX idx_event_blob_refs_group_blob
ON event_blob_refs(group_id, blob_id);
