ALTER TABLE clipboard_items ADD COLUMN sync_key TEXT;

UPDATE clipboard_items
SET sync_key = content_hash
WHERE sync_key IS NULL;

CREATE INDEX idx_clipboard_items_sync_key ON clipboard_items(sync_key);

ALTER TABLE sync_events ADD COLUMN event_kind INTEGER NOT NULL DEFAULT 0;

CREATE TABLE sync_item_states (
    sync_key                    TEXT    PRIMARY KEY NOT NULL,
    is_sensitive                INTEGER NOT NULL DEFAULT 0,
    is_favorite                 INTEGER,
    favorite_origin_sequence   INTEGER,
    favorite_origin_device_id  TEXT,
    is_pinned                   INTEGER,
    pinned_origin_sequence     INTEGER,
    pinned_origin_device_id    TEXT,
    note                        TEXT,
    note_origin_sequence       INTEGER,
    note_origin_device_id      TEXT,
    is_present                 INTEGER,
    presence_origin_sequence   INTEGER,
    presence_origin_device_id  TEXT,
    dirty_mask                 INTEGER NOT NULL DEFAULT 0,
    local_revision             INTEGER NOT NULL DEFAULT 0,
    created_at                 TEXT    NOT NULL,
    updated_at                 TEXT    NOT NULL
);

CREATE INDEX idx_sync_item_states_dirty
ON sync_item_states(dirty_mask) WHERE dirty_mask <> 0;

CREATE INDEX idx_sync_events_unapplied_kind_cursor
ON sync_events(event_kind, cursor) WHERE is_applied = 0;
