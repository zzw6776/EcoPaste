ALTER TABLE sync_content_outbox ADD COLUMN delivery_route TEXT
    CHECK (delivery_route IN ('lan', 'cloud'));

ALTER TABLE sync_events ADD COLUMN delivery_route TEXT
    CHECK (delivery_route IN ('lan', 'cloud'));

CREATE TABLE sync_remote_file_items (
    item_id          TEXT PRIMARY KEY NOT NULL REFERENCES clipboard_items(id) ON DELETE CASCADE,
    event_id         TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    origin_device_id TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'available'
                     CHECK (state IN ('available', 'expired')),
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE INDEX idx_sync_remote_file_items_event_id
ON sync_remote_file_items(event_id);

CREATE TABLE sync_active_transfers (
    task_id    TEXT PRIMARY KEY NOT NULL,
    item_id    TEXT NOT NULL REFERENCES clipboard_items(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_sync_active_transfers_item_id
ON sync_active_transfers(item_id);

CREATE TABLE sync_canceled_deliveries (
    event_id   TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

CREATE INDEX idx_sync_canceled_deliveries_target
ON sync_canceled_deliveries(target_id, event_id);
