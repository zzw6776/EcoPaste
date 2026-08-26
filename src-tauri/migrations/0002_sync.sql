CREATE TABLE sync_events (
    cursor              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT    NOT NULL UNIQUE,
    origin_device_id    TEXT    NOT NULL,
    origin_sequence     INTEGER NOT NULL,
    event_created_at_ms INTEGER NOT NULL,
    nonce               BLOB    NOT NULL,
    ciphertext          BLOB    NOT NULL,
    is_applied          INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL,
    UNIQUE (origin_device_id, origin_sequence)
);

CREATE INDEX idx_sync_events_cursor ON sync_events(cursor);

CREATE TABLE sync_event_blobs (
    event_id       TEXT    NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    blob_id        TEXT    NOT NULL,
    encrypted_path TEXT    NOT NULL,
    size           INTEGER NOT NULL,
    created_at     TEXT    NOT NULL,
    updated_at     TEXT    NOT NULL,
    PRIMARY KEY (event_id, blob_id)
);

CREATE TABLE sync_deliveries (
    event_id   TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

CREATE TABLE sync_peers (
    device_id         TEXT    PRIMARY KEY NOT NULL,
    device_name       TEXT    NOT NULL,
    platform          TEXT    NOT NULL,
    endpoint_id       TEXT    NOT NULL,
    direct_addresses  TEXT    NOT NULL,
    relay_urls        TEXT    NOT NULL,
    pull_cursor       INTEGER NOT NULL DEFAULT 0,
    last_seen_ms      INTEGER NOT NULL,
    created_at        TEXT    NOT NULL,
    updated_at        TEXT    NOT NULL
);

CREATE TABLE sync_state (
    key        TEXT PRIMARY KEY NOT NULL,
    value      TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE sync_pending_items (
    item_id    TEXT PRIMARY KEY NOT NULL REFERENCES clipboard_items(id) ON DELETE CASCADE,
    reason     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
