PRAGMA foreign_keys = ON;

CREATE TABLE sync_groups (
    group_id TEXT PRIMARY KEY NOT NULL,
    access_token_hash BLOB NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE devices (
    group_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    device_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    direct_addresses BLOB NOT NULL,
    relay_urls BLOB NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    PRIMARY KEY (group_id, device_id),
    FOREIGN KEY (group_id) REFERENCES sync_groups(group_id) ON DELETE CASCADE
);

CREATE TABLE events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    origin_device_id TEXT NOT NULL,
    origin_sequence INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    nonce BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    UNIQUE (group_id, event_id),
    UNIQUE (group_id, origin_device_id, origin_sequence),
    FOREIGN KEY (group_id) REFERENCES sync_groups(group_id) ON DELETE CASCADE
);

CREATE INDEX idx_events_group_cursor ON events(group_id, cursor);

CREATE TABLE blobs (
    group_id TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (group_id, blob_id),
    FOREIGN KEY (group_id) REFERENCES sync_groups(group_id) ON DELETE CASCADE
);
