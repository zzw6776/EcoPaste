CREATE TABLE sync_removed_peers (
    device_id     TEXT PRIMARY KEY NOT NULL,
    endpoint_id   TEXT NOT NULL,
    removed_at_ms INTEGER NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_sync_removed_peers_endpoint ON sync_removed_peers(endpoint_id);
