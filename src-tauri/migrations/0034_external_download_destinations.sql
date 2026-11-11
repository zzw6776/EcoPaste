CREATE TABLE sync_external_download_sessions (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    content_identity TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_sync_external_download_sessions_content
    ON sync_external_download_sessions(content_identity, created_at DESC);

CREATE TABLE sync_external_download_targets (
    session_id TEXT NOT NULL REFERENCES sync_external_download_sessions(id) ON DELETE CASCADE,
    destination_kind TEXT NOT NULL CHECK (destination_kind IN ('path', 'uri')),
    destination TEXT NOT NULL,
    expected_size INTEGER NOT NULL CHECK (expected_size >= 0),
    is_directory INTEGER NOT NULL CHECK (is_directory IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (session_id, destination)
);
