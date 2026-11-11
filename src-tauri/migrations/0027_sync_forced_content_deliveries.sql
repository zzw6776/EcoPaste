CREATE TABLE sync_forced_content_deliveries (
    event_id   TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);
