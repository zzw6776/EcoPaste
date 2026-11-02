CREATE TABLE retention_deferred_events (
    group_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    decoder_revision INTEGER NOT NULL CHECK (decoder_revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (group_id, event_id, device_id),
    FOREIGN KEY (group_id, event_id) REFERENCES events(group_id, event_id) ON DELETE CASCADE,
    FOREIGN KEY (group_id, device_id) REFERENCES devices(group_id, device_id) ON DELETE CASCADE
);

CREATE INDEX idx_retention_deferred_device
ON retention_deferred_events(group_id, device_id);
