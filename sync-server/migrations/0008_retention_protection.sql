ALTER TABLE sync_groups ADD COLUMN retention_epoch TEXT NOT NULL DEFAULT '';
UPDATE sync_groups SET retention_epoch = lower(hex(randomblob(16)));

CREATE TABLE retention_devices (
    group_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (group_id, device_id),
    FOREIGN KEY (group_id, device_id) REFERENCES devices(group_id, device_id) ON DELETE CASCADE
);

CREATE TABLE retention_protections (
    group_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (group_id, device_id, item_key),
    FOREIGN KEY (group_id, device_id) REFERENCES retention_devices(group_id, device_id) ON DELETE CASCADE
);
CREATE INDEX idx_retention_protections_item ON retention_protections(group_id, item_key);

ALTER TABLE events ADD COLUMN retention_key TEXT;
CREATE INDEX idx_events_retention_key ON events(group_id, retention_key);
