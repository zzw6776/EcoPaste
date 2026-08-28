CREATE TABLE removed_devices (
    group_id      TEXT    NOT NULL,
    device_id     TEXT    NOT NULL,
    endpoint_id   TEXT    NOT NULL,
    removed_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (group_id, device_id),
    UNIQUE (group_id, endpoint_id),
    FOREIGN KEY (group_id) REFERENCES sync_groups(group_id) ON DELETE CASCADE
);
