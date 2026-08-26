CREATE TABLE sync_item_events (
    item_id    TEXT NOT NULL REFERENCES clipboard_items(id) ON DELETE CASCADE,
    event_id   TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    direction  TEXT NOT NULL CHECK (direction IN ('local', 'remote')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (item_id, event_id)
);

CREATE INDEX idx_sync_item_events_event_id ON sync_item_events(event_id);

CREATE TABLE sync_delivery_states (
    event_id        TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id       TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('syncing', 'success', 'error')),
    last_error      TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

CREATE INDEX idx_sync_delivery_states_target ON sync_delivery_states(target_id, state);

CREATE TABLE sync_peer_connections (
    device_id         TEXT PRIMARY KEY NOT NULL REFERENCES sync_peers(device_id) ON DELETE CASCADE,
    state             TEXT NOT NULL CHECK (state IN ('connecting', 'online', 'offline', 'error')),
    connected_address TEXT,
    transport         TEXT CHECK (transport IS NULL OR transport IN ('direct', 'relay', 'unknown')),
    last_attempt_at   TEXT,
    last_success_at   TEXT,
    last_error        TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);
