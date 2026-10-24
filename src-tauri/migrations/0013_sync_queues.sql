CREATE TABLE sync_content_outbox (
    operation_order  INTEGER PRIMARY KEY NOT NULL,
    item_id          TEXT    NOT NULL,
    sync_key         TEXT    NOT NULL,
    source_revision  TEXT    NOT NULL,
    force_files      INTEGER NOT NULL DEFAULT 0,
    history_backfill INTEGER NOT NULL DEFAULT 0,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TEXT,
    last_error       TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    UNIQUE (item_id, source_revision, force_files, history_backfill)
);

CREATE INDEX idx_sync_content_outbox_ready
ON sync_content_outbox(next_attempt_at, operation_order);

CREATE INDEX idx_sync_content_outbox_key_order
ON sync_content_outbox(sync_key, operation_order);

ALTER TABLE sync_item_states ADD COLUMN pending_order INTEGER;

UPDATE sync_item_states
SET pending_order = rowid
WHERE dirty_mask <> 0;

CREATE INDEX idx_sync_item_states_pending_order
ON sync_item_states(pending_order) WHERE dirty_mask <> 0;

ALTER TABLE sync_events ADD COLUMN apply_state TEXT NOT NULL DEFAULT 'pending'
    CHECK (apply_state IN ('pending', 'applied', 'rejected'));
ALTER TABLE sync_events ADD COLUMN apply_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_events ADD COLUMN next_apply_at TEXT;
ALTER TABLE sync_events ADD COLUMN last_apply_error TEXT;

UPDATE sync_events
SET apply_state = 'applied'
WHERE is_applied = 1;

CREATE INDEX idx_sync_events_apply_ready
ON sync_events(event_kind, apply_state, next_apply_at, cursor)
WHERE is_applied = 0;
