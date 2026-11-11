ALTER TABLE sync_item_events ADD COLUMN content_identity TEXT;

CREATE INDEX idx_sync_item_events_content_identity
    ON sync_item_events(item_id, content_identity, created_at DESC);
