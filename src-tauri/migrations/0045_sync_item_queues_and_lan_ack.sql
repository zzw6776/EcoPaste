PRAGMA foreign_keys = ON;

-- Separate asynchronous Data-Plane blob pulls from inline metadata/text application so
-- unavailable or queued remote attachments never clog inline apply windows.
ALTER TABLE sync_items ADD COLUMN needs_blob_pull INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_sync_items_needs_blob_pull
ON sync_items(needs_blob_pull, payload_updated_at_ms DESC)
WHERE needs_blob_pull = 1;

-- Track each LAN peer's own sequence number separately from our outbound `pushed_local_seq`
-- so asynchronous `AckLanItems` receipts confirm the exact sender revision.
ALTER TABLE sync_item_targets ADD COLUMN remote_seq INTEGER NOT NULL DEFAULT 0;
