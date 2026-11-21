PRAGMA foreign_keys = ON;

-- Separate data-plane cloud upload policy (`auto`, `defer`, `requested`, `canceled`, `blocked`)
-- from pure per-target confirmation facts in `sync_item_targets`.
UPDATE sync_items
SET cloud_blob_policy = 'defer'
WHERE origin = 'remote'
  AND cloud_blob_policy = 'auto';

UPDATE sync_items
SET cloud_blob_policy = 'blocked'
WHERE EXISTS (
    SELECT 1 FROM sync_item_targets t
    WHERE t.sync_key = sync_items.sync_key
      AND t.target_id = 'cloud'
      AND t.confirmed_content <> 'complete'
      AND t.status = 'blocked'
);

UPDATE sync_items
SET cloud_blob_policy = 'canceled'
WHERE EXISTS (
    SELECT 1 FROM sync_item_targets t
    WHERE t.sync_key = sync_items.sync_key
      AND t.target_id = 'cloud'
      AND t.confirmed_content <> 'complete'
      AND t.status = 'canceled'
);

-- Drop half-baked rows left behind by canceled initial content preparations that never produced a snapshot.
DELETE FROM sync_items
WHERE prep_state = 'ready'
  AND nonce IS NULL
  AND deleted = 0
  AND origin = 'local';

-- Remove synthetic placeholder target rows that hold neither confirmation facts nor an error.
DELETE FROM sync_item_targets
WHERE confirmed_content = 'none'
  AND pushed_local_seq = 0
  AND remote_seq = 0
  AND last_error IS NULL;

DROP INDEX IF EXISTS idx_sync_item_targets_pending;
ALTER TABLE sync_item_targets DROP COLUMN status;
ALTER TABLE sync_item_targets DROP COLUMN acked_local_seq;

CREATE INDEX idx_sync_item_targets_target_seq
ON sync_item_targets(target_id, pushed_local_seq);
