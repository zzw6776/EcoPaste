PRAGMA foreign_keys = ON;

-- Separate data-plane cloud upload policy (`auto`, `defer`, `requested`) from control-plane
-- channel visibility and per-target delivery confirmation facts.
ALTER TABLE sync_items ADD COLUMN cloud_blob_policy TEXT NOT NULL DEFAULT 'auto';

UPDATE sync_items
SET cloud_blob_policy = CASE
    WHEN delivery_route = 'cloud' THEN 'requested'
    WHEN delivery_route = 'lan'
      OR EXISTS (
          SELECT 1 FROM sync_item_targets t
          WHERE t.sync_key = sync_items.sync_key
            AND t.target_id = 'cloud'
            AND t.desired_content = 'metadata'
      ) THEN 'defer'
    ELSE 'auto'
END;

-- Reconcile existing metadata-only cloud confirmations so they no longer look pending.
UPDATE sync_item_targets
SET status = 'confirmed'
WHERE target_id = 'cloud'
  AND desired_content = 'metadata'
  AND confirmed_content = 'metadata'
  AND status IN ('pending', 'awaiting_receipt');

ALTER TABLE sync_items DROP COLUMN delivery_route;
ALTER TABLE sync_item_targets DROP COLUMN desired_content;
