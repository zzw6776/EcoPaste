-- Migration 38 invalidated unproven file completions, but replaying their bodies can require
-- expired source files. Keep their confirmed records and let receivers restore content receipts.
UPDATE sync_delivery_targets
SET desired_content = 'metadata',
    status = 'confirmed',
    last_error = NULL,
    retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE target_id LIKE 'peer:%'
  AND desired_content = 'complete'
  AND confirmed_content = 'metadata'
  AND status IN ('pending', 'awaiting_receipt', 'blocked')
  AND julianday(created_at) <= (
      SELECT julianday(installed_on) FROM _sqlx_migrations WHERE version = 38
  )
  AND EXISTS (
      SELECT 1
      FROM sync_events event
      JOIN sync_item_events links ON links.event_id = event.event_id
      JOIN clipboard_items item ON item.id = links.item_id
      WHERE event.event_id = sync_delivery_targets.event_id
        AND sync_delivery_targets.target_id <> 'peer:' || event.origin_device_id
        AND item.kind = 'files'
  );

UPDATE sync_delivery_targets
SET receipt_pending = 1,
    updated_at = CURRENT_TIMESTAMP
WHERE confirmed_content <> 'none'
  AND julianday(created_at) <= (
      SELECT julianday(installed_on) FROM _sqlx_migrations WHERE version = 38
  )
  AND EXISTS (
      SELECT 1
      FROM sync_events event
      JOIN sync_item_events links ON links.event_id = event.event_id
      JOIN clipboard_items item ON item.id = links.item_id
      WHERE event.event_id = sync_delivery_targets.event_id
        AND sync_delivery_targets.target_id = 'peer:' || event.origin_device_id
        AND links.direction = 'remote'
        AND item.kind = 'files'
  );
