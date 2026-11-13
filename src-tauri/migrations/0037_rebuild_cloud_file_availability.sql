-- A cloud echo once confused the sender's local complete replica with Hub blob availability.
-- Rebuild only file-route facts; the availability stream restores blobs that really exist.
UPDATE sync_delivery_targets
SET desired_content = 'metadata',
    confirmed_content = 'metadata',
    status = 'confirmed',
    receipt_pending = 0,
    last_error = NULL,
    retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE target_id = 'cloud'
  AND confirmed_content = 'complete'
  AND EXISTS (
      SELECT 1
      FROM sync_item_events links
      JOIN clipboard_items item ON item.id = links.item_id
      WHERE links.event_id = sync_delivery_targets.event_id
        AND item.kind = 'files'
  );

UPDATE sync_state
SET value = '0', updated_at = CURRENT_TIMESTAMP
WHERE key = 'cloud_availability_cursor';
