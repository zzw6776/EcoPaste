-- Clean up invalid cloud target rows left on LAN-only items by 1.1.231 so they return to idle clickable state.
DELETE FROM sync_item_targets
WHERE target_id = 'cloud'
  AND pushed_local_seq = 0
  AND acked_local_seq = 0
  AND sync_key IN (
      SELECT sync_key FROM sync_items WHERE delivery_route = 'lan'
  );

-- Migrate legacy pull-model LAN metadata targets from awaiting_receipt to confirmed so senders
-- show blue metadata-only state instead of staying permanently stuck in queued.
UPDATE sync_item_targets
SET status = 'confirmed'
WHERE target_id LIKE 'peer:%'
  AND status = 'awaiting_receipt'
  AND confirmed_content = 'metadata';
