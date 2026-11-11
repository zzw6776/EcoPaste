-- Older clients did not persist whether a LAN file delivery included bytes. Treat those rows as
-- unverified metadata delivery until a new transfer confirms the content on that peer.
UPDATE sync_deliveries
SET content_delivered = 0
WHERE target_id LIKE 'peer:%'
  AND event_id IN (
      SELECT links.event_id
      FROM sync_item_events links
      JOIN clipboard_items item ON item.id = links.item_id
      WHERE links.direction = 'local' AND item.kind = 'files'
  );
