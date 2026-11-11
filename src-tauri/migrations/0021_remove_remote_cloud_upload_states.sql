DELETE FROM sync_delivery_states
WHERE target_id = 'cloud'
  AND state <> 'success'
  AND event_id IN (
      SELECT event_id
      FROM sync_item_events
      WHERE direction = 'remote'
  );
