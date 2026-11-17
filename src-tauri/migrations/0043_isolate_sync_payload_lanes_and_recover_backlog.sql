-- Isolate sync payload lanes (state vs text vs image vs files) and index event business timestamps.
ALTER TABLE sync_events ADD COLUMN payload_kind TEXT;
ALTER TABLE sync_events ADD COLUMN sync_key TEXT;
ALTER TABLE sync_events ADD COLUMN payload_updated_at_ms INTEGER;

CREATE INDEX IF NOT EXISTS idx_sync_events_unapplied_lane
ON sync_events(is_applied, apply_state, event_kind, payload_kind, payload_updated_at_ms DESC, cursor ASC);

CREATE INDEX IF NOT EXISTS idx_sync_events_outbound_lane
ON sync_events(event_kind, payload_kind, payload_updated_at_ms DESC, cursor ASC);

CREATE INDEX IF NOT EXISTS idx_sync_events_sync_key_cursor
ON sync_events(sync_key, cursor DESC);

CREATE INDEX IF NOT EXISTS idx_sync_events_unindexed
ON sync_events(cursor) WHERE payload_kind IS NULL;

-- Backfill payload_kind, sync_key, and payload_updated_at_ms before any history pruning removes links.
UPDATE sync_events
SET payload_kind = 'state',
    payload_updated_at_ms = COALESCE(payload_updated_at_ms, event_created_at_ms)
WHERE event_kind = 1
  AND (payload_kind IS NULL OR payload_updated_at_ms IS NULL);

UPDATE sync_events
SET payload_kind = (
        SELECT item.kind
        FROM sync_item_events link
        JOIN clipboard_items item ON item.id = link.item_id
        WHERE link.event_id = sync_events.event_id
        ORDER BY link.created_at DESC
        LIMIT 1
    ),
    sync_key = (
        SELECT item.sync_key
        FROM sync_item_events link
        JOIN clipboard_items item ON item.id = link.item_id
        WHERE link.event_id = sync_events.event_id
        ORDER BY link.created_at DESC
        LIMIT 1
    ),
    payload_updated_at_ms = COALESCE(
        (
            SELECT CASE
                WHEN strftime('%s', item.updated_at) IS NOT NULL
                    THEN CAST(strftime('%s', item.updated_at) AS INTEGER) * 1000
                ELSE NULL
            END
            FROM sync_item_events link
            JOIN clipboard_items item ON item.id = link.item_id
            WHERE link.event_id = sync_events.event_id
            ORDER BY link.created_at DESC
            LIMIT 1
        ),
        event_created_at_ms
    )
WHERE event_kind = 0
  AND EXISTS (
      SELECT 1 FROM sync_item_events link WHERE link.event_id = sync_events.event_id
  );

UPDATE sync_events
SET payload_kind = 'files',
    payload_updated_at_ms = COALESCE(payload_updated_at_ms, event_created_at_ms)
WHERE event_kind = 0
  AND payload_kind IS NULL
  AND EXISTS (
      SELECT 1 FROM sync_remote_file_items remote WHERE remote.event_id = sync_events.event_id
  );

-- Prune orphaned, excess backfill, and superseded entries in sync_content_outbox.
DELETE FROM sync_content_outbox
WHERE NOT EXISTS (
    SELECT 1 FROM clipboard_items item WHERE item.id = sync_content_outbox.item_id
);

DELETE FROM sync_content_outbox
WHERE history_backfill = 1
  AND EXISTS (
      SELECT 1
      FROM sync_item_events link
      JOIN sync_events e ON e.event_id = link.event_id
      WHERE link.item_id = sync_content_outbox.item_id
        AND e.event_kind = 0
  );

DELETE FROM sync_content_outbox
WHERE manual_share = 0
  AND EXISTS (
      SELECT 1
      FROM sync_content_outbox newer
      WHERE newer.sync_key = sync_content_outbox.sync_key
        AND newer.operation_order > sync_content_outbox.operation_order
  );

-- Stop peers from retrying outbound uploads for events that have no local managed replica,
-- and supersede older unconfirmed attachment deliveries for the same sync_key.
UPDATE sync_delivery_targets
SET desired_content = 'metadata',
    status = CASE WHEN confirmed_content <> 'none' THEN 'confirmed' ELSE status END,
    last_error = CASE WHEN confirmed_content <> 'none' THEN NULL ELSE last_error END,
    retry_at = CASE WHEN confirmed_content <> 'none' THEN NULL ELSE retry_at END,
    updated_at = CURRENT_TIMESTAMP
WHERE desired_content = 'complete'
  AND confirmed_content <> 'complete'
  AND NOT EXISTS (
      SELECT 1 FROM sync_local_content c
      WHERE c.event_id = sync_delivery_targets.event_id AND c.state = 'managed'
  )
  AND target_id <> 'peer:' || COALESCE(
      (SELECT e.origin_device_id FROM sync_events e WHERE e.event_id = sync_delivery_targets.event_id),
      ''
  );

UPDATE sync_delivery_targets
SET desired_content = 'metadata',
    confirmed_content = CASE WHEN confirmed_content = 'none' THEN 'metadata' ELSE confirmed_content END,
    status = 'confirmed',
    last_error = NULL,
    retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE confirmed_content <> 'complete'
  AND EXISTS (
      SELECT 1
      FROM sync_events cur
      JOIN sync_events newer
        ON newer.event_kind = 0
       AND COALESCE(newer.sync_key, '') <> ''
       AND newer.sync_key = cur.sync_key
       AND newer.cursor > cur.cursor
      WHERE cur.event_id = sync_delivery_targets.event_id
        AND cur.event_kind = 0
        AND COALESCE(cur.payload_kind, '') IN ('image', 'files')
        AND COALESCE(cur.sync_key, '') <> ''
  );
