-- A remote file card marked as available contains metadata only, not managed file bytes.
UPDATE sync_deliveries
SET content_delivered = 0,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id IN (
    SELECT event_id
    FROM sync_remote_file_items
    WHERE state = 'available' AND event_id IS NOT NULL
);
