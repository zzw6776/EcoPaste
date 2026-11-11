UPDATE sync_remote_file_items
SET state = 'expired',
    event_id = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE event_id IN (
    SELECT event_id
    FROM sync_events
    WHERE substr(ciphertext, 1, 8) <> X'45434F4556413032'
);

DELETE FROM sync_events
WHERE substr(ciphertext, 1, 8) <> X'45434F4556413032';

DELETE FROM sync_state
WHERE key IN ('history_backfill_group_id', 'cloud_retention_snapshot');
