-- Resume only work quarantined by the erroneous source-device display validation.
UPDATE sync_delivery_targets
SET status = 'pending', last_error = NULL, retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'blocked'
  AND last_error = 'invalid synchronized source device';

UPDATE sync_events
SET is_applied = 0, apply_state = 'pending', apply_attempt_count = 0,
    next_apply_at = NULL, last_apply_error = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE apply_state = 'rejected'
  AND last_apply_error = 'invalid synchronized source device';
