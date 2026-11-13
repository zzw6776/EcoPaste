-- Preserve delivery facts while adding a transport-complete state that cannot be resent.
CREATE TABLE sync_delivery_targets_receipt_wait (
    event_id          TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id         TEXT NOT NULL,
    desired_content   TEXT NOT NULL DEFAULT 'metadata'
                      CHECK (desired_content IN ('metadata', 'complete')),
    confirmed_content TEXT NOT NULL DEFAULT 'none'
                      CHECK (confirmed_content IN ('none', 'metadata', 'complete')),
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'awaiting_receipt', 'confirmed', 'blocked', 'canceled')),
    receipt_pending   INTEGER NOT NULL DEFAULT 0 CHECK (receipt_pending IN (0, 1)),
    last_error        TEXT,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    retry_at          TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

INSERT INTO sync_delivery_targets_receipt_wait (
    event_id, target_id, desired_content, confirmed_content, status,
    receipt_pending, last_error, attempt_count, retry_at, created_at, updated_at
)
SELECT event_id, target_id, desired_content, confirmed_content, status,
       receipt_pending, last_error, attempt_count, retry_at, created_at, updated_at
FROM sync_delivery_targets;

DROP TABLE sync_delivery_targets;
ALTER TABLE sync_delivery_targets_receipt_wait RENAME TO sync_delivery_targets;

CREATE INDEX idx_sync_delivery_targets_pending
ON sync_delivery_targets(target_id, status, retry_at, updated_at);

CREATE INDEX idx_sync_delivery_targets_receipts
ON sync_delivery_targets(target_id, receipt_pending, created_at);

-- Outbound file completions had no proof distinguishing record acknowledgement from receipt.
-- Replan them once; the receiver skips existing files and applies its own download limit.
UPDATE sync_delivery_targets
SET confirmed_content = 'metadata',
    status = CASE WHEN status IN ('blocked', 'canceled') THEN status ELSE 'pending' END,
    receipt_pending = 0,
    last_error = CASE WHEN status IN ('blocked', 'canceled') THEN last_error ELSE NULL END,
    retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE target_id LIKE 'peer:%'
  AND confirmed_content = 'complete'
  AND EXISTS (
      SELECT 1
      FROM sync_events event
      JOIN sync_item_events links ON links.event_id = event.event_id
      JOIN clipboard_items item ON item.id = links.item_id
      WHERE event.event_id = sync_delivery_targets.event_id
        AND sync_delivery_targets.target_id <> 'peer:' || event.origin_device_id
        AND item.kind = 'files'
  );

-- Existing receivers republish their real metadata/content receipts after both sides upgrade.
UPDATE sync_delivery_targets
SET receipt_pending = 1,
    updated_at = CURRENT_TIMESTAMP
WHERE confirmed_content <> 'none'
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
