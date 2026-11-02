ALTER TABLE sync_deliveries ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0
    CHECK (confirmed IN (0, 1));

ALTER TABLE sync_delivery_states ADD COLUMN phase TEXT NOT NULL DEFAULT 'queued'
    CHECK (phase IN ('queued', 'uploading', 'confirming'));

CREATE INDEX idx_sync_unconfirmed_cloud_deliveries
ON sync_deliveries(event_id) WHERE target_id = 'cloud' AND confirmed = 0;

INSERT INTO sync_state (key, value, created_at, updated_at)
VALUES ('cloud_confirmation_repair_before', 'start', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
