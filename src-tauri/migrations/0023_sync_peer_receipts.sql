CREATE TABLE sync_peer_receipts (
    peer_device_id TEXT NOT NULL REFERENCES sync_peers(device_id) ON DELETE CASCADE,
    event_id       TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (peer_device_id, event_id)
);

CREATE INDEX idx_sync_peer_receipts_created
ON sync_peer_receipts(peer_device_id, created_at);

-- Successful delivery has one durable source of truth: sync_deliveries.
DELETE FROM sync_delivery_states WHERE state = 'success';

-- An event never needs to be delivered back to the device that originated it.
DELETE FROM sync_delivery_states
WHERE target_id IN (
    SELECT 'peer:' || origin_device_id FROM sync_events
    WHERE sync_events.event_id = sync_delivery_states.event_id
);

DELETE FROM sync_canceled_deliveries
WHERE target_id IN (
    SELECT 'peer:' || origin_device_id FROM sync_events
    WHERE sync_events.event_id = sync_canceled_deliveries.event_id
);
