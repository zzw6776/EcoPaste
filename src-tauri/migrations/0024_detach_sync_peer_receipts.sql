CREATE TABLE sync_peer_receipts_next (
    peer_device_id TEXT NOT NULL REFERENCES sync_peers(device_id) ON DELETE CASCADE,
    event_id       TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (peer_device_id, event_id)
);

INSERT INTO sync_peer_receipts_next (
    peer_device_id,
    event_id,
    created_at,
    updated_at
)
SELECT
    peer_device_id,
    event_id,
    created_at,
    updated_at
FROM sync_peer_receipts;

DROP TABLE sync_peer_receipts;
ALTER TABLE sync_peer_receipts_next RENAME TO sync_peer_receipts;

CREATE INDEX idx_sync_peer_receipts_created
ON sync_peer_receipts(peer_device_id, created_at);
