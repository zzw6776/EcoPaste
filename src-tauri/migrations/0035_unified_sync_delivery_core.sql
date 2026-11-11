-- One row owns the desired and confirmed content level for one event/route target.
CREATE TABLE sync_delivery_targets (
    event_id          TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id         TEXT NOT NULL,
    desired_content   TEXT NOT NULL DEFAULT 'metadata'
                      CHECK (desired_content IN ('metadata', 'complete')),
    confirmed_content TEXT NOT NULL DEFAULT 'none'
                      CHECK (confirmed_content IN ('none', 'metadata', 'complete')),
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'confirmed', 'blocked', 'canceled')),
    receipt_pending   INTEGER NOT NULL DEFAULT 0 CHECK (receipt_pending IN (0, 1)),
    last_error        TEXT,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    retry_at          TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

CREATE INDEX idx_sync_delivery_targets_pending
ON sync_delivery_targets(target_id, status, retry_at, updated_at);

CREATE INDEX idx_sync_delivery_targets_receipts
ON sync_delivery_targets(target_id, receipt_pending, created_at);

-- Preserve every confirmed route before retiring the overlapping delivery tables.
INSERT INTO sync_delivery_targets (
    event_id, target_id, desired_content, confirmed_content, status,
    last_error, created_at, updated_at
)
SELECT
    delivery.event_id,
    delivery.target_id,
    CASE
        WHEN forced.event_id IS NOT NULL OR delivery.content_delivered = 1 THEN 'complete'
        ELSE 'metadata'
    END,
    CASE
        WHEN delivery.confirmed = 0 THEN 'none'
        WHEN replica.state IN ('managed', 'external') OR delivery.content_delivered = 1
            THEN 'complete'
        ELSE 'metadata'
    END,
    CASE WHEN delivery.confirmed = 1 THEN 'confirmed' ELSE 'pending' END,
    state.last_error,
    delivery.created_at,
    MAX(delivery.updated_at, COALESCE(state.updated_at, delivery.updated_at))
FROM sync_deliveries delivery
LEFT JOIN sync_target_replicas replica
    ON replica.event_id = delivery.event_id AND replica.target_id = delivery.target_id
LEFT JOIN sync_forced_content_deliveries forced
    ON forced.event_id = delivery.event_id AND forced.target_id = delivery.target_id
LEFT JOIN sync_delivery_states state
    ON state.event_id = delivery.event_id AND state.target_id = delivery.target_id;

-- Preserve pending and failed attempts which did not yet create a confirmed delivery.
INSERT OR IGNORE INTO sync_delivery_targets (
    event_id, target_id, desired_content, confirmed_content, status,
    last_error, attempt_count, created_at, updated_at
)
SELECT
    state.event_id,
    state.target_id,
    CASE WHEN forced.event_id IS NULL THEN 'metadata' ELSE 'complete' END,
    'none',
    'pending',
    state.last_error,
    CASE WHEN state.last_attempt_at IS NULL THEN 0 ELSE 1 END,
    state.created_at,
    state.updated_at
FROM sync_delivery_states state
LEFT JOIN sync_forced_content_deliveries forced
    ON forced.event_id = state.event_id AND forced.target_id = state.target_id;

-- A replica acknowledgement is authoritative even when an older delivery row is absent.
INSERT OR IGNORE INTO sync_delivery_targets (
    event_id, target_id, desired_content, confirmed_content, status,
    created_at, updated_at
)
SELECT
    replica.event_id,
    replica.target_id,
    CASE WHEN replica.state = 'metadata' THEN 'metadata' ELSE 'complete' END,
    CASE WHEN replica.state = 'metadata' THEN 'metadata' ELSE 'complete' END,
    'confirmed',
    replica.created_at,
    replica.updated_at
FROM sync_target_replicas replica;

-- Inbound route availability uses the same route record as outbound acknowledgements.
INSERT OR IGNORE INTO sync_delivery_targets (
    event_id, target_id, desired_content, confirmed_content, status,
    created_at, updated_at
)
SELECT
    ingress.event_id,
    ingress.source_id,
    CASE WHEN ingress.outcome = 'metadata' THEN 'metadata' ELSE 'complete' END,
    CASE WHEN ingress.outcome = 'metadata' THEN 'metadata' ELSE 'complete' END,
    'confirmed',
    ingress.created_at,
    ingress.updated_at
FROM sync_event_ingress ingress;

-- Receipt delivery is queue state on the same event/peer fact, not a second content fact.
INSERT INTO sync_delivery_targets (
    event_id, target_id, desired_content, confirmed_content, status,
    receipt_pending, created_at, updated_at
)
SELECT
    receipt.event_id,
    'peer:' || receipt.peer_device_id,
    CASE WHEN receipt.content_state = 'metadata' THEN 'metadata' ELSE 'complete' END,
    CASE WHEN receipt.content_state = 'metadata' THEN 'metadata' ELSE 'complete' END,
    'confirmed',
    1,
    receipt.created_at,
    receipt.updated_at
FROM sync_peer_receipts receipt
WHERE 1 = 1
ON CONFLICT(event_id, target_id) DO UPDATE SET
    confirmed_content = CASE
        WHEN excluded.confirmed_content = 'complete' THEN 'complete'
        ELSE sync_delivery_targets.confirmed_content
    END,
    receipt_pending = 1,
    updated_at = excluded.updated_at;

UPDATE sync_delivery_targets
SET desired_content = 'complete',
    status = CASE
        WHEN confirmed_content = 'complete' THEN 'confirmed'
        ELSE 'pending'
    END,
    last_error = CASE WHEN confirmed_content = 'complete' THEN NULL ELSE last_error END,
    updated_at = CURRENT_TIMESTAMP
WHERE EXISTS (
    SELECT 1 FROM sync_forced_content_deliveries forced
    WHERE forced.event_id = sync_delivery_targets.event_id
      AND forced.target_id = sync_delivery_targets.target_id
);

UPDATE sync_delivery_targets
SET status = CASE
        WHEN last_error = 'transfer canceled' THEN 'canceled'
        ELSE 'blocked'
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE EXISTS (
    SELECT 1 FROM sync_canceled_deliveries canceled
    WHERE canceled.event_id = sync_delivery_targets.event_id
      AND canceled.target_id = sync_delivery_targets.target_id
);

-- Local materialization is independent from route availability.
CREATE TABLE sync_local_content (
    event_id           TEXT PRIMARY KEY REFERENCES sync_events(event_id) ON DELETE CASCADE,
    state              TEXT NOT NULL
                       CHECK (state IN ('metadata', 'managed', 'external', 'external_missing')),
    destinations_json  TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

INSERT INTO sync_local_content (
    event_id, state, destinations_json, created_at, updated_at
)
SELECT event_id, state, NULL, created_at, updated_at
FROM sync_local_replicas;

-- Preserve the latest externally saved destination set for each event.
UPDATE sync_local_content
SET state = 'external',
    destinations_json = (
        SELECT json_group_array(json_object(
            'destinationKind', target.destination_kind,
            'destination', target.destination,
            'expectedSize', target.expected_size,
            'isDirectory', target.is_directory
        ))
        FROM sync_external_download_sessions session
        JOIN sync_external_download_targets target ON target.session_id = session.id
        WHERE session.id = (
            SELECT latest.id
            FROM sync_external_download_sessions latest
            WHERE latest.event_id = sync_local_content.event_id
            ORDER BY latest.created_at DESC
            LIMIT 1
        )
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE EXISTS (
    SELECT 1 FROM sync_external_download_sessions session
    WHERE session.event_id = sync_local_content.event_id
);

INSERT INTO sync_local_content (
    event_id, state, destinations_json, created_at, updated_at
)
SELECT
    session.event_id,
    'external',
    json_group_array(json_object(
        'destinationKind', target.destination_kind,
        'destination', target.destination,
        'expectedSize', target.expected_size,
        'isDirectory', target.is_directory
    )),
    session.created_at,
    session.updated_at
FROM sync_external_download_sessions session
JOIN sync_external_download_targets target ON target.session_id = session.id
WHERE session.id = (
    SELECT latest.id
    FROM sync_external_download_sessions latest
    WHERE latest.event_id = session.event_id
    ORDER BY latest.created_at DESC
    LIMIT 1
)
  AND NOT EXISTS (
      SELECT 1 FROM sync_local_content local WHERE local.event_id = session.event_id
  )
GROUP BY session.id, session.event_id, session.created_at, session.updated_at;

DROP TABLE sync_external_download_targets;
DROP TABLE sync_external_download_sessions;
DROP TABLE sync_peer_receipts;
DROP TABLE sync_forced_content_deliveries;
DROP TABLE sync_event_ingress;
DROP TABLE sync_local_replicas;
DROP TABLE sync_target_replicas;
DROP TABLE sync_delivery_states;
DROP TABLE sync_canceled_deliveries;
DROP TABLE sync_deliveries;

DELETE FROM sync_state WHERE key = 'cloud_confirmation_repair_before';
