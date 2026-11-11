-- Route transport history and local content availability are independent facts.
CREATE TABLE sync_event_ingress (
    event_id TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    route TEXT NOT NULL CHECK (route IN ('lan', 'cloud')),
    source_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (
        outcome IN ('metadata', 'received', 'deduplicated', 'external')
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, source_id)
);

CREATE INDEX idx_sync_event_ingress_event_route
    ON sync_event_ingress(event_id, route, updated_at DESC);

CREATE TABLE sync_local_replicas (
    event_id TEXT PRIMARY KEY REFERENCES sync_events(event_id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('metadata', 'managed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE sync_target_replicas (
    event_id TEXT NOT NULL REFERENCES sync_events(event_id) ON DELETE CASCADE,
    target_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('metadata', 'managed', 'external')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, target_id)
);

ALTER TABLE sync_peer_receipts ADD COLUMN content_state TEXT NOT NULL DEFAULT 'metadata'
    CHECK (content_state IN ('metadata', 'managed', 'external'));

UPDATE sync_peer_receipts
SET content_state = CASE WHEN content_delivered = 1 THEN 'managed' ELSE 'metadata' END;

-- Preserve the useful facts from the previous overloaded delivery projection.
INSERT OR IGNORE INTO sync_event_ingress (
    event_id, route, source_id, outcome, created_at, updated_at
)
SELECT DISTINCT
    links.event_id,
    CASE WHEN delivery.target_id = 'cloud' THEN 'cloud' ELSE 'lan' END,
    delivery.target_id,
    CASE
        WHEN remote.item_id IS NOT NULL AND delivery.content_delivered = 1 THEN 'external'
        WHEN delivery.content_delivered = 1 THEN 'received'
        ELSE 'metadata'
    END,
    delivery.created_at,
    delivery.updated_at
FROM sync_item_events links
JOIN sync_deliveries delivery ON delivery.event_id = links.event_id
LEFT JOIN sync_remote_file_items remote
    ON remote.item_id = links.item_id AND remote.event_id = links.event_id
WHERE links.direction = 'remote'
  AND (delivery.target_id = 'cloud' OR delivery.target_id LIKE 'peer:%');

INSERT OR IGNORE INTO sync_local_replicas (
    event_id, state, created_at, updated_at
)
SELECT
    links.event_id,
    CASE WHEN remote.item_id IS NULL THEN 'managed' ELSE 'metadata' END,
    links.created_at,
    links.updated_at
FROM sync_item_events links
LEFT JOIN sync_remote_file_items remote
    ON remote.item_id = links.item_id AND remote.event_id = links.event_id
JOIN sync_events event ON event.event_id = links.event_id
WHERE event.event_kind = 0;

INSERT OR IGNORE INTO sync_target_replicas (
    event_id, target_id, state, created_at, updated_at
)
SELECT
    delivery.event_id,
    delivery.target_id,
    CASE WHEN delivery.content_delivered = 1 THEN 'managed' ELSE 'metadata' END,
    delivery.created_at,
    delivery.updated_at
FROM sync_deliveries delivery
JOIN sync_item_events links ON links.event_id = delivery.event_id
WHERE links.direction = 'local'
  AND (delivery.target_id = 'cloud' OR delivery.target_id LIKE 'peer:%');
