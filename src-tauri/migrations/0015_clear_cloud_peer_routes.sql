-- Hub responses from older sync services could restore routes after migration 0014.
-- Rebuild the candidate list from fresh local discovery while retaining the
-- last proven direct route used for background reconnects.
UPDATE sync_peers
SET direct_addresses = '[]',
    relay_urls = '[]',
    updated_at = CURRENT_TIMESTAMP
WHERE direct_addresses <> '[]' OR relay_urls <> '[]';

UPDATE sync_peer_connections
SET state = 'offline',
    last_error = NULL,
    updated_at = CURRENT_TIMESTAMP;
