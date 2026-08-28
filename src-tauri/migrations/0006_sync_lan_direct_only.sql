-- LAN synchronization no longer supports Relay routes. Remove cached Relay
-- candidates and invalidate any previously reported Relay connection path.
UPDATE sync_peers
SET relay_urls = '[]', updated_at = CURRENT_TIMESTAMP
WHERE relay_urls <> '[]';

UPDATE sync_peer_connections
SET state = 'offline',
    connected_address = NULL,
    transport = NULL,
    last_error = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE transport = 'relay';
