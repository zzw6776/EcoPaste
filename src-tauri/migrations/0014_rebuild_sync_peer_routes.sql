-- LAN direct addresses are volatile mDNS observations. Existing rows do not
-- retain the interface that observed each route, so rebuild them instead of
-- guessing whether a private address belongs to a physical or virtual network.
UPDATE sync_peers
SET direct_addresses = '[]', updated_at = CURRENT_TIMESTAMP
WHERE direct_addresses <> '[]';

-- Preserve the last successful direct route as a proven fallback for background
-- reconnects, especially when Android is not temporarily holding MulticastLock.
UPDATE sync_peer_connections
SET state = 'offline',
    last_error = NULL,
    updated_at = CURRENT_TIMESTAMP;
