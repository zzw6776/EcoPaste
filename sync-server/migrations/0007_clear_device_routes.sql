-- LAN routes are learned by clients through local discovery and must not be
-- cached or redistributed by the cloud membership directory.
UPDATE devices
SET direct_addresses = X'', relay_urls = X''
WHERE length(direct_addresses) > 0 OR length(relay_urls) > 0;
