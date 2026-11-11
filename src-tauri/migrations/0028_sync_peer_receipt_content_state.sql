ALTER TABLE sync_peer_receipts ADD COLUMN content_delivered INTEGER NOT NULL DEFAULT 0
    CHECK (content_delivered IN (0, 1));
