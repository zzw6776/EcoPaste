ALTER TABLE sync_deliveries ADD COLUMN content_delivered INTEGER NOT NULL DEFAULT 1
    CHECK (content_delivered IN (0, 1));
