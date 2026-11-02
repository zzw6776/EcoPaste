ALTER TABLE sync_content_outbox ADD COLUMN manual_share INTEGER NOT NULL DEFAULT 0
    CHECK (manual_share IN (0, 1));
