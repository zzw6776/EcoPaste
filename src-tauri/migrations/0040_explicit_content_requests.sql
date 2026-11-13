-- Keep an explicit content request on its route target, independently of automatic routing.
ALTER TABLE sync_delivery_targets
ADD COLUMN content_requested INTEGER NOT NULL DEFAULT 0
CHECK (content_requested IN (0, 1));
