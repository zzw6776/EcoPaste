ALTER TABLE events ADD COLUMN event_kind INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_events_group_kind_cursor
ON events(group_id, event_kind, cursor);
