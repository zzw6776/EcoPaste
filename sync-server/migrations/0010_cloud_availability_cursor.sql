ALTER TABLE sync_groups
ADD COLUMN availability_cursor INTEGER NOT NULL DEFAULT 0;

ALTER TABLE events
ADD COLUMN availability_cursor INTEGER NOT NULL DEFAULT 0;

UPDATE events
SET availability_cursor = cursor;

UPDATE sync_groups
SET availability_cursor = COALESCE((
    SELECT MAX(events.cursor)
    FROM events
    WHERE events.group_id = sync_groups.group_id
), 0);

CREATE INDEX idx_events_group_availability_cursor
ON events(group_id, availability_cursor);
