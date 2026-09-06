DROP TRIGGER clipboard_items_au;

CREATE TRIGGER clipboard_items_au
AFTER UPDATE OF search_text, note ON clipboard_items
WHEN old.search_text IS NOT new.search_text OR old.note IS NOT new.note
BEGIN
    INSERT INTO clipboard_items_fts(clipboard_items_fts, rowid, search_text, note)
    VALUES ('delete', old.rowid, old.search_text, old.note);
    INSERT INTO clipboard_items_fts(rowid, search_text, note)
    VALUES (new.rowid, new.search_text, new.note);
END;

CREATE INDEX idx_sync_events_unapplied_cursor
ON sync_events(cursor) WHERE is_applied = 0;
