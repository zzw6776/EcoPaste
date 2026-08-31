ALTER TABLE clipboard_apps ADD COLUMN icon_hash TEXT;
ALTER TABLE clipboard_apps ADD COLUMN accent_start TEXT;
ALTER TABLE clipboard_apps ADD COLUMN accent_end TEXT;

ALTER TABLE clipboard_items ADD COLUMN source_revision TEXT NOT NULL DEFAULT '';
ALTER TABLE clipboard_items ADD COLUMN source_updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z';

UPDATE clipboard_items
SET source_revision = id,
    source_updated_at = updated_at;

CREATE TABLE source_app_sync_aliases (
    group_id           TEXT NOT NULL,
    source_key         TEXT NOT NULL,
    app_id             TEXT NOT NULL REFERENCES clipboard_apps(id) ON DELETE CASCADE,
    icon_hash          TEXT,
    blob_id            TEXT,
    icon_original_size INTEGER,
    icon_encrypted_size INTEGER,
    source_updated_at  TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    PRIMARY KEY (group_id, source_key)
);

CREATE INDEX idx_source_app_sync_aliases_app_id
ON source_app_sync_aliases (app_id);
