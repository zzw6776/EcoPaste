CREATE TABLE sync_source_icon_uploads (
    group_id       TEXT NOT NULL,
    blob_id        TEXT NOT NULL,
    encrypted_path TEXT NOT NULL,
    size           INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (group_id, blob_id)
);
