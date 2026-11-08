ALTER TABLE sync_event_blobs ADD COLUMN source_path TEXT;
ALTER TABLE sync_event_blobs ADD COLUMN source_is_directory INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_event_blobs ADD COLUMN stream_nonce BLOB;

ALTER TABLE sync_source_icon_uploads ADD COLUMN source_path TEXT;
ALTER TABLE sync_source_icon_uploads ADD COLUMN source_is_directory INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_source_icon_uploads ADD COLUMN stream_nonce BLOB;
