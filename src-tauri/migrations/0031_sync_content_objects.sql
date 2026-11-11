CREATE TABLE sync_content_objects (
    group_id TEXT NOT NULL,
    plaintext_hash TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    stream_nonce BLOB NOT NULL,
    original_size INTEGER NOT NULL,
    encrypted_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (group_id, plaintext_hash),
    UNIQUE (group_id, blob_id)
);
