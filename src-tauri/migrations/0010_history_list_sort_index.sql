CREATE INDEX idx_clipboard_items_updated_sort
ON clipboard_items(is_pinned DESC, updated_at DESC, created_at DESC);
