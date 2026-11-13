-- Capture provenance independently of the device that later publishes a sync event.
ALTER TABLE clipboard_items ADD COLUMN source_device_id TEXT;
ALTER TABLE clipboard_items ADD COLUMN source_device_name TEXT;
