ALTER TABLE sync_groups
ADD COLUMN pruned_availability_cursor INTEGER NOT NULL DEFAULT 0;

-- Revalidate existing client facts once when this Hub has already reclaimed history.
UPDATE sync_groups
SET availability_cursor = availability_cursor + 1,
    pruned_availability_cursor = availability_cursor + 1
WHERE pruned_through_cursor > 0;
