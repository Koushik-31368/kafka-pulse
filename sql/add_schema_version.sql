-- Migration: add schema_version column to pipeline_metrics
-- Safe to re-run (uses IF NOT EXISTS guards).
-- Applied automatically if you use the alembic workflow; otherwise run manually:
--
--   Get-Content sql\add_schema_version.sql | docker exec -i postgres psql -U pipeline_user -d pipeline_db
--

-- 1. Add schema_version column to pipeline_metrics (tracks which version wrote the row)
ALTER TABLE pipeline_metrics
    ADD COLUMN IF NOT EXISTS schema_version TEXT DEFAULT '1.2.0';

-- 2. Add event_schema_version column to raw_events (tracks producer schema version per event)
ALTER TABLE raw_events
    ADD COLUMN IF NOT EXISTS event_schema_version TEXT DEFAULT '1.0';

-- 3. Create an index on event_type + created_at for faster monitor queries
CREATE INDEX IF NOT EXISTS idx_processed_events_type_created
    ON processed_events (event_type, created_at DESC);

-- 4. Create an index on raw_events.event_type for aggregation queries
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type
    ON raw_events (event_type);

-- Verify
SELECT
    column_name,
    data_type,
    column_default
FROM information_schema.columns
WHERE table_name IN ('pipeline_metrics', 'raw_events')
  AND column_name IN ('schema_version', 'event_schema_version')
ORDER BY table_name, column_name;
