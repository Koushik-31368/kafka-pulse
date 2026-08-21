-- Migration: add latency_ms column to processed_events
-- Safe to run multiple times (IF NOT EXISTS guard).
-- Existing rows will have NULL latency_ms, which is correct.

ALTER TABLE processed_events
    ADD COLUMN IF NOT EXISTS latency_ms NUMERIC(12, 3);

-- Index to make percentile queries fast on large batches
CREATE INDEX IF NOT EXISTS idx_processed_events_latency_ms
    ON processed_events (latency_ms)
    WHERE latency_ms IS NOT NULL;

COMMENT ON COLUMN processed_events.latency_ms IS
    'End-to-end latency in milliseconds: time between producer send (event.timestamp) and DB INSERT (NOW() in consumer).';
