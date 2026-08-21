-- Initialize the pipeline database schema

-- Table to store raw events received from Kafka
CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL UNIQUE,
    event_type VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Table to store processed/aggregated data
CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL REFERENCES raw_events(event_id),
    user_id INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2),
    status VARCHAR(20) DEFAULT 'processed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table to track pipeline metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15, 4) NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw_events(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_received_at ON raw_events(received_at);
CREATE INDEX IF NOT EXISTS idx_processed_events_user_id ON processed_events(user_id);

-- Sample view for quick reporting
CREATE OR REPLACE VIEW event_summary AS
SELECT
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    MAX(received_at) AS last_event_at
FROM raw_events
GROUP BY event_type;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
