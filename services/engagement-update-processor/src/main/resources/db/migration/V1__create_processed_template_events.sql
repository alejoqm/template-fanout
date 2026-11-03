-- V1__create_processed_template_events.sql
-- Tracks processed Kafka events for idempotency

CREATE TABLE processed_template_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,  -- Deduplicates on arrival
    template_id VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    affected_firms JSONB NOT NULL,         -- Stores list of firm IDs (for audit/replay)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_processed_events_template ON processed_template_events(template_id, version);
CREATE INDEX idx_processed_events_created_at ON processed_template_events(created_at DESC);

-- Comments for documentation
COMMENT ON TABLE processed_template_events IS 'Idempotency tracker: prevents duplicate fan-out for the same publish event';
COMMENT ON COLUMN processed_template_events.event_id IS 'Unique identifier from the producer; stable across redeliveries';
COMMENT ON COLUMN processed_template_events.affected_firms IS 'Pre-computed list of firms using this template (from resolver)';
