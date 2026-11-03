-- V2__create_outbox_entries.sql
-- Pre-populated by the resolver; processor polls this table to fetch engagements

CREATE TABLE outbox_entries (
    task_id BIGSERIAL PRIMARY KEY,
    engagement_id VARCHAR(64) NOT NULL,
    template_id VARCHAR(64) NOT NULL,
    template_version VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    summary_artifact_path VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Critical: Idempotency constraint — same engagement + template version = one task
CREATE UNIQUE INDEX idx_outbox_unique_engagement_template 
    ON outbox_entries(engagement_id, template_id, template_version);

-- Index for efficient polling: find all PENDING tasks ordered by creation
CREATE INDEX idx_outbox_status_created ON outbox_entries(status, created_at ASC);

-- Index for reconciliation: find stuck entries (updated_at too old)
CREATE INDEX idx_outbox_updated_at ON outbox_entries(updated_at DESC);

-- Foreign key-style comment (logical relationship, not enforced FK)
COMMENT ON TABLE outbox_entries IS 'Work queue for engagement template updates; populated by resolver, consumed by processor';
COMMENT ON COLUMN outbox_entries.task_id IS 'Unique workflow identifier for this update (correlates with Kafka event + engagement)';
COMMENT ON COLUMN outbox_entries.status IS 'Enum: PENDING, PROCESSING, COMPLETED, FAILED, EXPIRED';
COMMENT ON COLUMN outbox_entries.summary_artifact_path IS 'Path to immutable summary once generated';