-- V3__add_firm_id_to_outbox.sql
-- Adds firm_id to outbox_entries so the processor can filter engagements by firm.
-- This is required for the readEngagementIds(List<String> firmIds, String templateId) contract.
--
-- Idempotent: uses IF NOT EXISTS guards so re-runs are safe.

ALTER TABLE outbox_entries
    ADD COLUMN IF NOT EXISTS firm_id VARCHAR(64);

-- Index for efficient queries filtering by firm + template + status
CREATE INDEX IF NOT EXISTS idx_outbox_firm_template_status
    ON outbox_entries(firm_id, template_id, status)
    WHERE status = 'PENDING';

-- Partial index saves space and speeds up the common query pattern (status = 'PENDING')

COMMENT ON COLUMN outbox_entries.firm_id IS 'Owner firm for this engagement; used to route updates to regional processors';
