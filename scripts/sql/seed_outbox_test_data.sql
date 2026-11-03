-- ============================================================================
-- Test data: populate outbox_entries for the template-publish flow
--
-- PREREQUISITE: Flyway migrations V1-V4 must have run.
-- task_id is BIGSERIAL — we do NOT supply it; Postgres assigns it.
--
-- Aligns with the Kafka test event:
--   templateId = TPL-AUDIT-2024
--   version    = v5.2
--   affectedFirms = FIRM-001, FIRM-002, FIRM-003
--
-- Safe to re-run: the unique index on
-- (engagement_id, template_id, template_version) suppresses duplicates.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- FIRM-001
-- ---------------------------------------------------------------------------
INSERT INTO outbox_entries
(engagement_id, firm_id, template_id, template_version, status)
SELECT
    'ENG-1' || LPAD(i::text, 3, '0'),
    'FIRM-001',
    'TPL-AUDIT-2024',
    'v5.2',
    'PENDING'
FROM generate_series(1, 129) AS i
    ON CONFLICT (engagement_id, template_id, template_version) DO NOTHING;

-- ---------------------------------------------------------------------------
-- FIRM-002
-- ---------------------------------------------------------------------------
INSERT INTO outbox_entries
(engagement_id, firm_id, template_id, template_version, status)
SELECT
    'ENG-2' || LPAD(i::text, 3, '0'),
    'FIRM-002',
    'TPL-AUDIT-2024',
    'v5.2',
    'PENDING'
FROM generate_series(1, 230) AS i
    ON CONFLICT (engagement_id, template_id, template_version) DO NOTHING;

-- ---------------------------------------------------------------------------
-- FIRM-003
-- ---------------------------------------------------------------------------
INSERT INTO outbox_entries
(engagement_id, firm_id, template_id, template_version, status)
SELECT
    'ENG-3' || LPAD(i::text, 3, '0'),
    'FIRM-003',
    'TPL-AUDIT-2024',
    'v5.2',
    'PENDING'
FROM generate_series(1, 480) AS i
    ON CONFLICT (engagement_id, template_id, template_version) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Noise rows: different template + older version, to prove selective pickup.
-- ---------------------------------------------------------------------------
INSERT INTO outbox_entries
(engagement_id, firm_id, template_id, template_version, status)
SELECT
    'ENG-N' || LPAD(i::text, 3, '0'),
    'FIRM-001',
    'TPL-RISK-2024',          -- different template: must NOT be selected
    'v3.1',
    'PENDING'
FROM generate_series(1, 50) AS i
    ON CONFLICT (engagement_id, template_id, template_version) DO NOTHING;

INSERT INTO outbox_entries
(engagement_id, firm_id, template_id, template_version, status)
SELECT
    'ENG-O' || LPAD(i::text, 3, '0'),
    'FIRM-002',
    'TPL-AUDIT-2024',
    'v5.1',                   -- older version: must NOT be selected
    'PENDING'
FROM generate_series(1, 35) AS i
    ON CONFLICT (engagement_id, template_id, template_version) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Verification
-- Expected: 30 PENDING rows for (TPL-AUDIT-2024, v5.2) across FIRM-001..003
-- ---------------------------------------------------------------------------
SELECT firm_id, template_id, template_version, status, COUNT(*) AS rows
FROM outbox_entries
GROUP BY firm_id, template_id, template_version, status
ORDER BY firm_id, template_id, template_version;
