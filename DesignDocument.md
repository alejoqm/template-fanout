# Design Document

## Executive Summary

This document addresses the system design challenge of tracking product template updates across approximately **800,000 engagement files** from **~4,000 firms**, with requirements around real-time visibility, human-readable summaries, cost constraints, data residency (EU/Canada), and handling complex version branching/merging.

### Primary Architectural Decision

Implement a **projection/index layer outside the engagement store** to maintain per-engagement records of "which template version lineage is currently pinned." This avoids the impossible constraint of reading all 800k engagements (~1 min each) at publish time.

We maintain the relationship between Product Templates and Engagement Files via an event-driven index. For consistency and idempotency, the `(engagement_id, firm_id, template_id)` combination is unique; template version updates when a customer applies a new version.

| Event | Action |
|-------|--------|
| `ENGAGEMENT_CREATED` | Insert row into central index: `(engagement_id, firm_id, template_id, template_version)` |
| `ENGAGEMENT_DELETED` | Mark row as soft-deleted (audit trail preserved) |

**Note on region:** the index deliberately does **not** carry a region column. Region is a property of the *firm*, not of an individual engagement, and is resolved through a separate **Firm Registry** table. Keeping region out of the index avoids duplicating it across hundreds of thousands of rows and avoids a mass update whenever a firm relocates.

---

## Glossary

| Term | Definition |
|------|------------|
| **Product Template** | Shared, non-firm-specific template defining engagement structure |
| **Engagement File** | Firm-owned document created from a Product Template; region-bound by firm regulations |
| **Template Version** | Iterative update to a Product Template with branching/merging support |
| **Pending Update** | Indicator that an engagement file has a newer template version available |
| **Fan-out Worker** | Component that propagates template updates to affected engagements |
| **Index/Projection** | Denormalized mapping of `(template_id, version) → [engagement_ids]` |
| **Firm Registry** | Authoritative `firm_id → region` mapping; the single place residency is recorded |
| **Outbox Pattern** | Reliable event emission pattern where events are written to same DB transaction |

---

## Core Components

### 1. Template Management System (Existing - Non-Modifiable)
- Stores Product Templates and all versions
- Publish action triggers `PRODUCT_UPDATED`, `ENGAGEMENT_CREATED`, `ENGAGEMENT_DELETED` events
- Cannot be significantly modified (no maintenance windows)

### 2. Engagement Management System (Existing - Non-Modifiable)
- Stores 800k engagement files across 4,000 firms
- Loading one engagement takes ~1 minute (hard constraint)
- Region-bound by firm data residency requirements (EU/Canada)

### 3. Firm Registry (New)

Small, slowly-changing table holding `(firm_id, region)`. This is the **only** place residency is recorded, which means:

- The index carries no region column, so no redundant copies across 800k rows
- A firm changing region is a single-row update, not a bulk rewrite
- Routing logic joins the index against this table at fan-out time

Expected size: ~4,000 rows. Updated only on firm onboarding or relocation (rare).

### 4. Template-Engagement Index (New)

Built incrementally at engagement creation time via event listener — not discovered at publish time. Entry point: listen to `ENGAGEMENT_CREATED` events from the engagement management system. Each event captures `(engagement_id, firm_id, template_id, template_version)` and writes to the index immediately. No backfill needed; index grows organically as firms use templates.

| Tier | Scope | Row Shape | Purpose |
|------|-------|-----------|---------|
| Central | Cross-firm routing | `(template_id, firm_id)` | Answer "which firms use template X?" |
| Regional | Within-region fan-out | `(engagement_id, template_id, version, status)` | Per-engagement processing state |

Region is intentionally absent from both tiers. The central tier answers *which firms* are affected; the Firm Registry answers *where those firms live*.

### 5. engagement-indexer (Central Lambda)

Listens to `ENGAGEMENT_CREATED` events and populates the central index with `(template_id, firm_id)` mappings. Runs once per new engagement, updating the centralized database that tracks which firms use which templates.

### 6. template-publish-resolver (Central Lambda)

Listens to `PRODUCT_UPDATED` events and queries the central index to discover which firms are affected by a template change. Returns a list of `(firm_id, template_id, version)` tuples for downstream routing.

### 7. affected-engagement-resolver (Regional Lambdas)

One instance per region (EU, Canada, US). Receives firm IDs from template-publish-resolver, queries the regional index for engagement IDs belonging to those firms in that region, and populates the regional outbox table with pending update entries.

### 8. engagement-update-processor (Regional Fargate Tasks)

Polls the regional outbox table, executes downstream calls to load engagements (~1 min each), and coordinates with the summary generation service. Handles retries, timeouts, and dead-letter queuing.

### 9. Outbox Table (New)

Records `(engagement_id, template_version, status, created_at, updated_at)`

| Status | Meaning | Latency Target |
|--------|---------|----------------|
| `PENDING_INDICATOR` | New template version detected; pending alert visible to user | Seconds |
| `SUMMARY_GENERATING` | Summary computation in progress | Minutes to hours |
| `SUMMARY_READY` | Human-readable summary available for review | Async complete |
| `APPLIED` | User accepted; changes merged | Asynchronously processed |
| `DECLINED` | User rejected; pinned to current version | Immediate |
| `EXPIRED` | Template withdrawn or replaced by newer version | Eventual |

- **Timeout handling:** If status stays `SUMMARY_GENERATING` > 15 minutes, retry generation
- **Idempotency:** Unique constraint on `(engagement_id, template_version)` prevents duplicate processing

### 10. Summary Generation Service (New)

- Generates human-readable diff summaries from JSON changes (ADDED/REMOVED/UPDATED groups)
- **Immutable once shown** (regulatory defensibility)
- Content-addressed with SHA-256 hash for provenance
- Stored alongside pending-update decision, not regenerated on demand
- Audit tables track all changes with timestamps and actor information

---

## Architecture Flow

```
                                    ENGAGEMENT_CREATED
                                    │
                                    ▼
                          ┌──────────────────────┐
                          │ engagement-indexer   │
                          │   (Central Lambda)   │
                          └──────────┬───────────┘
                                     │
                         INSERT (template_id, firm_id)
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   Central Index      │
                          │   (Persistent)       │
                          └──────────────────────┘


                                    PRODUCT_UPDATED
                                    │
                                    ▼
                          ┌──────────────────────┐
                          │ template-publish-resolver  │
                          │   (Central Lambda)   │
                          └──────────┬───────────┘
                                     │
                          QUERY: which firms use this template?
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   Central Index      │
                          │ + Firm Registry      │
                          └──────────┬───────────┘
                                     │
                          GROUP BY: EU, CANADA, US
                                     │
                ┌────────────────────┼────────────────────┐
                ▼                    ▼                    ▼
       ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
       │ EU Lambda      │   │CANADA Lambda   │   │ US Lambda      │
       │ affected-      │   │affected-       │   │ affected-      │
       │ engagement-    │   │ engagement-    │   │ engagement-    │
       │ resolver       │   │ resolver       │   │ resolver       │
       └────────┬───────┘   └────────┬───────┘   └────────┬───────┘
                │                   │                  │
        INSERT outbox rows          │                  │
                │                   │                  │
                ▼                   ▼                  ▼
       ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
       │ EU Outbox Table│   │CANADA Outbox   │   │ US Outbox Table│
       └────────┬───────┘   └────────┬───────┘   └────────┬───────┘
                │                   │                  │
           POLL (polling)           │                  │
                │                   │                  │
                ▼                   ▼                  ▼
       ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
       │ EU Fargate     │   │CANADA Fargate  │   │ US Fargate     │
       │ engagement-    │   │ engagement-    │   │ engagement-    │
       │ update-        │   │ update-        │   │ update-        │
       │ processor      │   │ processor      │   │ processor      │
       └────────┬───────┘   └────────┬───────┘   └────────┬───────┘
                │                   │                  │
        LOAD engagement             │                  │
        (~1 min each)               │                  │
                │                   │                  │
                ▼                   ▼                  ▼
       ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
       │  Engagement Mgmt│   │ Engagement Mgmt│   │ Engagement Mgmt│
       │   (Existing)    │   │  (Existing)    │   │  (Existing)    │
       └────────────────┘   └────────────────┘   └────────────────┘
```

---

## Scale Analysis

### Worst Case vs. Typical Case

| Scenario | Calculation | Engagements Affected |
|----------|-------------|---------------------|
| Best Case (uniform distribution) | 800k ÷ 40 products | 20,000 per update |
| Worst Case (single product dominates) | All 800k use one template | 800,000 per update |
| Typical Assumption | Based on uniform distribution | 20,000 per update |

### Processing Time Calculation

For 20,000 engagements at ~1 minute downstream processing each:

- With 100 concurrent workers: 20,000 ÷ 100 = **200 minutes (~3.3 hours)**
- Even with 1,000 workers: 20,000 ÷ 1,000 = **20 minutes**

### Key Insight

The "within seconds" requirement **cannot be achieved for complete processing** at scale. We must distinguish:

| Latency Target | Feasible? | Implementation |
|----------------|-----------|----------------|
| Pending indicator lights up | ✓ Yes (seconds) | Index entry created immediately via Lambda |
| Summary generation starts | ✓ Yes (seconds) | Outbox entry triggers async pipeline |
| Summary completion | ✗ No (hours at scale) | Async, user notified when ready |
| Engagement transformation | ✗ No (hours at scale) | Background processing |

---

## Cost Analysis

### Assumptions

- Cost ceiling assumption: **$5,000/month**
- Update frequency: 40 templates × 1 update/week = 160 updates/month
- Average engagements per update: 20,000 (uniform distribution)
- Downstream processing: ~1 minute per engagement
- Regional count: 3 (EU, Canada, US)

### Monthly Cost Breakdown

| Component | Monthly Cost | Calculation |
|-----------|--------------|-------------|
| Central Lambdas (indexer + resolver) | $2.50 | 160 + engagement creation events |
| Regional Lambdas (resolver) | $2.00 | 160 × 3 regions |
| Fargate Workers | $120.00 | 20k engagements × 4 min × 160 publishes ÷ 60 × $0.04/hr |
| Index Storage (central) | $5.00 | 4k firms × 40 templates × minimal metadata |
| Index Storage (regional) | $15.00 | 800k engagements × ~100 bytes/row |
| SQS/SNS Messaging | $25.00 | 160 × 20k events |
| LLM Summaries | $20.00 | 20k × $0.001 |
| Reconciliation Jobs | $30.00 | Weekly 800k scan × 4 weeks |
| **Total** | **~$219.50/month** | Well under $5,000 budget |

Even at 5× typical load, total cost remains under $1,100/month.

---

## Reliability & Idempotency

### Event Handling

Events will be dropped, duplicated, and arrive out of order. Mitigation strategies:

| Issue | Mitigation |
|-------|------------|
| Duplicate events | Idempotency keys on fan-out tasks (check status before processing); unique constraint on `(engagement_id, template_version)` |
| Lost events | **Daily reconciliation job per region:** Detect template updates in last 24h, query central index for affected firms, verify outbox table entries exist, inject corrective entries for missing ones |
| Lost Lambda invocations | **Hourly reconciliation job per region:** Check all records without `SUMMARY_DONE`, validate timeout threshold (>15 min), relaunch process |
| Out-of-order events | Version sequence numbers to handle ordering (ignore older version updates) |
| Failed Lambda dispatch | CloudWatch alarms trigger manual retry; partial cross-region success is acceptable (eventual consistency via reconciliation) |

### Dead-Letter Queue (DLQ)

Failed processing moves to DLQ after 3 retries. Manual triage required for:
- Engagement load failures (data corruption?)
- Downstream service errors (capacity issues?)
- Timeout exceeded (worker stuck?)

---

## Data Residency Considerations

### Approach

Storage is partitioned by region (EU, Canada, US). This avoids security and compliance issues by keeping engagement metadata physically close to where the firm operates.

| Storage | Location | Contents |
|---------|----------|----------|
| Central Index | Multi-region (read replicas) | `(template_id, firm_id)` — no engagement details, no region |
| Firm Registry | Multi-region (read replicas) | `(firm_id, region)` — routing metadata only |
| Regional Index | Matching firm region | `(engagement_id, template_id, version, status)` |
| Outbox Tables | Matching firm region | Same as regional index plus timestamps |

Because region lives only in the Firm Registry, a firm relocating is a single-row change plus a controlled migration of its regional index rows — not a rewrite of the central index.

### Template Withdrawal

- When template is marked `WITHDRAWN`, existing pending updates transition to `EXPIRED`
- Previously `APPLIED` engagements retain original version data (audit trail preserved)
- Users see "This update was withdrawn" instead of the summary

---

## Tradeoffs, Assumptions, and Challenged Requirements

### Challenged Requirement

> **"Real-time (<seconds) availability for all affected engagement files when a template is published"**

**Why it cannot be met end-to-end:** Applying an update requires loading each engagement, a ~1 minute operation treated as a hard constraint and owned by another team with limited capacity. For a product used by 20,000 engagements, even at 100-way parallelism the fan-out takes ~3.3 hours; the worst case (a product used by all 800,000) exceeds a week. Parallelism cannot close this gap.

**Proposed Decomposition:** Split the requirement into two signals.

| Signal | Description | Timing |
|--------|-------------|--------|
| Pending-update indicator | Derived from the index alone, no engagement load required. Available within seconds of publish, satisfying the real-time requirement as users experience it. | Seconds |
| Update application | Asynchronous, completing in hours at scale. Users are notified on completion. | Hours |

**UX Consequence:** Phase 1 shows "Template X has been updated — review required." Phase 2 delivers the summary and the accept/decline action. Users are never blocked waiting on fan-out.

### Riskiest Component

**Central Template-Firm Index** is a single point of failure for routing correctness. Corruption or lag affects all downstream processing.

**Mitigations:**
- Daily checksums to detect drift
- Weekly full reconciliation
- Read replicas for availability during failover

### What We Left Out

| Excluded | Reason |
|----------|--------|
| Auto-application of changes | Explicitly out of scope per assignment |
| Real-time engagement transformation | Bounded by 1-minute downstream constraint; async is the only viable approach |
| Cross-region engagement migration | Firms rarely move regions; handled as edge case via reconciliation |
| Full engagement content replication | Violates data residency; only metadata indexed |
| Complex version merge semantics | Assignment states applying changes is out of scope; tracking pending updates is sufficient |

### Assumptions Made

1. Event delivery from existing systems can be augmented (hooks added to engagement creation/deletion)
2. Central Index can be queried at scale without impacting existing template operations
3. LLM-generated summaries require no human review for minor template changes
4. Firms operate from a single primary region (engagement mobility is rare)

---

*Date: November 2, 2026*
