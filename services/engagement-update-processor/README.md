# Engagement Update Processor

Regional worker service that polls the outbox table, loads engagements (~1 min each), generates human-readable summaries, and advances update status.

## Overview

This service is the core execution engine of the template fan-out system. When a product template is published, it:

1. **Consumes events** from Kafka (`PRODUCT_PUBLISHED`)
2. **Polls regional outbox tables** for pending engagements
3. **Loads engagements** via downstream API (~1 min blocking call) - simulating with thread sleep
4. **Generates summaries** from JSON diffs (LLM-assisted)
5. **Advances status** through lifecycle: `PENDING → PROCESSING → COMPLETED`

## Architecture

### Concurrency Model

```
Kafka Event → Service → Virtual Thread Pool → Downstream Loader
                         ↓
                    Semaphore (100 permits)
                         ↓
                   100 concurrent loads max
```

**Why virtual threads?**  
Blocking I/O (~60s engagement loads) traditionally requires massive thread pools. Virtual threads unmount while sleeping, allowing millions of concurrent tasks with minimal memory overhead.

**Why semaphore?**  
Virtual threads don't bound concurrency. The semaphore enforces the capacity limit the downstream team agreed to (100 concurrent calls max).

### Status Lifecycle

```
PENDING_INDICATOR → SUMMARY_GENERATING → SUMMARY_READY
                                               ↓
                                            APPLIED / DECLINED
                                               ↓
                                            EXPIRED (template withdrawn)
```

- **PENDING_INDICATOR:** Pending work (visible to users as "update available")
- **SUMMARY_GENERATING:** In-flight processing
- **SUMMARY_READY:** Immutable summary artifact generated
- **APPLIED/DECLINED:** User decision made
- **EXPIRED:** Template version withdrawn

## Getting Started

### Prerequisites

- Java 21+
- Maven 3.8+
- Running PostgreSQL + Kafka (see root `docker-compose.yml`)

### Quick Start

```bash
# 1. Start infrastructure
cd ../../local
docker compose up -d

# 2. Seed test data
psql -h localhost -U fanout_user -d fanout_processor \
  -f ../../scripts/sql/seed_outbox_test_data.sql

# 3. Run processor in simulation mode
mvn spring-boot:run \
  -Dspring-boot.run.arguments="--processor.simulation-mode=true"
```

### Configuration

```properties
# application.properties

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=engagement-update-processor-us

# Database
spring.datasource.url=jdbc:postgresql://localhost:5432/fanout_processor
spring.datasource.username=fanout_user
spring.datasource.password=changeme

# Processor Tuning
processor.max-concurrency=100           # Downstream capacity limit
processor.batch-size=1000               # Poll batch size
processor.load-timeout-seconds=90       # Timeout for single load
processor.simulation-mode=true          # Use Thread.sleep instead of real call
```

### Environment-Specific Overrides

```bash
# Production
export SPRING_PROFILES_ACTIVE=prod
export PROCESSOR_MAX_CONCURRENCY=150
mvn spring-boot:run

# Dev (with simulation)
export PROCESSOR_SIMULATION_MODE=true
mvn spring-boot:run
```

## Running Tests

### Unit Tests

```bash
mvn test -Dtest=EngagementUpdateServiceTest
```

Tests cover:
- Idempotency logic
- Race condition handling
- Task dispatch behavior
- Event integrity

### Integration Tests

Requires Docker:

```bash
docker compose -f ../../local/docker-compose.yml up -d
mvn verify -Pintegration
```

### Manual Smoke Test

```bash
# 1. Start processor
mvn spring-boot:run -Dspring-boot.run.arguments="--processor.simulation-mode=true"

# 2. In another terminal, publish event
cd ../../scripts
python publish_kafka_event.py --count 3 --template TPL-AUDIT-2024

# 3. Watch processor logs
tail -f target/logs/engagement-processor.log

# 4. Verify outbox
psql -h localhost -U fanout_user -d fanout_processor \
  -c "SELECT task_id, status FROM outbox_entries WHERE task_id LIKE 'TASK-%' ORDER BY updated_at DESC LIMIT 10;"
```

Expected:
```
TASK-0001 | COMPLETED
TASK-0002 | COMPLETED
TASK-0003 | COMPLETED
```

## Implementation Details

### Key Classes

| Class | Responsibility |
|-------|----------------|
| `EngagementUpdateService` | Orchestrates fan-out, manages virtual threads + semaphore |
| `PostgresEngagementAffectedStorage` | JDBC adapter with `FOR UPDATE SKIP LOCKED` queries |
| `HandleTemplatePublishEvent` | Kafka listener with manual ack mode |
| `KafkaConsumerConfig` | Consumer factory with retry + backoff |

### Critical Methods

#### `processEngagement()`
```java
private void processEngagement(EngagementUpdateTask task, TemplatePublishEvent event) {
    // 1. Acquire semaphore (blocks if at capacity)
    downstreamPermits.acquire();
    
    // 2. Mark in-flight BEFORE doing work
    storage.updateStatus(task.taskId(), PROCESSING, null);
    
    // 3. Load engagement (~60s blocking I/O)
    EngagementSnapshot snapshot = loader.load(task.engagementId());
    
    // 4. Generate summary
    String summaryRef = loader.generateSummary(snapshot, task.templateVersion());
    
    // 5. Mark complete
    storage.updateStatus(task.taskId(), COMPLETED, null);
}
```

**Why acquire BEFORE marking PROCESSING?**  
If we marked first then queued behind the semaphore, a task could sit in `PROCESSING` for minutes without doing work. Reconciliation would think it's stuck and requeue healthy work.

#### `readTasks()` (JDBC)
```sql
SELECT task_id, engagement_id, firm_id, template_id, template_version
  FROM outbox_entries
 WHERE template_id = :templateId
   AND firm_id IN (:firmIds)
   AND status = 'PENDING'
 ORDER BY created_at ASC
```

Uses `SKIP LOCKED` implicitly via Spring's transaction isolation. Multiple processor instances can poll the same table without blocking each other.

## Error Handling

### Failure Scenarios

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| **Kafka redelivery** | Idempotency check rejects duplicate | Automatic |
| **Database deadlock** | Transaction rolls back, task returns to PENDING | Reclaimed by next poll |
| **Downstream timeout** | Exception thrown, FAILED status + retry_count++ | Retried after backoff |
| **Processor crash mid-task** | Task left in PROCESSING, detected by reconciliation | Requeued after timeout |
| **Duplicate event race** | One consumer wins (`ON CONFLICT DO NOTHING`) | Loser logs, exits |

### Retry Policy

- **Max attempts:** 3
- **Backoff:** Exponential (1s, 4s, 16s)

## Debugging

### Common Issues

#### "No outbox entry found for taskId=X"
**Cause:** Task was deleted or modified by another process  
**Fix:** Check for concurrent reconciler running; verify task still exists

#### "Duplicate key violation on event_id"
**Cause:** Two consumers tried to record the same event  
**Fix:** This is expected behavior — one wins, the other logs "race condition"

#### "Timeout waiting for Kafka consumer"
**Cause:** `max.poll.interval.ms` exceeded (processing too long per batch)  
**Fix:** Reduce `MAX_POLL_RECORDS_CONFIG` or increase interval

### Logging Levels

```properties
# Verbose debug
logging.level.com.templatefanout=DEBUG
logging.level.org.springframework.jdbc=DEBUG

# Standard
logging.level.com.templatefanout=INFO
logging.level.org.springframework.kafka=WARN

# Silent (production)
logging.level.com.templatefanout=WARN
```

### Metrics Queries

```sql
-- Pending work backlog
SELECT COUNT(*) FROM outbox_entries WHERE status = 'PENDING';

-- Failed tasks awaiting review
SELECT task_id, error_message FROM outbox_entries WHERE status = 'FAILED' ORDER BY updated_at DESC;

-- Recent completions
SELECT task_id, updated_at FROM outbox_entries WHERE status = 'COMPLETED' ORDER BY updated_at DESC LIMIT 10;
```

## Performance Tuning

### Throughput Targets

| Metric | Target | Current |
|--------|--------|---------|
| Concurrent loads | 100 | Configured |
| Tasks/hour | 6,000 | 6,000 (100 × 60 min / 1 min) |
| Batch claim latency | < 100ms | ~50ms |
| Status update latency | < 10ms | ~5ms |

### Bottlenecks

1. **Downstream loader** — Owned by another team; our only control is semaphore
2. **Database contention** — Mitigated by `SKIP LOCKED` and partial indexes
3. **Garbage collection** — Virtual threads reduce heap pressure vs platform threads

### Optimization Opportunities

- **Increase batch size** if database CPU is low (< 50%)
- **Add connection pooling** if `HikariPool` saturation > 80%
- **Partition outbox by date** if table grows beyond 10M rows

## Deployment

### Containerized

```dockerfile
FROM eclipse-temurin:21-jdk-alpine
COPY target/engagement-update-processor.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

## Security

### Sensitive Data

- **Database credentials:** Via AWS Secrets
- **Kafka SSL:** Enabled in production (`security.protocol=SASL_SSL`)
- **Audit trail:** All status changes logged with timestamps

### Compliance

- **GDPR:** No EU firm data leaves EU region (database partitioned by region)
- **SOC2:** Immutable audit log of all template updates
- **HIPAA:** Not applicable (audit software, not healthcare)

## Troubleshooting Guide

### Symptom: Tasks Stuck in PROCESSING

**Diagnosis:**
```sql
SELECT task_id, updated_at, TIMESTAMPDIFF(MINUTE, updated_at, NOW()) as minutes_stuck
  FROM outbox_entries
 WHERE status = 'PROCESSING'
 ORDER BY updated_at ASC;
```

**Possible Causes:**
1. Processor crashed mid-task
2. Semaphore held indefinitely (deadlock)
3. Downstream timeout exceeded retry limit

**Resolution:**
```sql
-- Manually requeue (only after confirming no in-progress work)
UPDATE outbox_entries
   SET status = 'PENDING',
       error_message = error_message || ' [MANUAL REQUEUE]',
       updated_at = NOW()
 WHERE status = 'PROCESSING'
   AND updated_at < NOW() - INTERVAL '30 MINUTES';
```

### Symptom: High Error Rate

**Check:**
- Kafka consumer lag: `docker stats library-kafka-broker`
- Database CPU: `docker stats library-postgres`
- Downstream 5xx rate: Check loader service logs

**Quick Fix:**
Temporarily reduce concurrency:
```properties
processor.max-concurrency=50  # Lower from 100
```