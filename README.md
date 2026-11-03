# Template Fan-Out System

A distributed system for tracking product template updates across thousands of engagement files with regional data residency compliance.

## Overview

Enterprise's audit and assurance professionals work in **Engagement Files** that are created from **Product Templates**. When templates are updated, affected engagements need to be notified, reviewed, and potentially updated.

This system solves:
- **Scale:** 800,000+ engagement files across 4,000+ firms
- **Performance:** Real-time pending indicators (seconds) despite ~1 min engagement load times
- **Compliance:** Data residency requirements (EU/Canada/US) with regional partitioning
- **Reliability:** Eventual consistency with idempotency guarantees and reconciliation

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         TEMPLATE FAN-OUT                         │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────────┐     PRODUCT_UPDATED      ┌──────────────────────┐
│ Engagement-Indexer   │ ───────────────────────► │ Template-Publish-    │
│ (Central Lambda)     │                          │ Resolver (Central)   │
└──────────┬───────────┘                          └──────────┬───────────┘
           │ ENGAGEMENT_CREATED                               │ QUERY
           ▼                                                  ▼
┌──────────────────────┐                            ┌──────────────────────┐
│ Central Index        │                            │ Firm Registry        │
│ (template_id,        │                            │ (firm_id → region)   │
│  firm_id)            │                            └──────────┬───────────┘
└──────────────────────┘                                       │
                                                               │ RESOLVE
                                                               ▼
                                              ┌────────────────────────────┐
                                              │ Affected-Engagement-       │
                                              │ Resolver (Regional Lambdas)│
                                              └──────────┬─────────────────┘
                                                         │ POPULATE
                                                         ▼
                                              ┌────────────────────────────┐
                                              │ Regional Outbox Tables     │
                                              │ (EU / CANADA / US)         │
                                              └──────────┬─────────────────┘
                                                         │ POLL
                                                         ▼
                                              ┌────────────────────────────┐
                                              │ Engagement-Update-         │
                                              │ Processor (Fargate)        │
                                              └────────────────────────────┘
```

### Data Flow

1. **Engagement Creation:** `engagement-indexer` listens to events, populates central index
2. **Template Publish:** `template-publish-resolver` queries index for affected firms
3. **Region Routing:** Firms mapped to regions via Firm Registry
4. **Outbox Population:** Regional `affected-engagement-resolver` creates outbox entries
5. **Processing:** Regional `engagement-update-processor` polls outbox, loads engagements, generates summaries
6. **Completion:** Status advances through `PENDING → PROCESSING → COMPLETED`

### Key Patterns

| Pattern | Why |
|---------|-----|
| **Outbox Table** | Decouples event publication from processing; durability guarantees |
| **Virtual Threads** | One thread per engagement task; cheap blocking I/O simulation |
| **Semaphore Backpressure** | Bounds concurrent calls to downstream loader with limited capacity |
| **`FOR UPDATE SKIP LOCKED`** | Multiple processor instances can work without blocking |
| **Idempotent Inserts** | `ON CONFLICT DO NOTHING` handles duplicate Kafka events |
| **Partial Indexes** | `WHERE status = 'PENDING'` keeps indexes small and fast |

## Getting Started

### Prerequisites

- Docker Desktop (Kafka + PostgreSQL)
- Java 21+ (for services)
- Python 3.9+ (for test scripts)
- Maven 3.8+ (for building)

### Quick Start

```bash
# 1. Start infrastructure
cd local
docker compose up -d

# 2. Wait for services to be healthy
docker compose ps  # Should show all GREEN

# 3. Seed test data into outbox
psql -h localhost -U fanout_user -d fanout_processor -f ../scripts/sql/seed_outbox_test_data.sql

# 4. Run the processor (simulation mode)
cd ../services/engagement-update-processor
mvn spring-boot:run -Dspring-boot.run.arguments="--processor.simulation-mode=true"

# 5. Watch logs
docker compose logs -f engagement-update-processor
```

### Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `scripts/publish_kafka_event.py` | Send test events to Kafka | `python publish_kafka_event.py --count 5 --template TPL-AUDIT-2024` |
| `scripts/sql/seed_outbox_test_data.sql` | Populate outbox for testing | Run once after migrations |

## Project Structure

```
template-fanout/
├── local/
│   ├── docker-compose.yaml          # Kafka + PostgreSQL
│   └── README.md                     # Local environment setup
├── services/
│   ├── engagement-update-processor/  # Core worker service-migrations flyway are here (for future versions we need to move that because could be shared for different components).
│   ├── affected-engagement-resolver/ # Regional fan-out
│   └── engagement-indexer/           # Central index builder
├── scripts/
│   ├── publish_kafka_event.py        # Test event publisher
│   └── sql/
│       ├── seed_outbox_test_data.sql
├── reconciliation/                    # Drift detection jobs
└── README.md                         # This file
```

## Testing

### Unit Tests

```bash
cd services/engagement-update-processor
mvn test -Dtest=EngagementUpdateServiceTest
```

### Integration Tests

Requires Kafka + PostgreSQL running:

```bash
cd local
docker compose up -d

cd ../services/engagement-update-processor
mvn verify -Pintegration
```

### Manual End-to-End

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Run processor in simulation mode
cd services/engagement-update-processor
mvn spring-boot:run -Dspring-boot.run.arguments="--processor.simulation-mode=true"

# 3. Publish an event
python ../../scripts/publish_kafka_event.py --count 1 --template TPL-AUDIT-2024

# 4. Check outbox
psql -h localhost -U fanout_user -d fanout_processor -c \
  "SELECT task_id, status, retry_count FROM outbox_entries ORDER BY created_at DESC LIMIT 5;"
```

## Configuration

### Environment Variables

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=engagement-update-processor-{REGION}

# Database
spring.datasource.url=jdbc:postgresql://localhost:5432/fanout_processor
spring.datasource.username=fanout_user
spring.datasource.password=changeme

# Processor
processor.max-concurrency=100
processor.batch-size=1000
processor.simulation-mode=false
processor.load-timeout-seconds=90
```

### Simulation Mode

as I am simulating Engagement response and summary response I add a Thread sleep in this part.

## Troubleshooting

### Kafka Connection Fails

```bash
# Check if Kafka is healthy
docker compose ps kafka
docker compose logs kafka

# Restart if needed
docker compose restart kafka
```

### Processor Not Processing

1. Check logs: `docker compose logs engagement-update-processor`
2. Verify outbox has PENDING rows: `psql -c "SELECT COUNT(*) FROM outbox_entries WHERE status = 'PENDING';"`
3. Check semaphore permits: `grep "downstreamPermits" logs/*.log`

### Migration Errors

```bash
# Check Flyway history
psql -h localhost -U fanout_user -d fanout_processor -c \
  "SELECT version, description, installed_on FROM flyway_schema_history ORDER BY installed_on DESC;"

# Roll back one migration
docker exec -it library-postgres psql -U fanout_user -d fanout_processor -c \
  "DELETE FROM flyway_schema_history WHERE version = '3'; DROP TABLE IF EXISTS outbox_entries CASCADE;"
```

## Design Decisions

- Virtual threads vs platform threads
- Semaphore vs synchronized for backpressure
- `FOR UPDATE SKIP LOCKED` for concurrent processing
- Partial indexes for pending-only queries
- JSONB vs relational storage for `affected_firms`
- Idempotency patterns (event ID vs optimistic locking)

## License

Proprietary — Internal use only
