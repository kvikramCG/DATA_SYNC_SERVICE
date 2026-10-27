# Data Sync Service - Solution Architecture

Version: 1.0

---

# 1. Executive Summary

This document describes the architecture for the Orion → Data Sync Service → LedgerPoint synchronization platform.

The objective is to build a reliable, scalable, observable, and maintainable synchronization service that:

- Fetches time-series data from Orion.
- Applies transformation and validation rules.
- Handles timezone and DST complexities.
- Detects changes against previously synced values.
- Publishes only necessary data to LedgerPoint.
- Tracks execution history through Jobs and Tasks.
- Supports retries, recovery, auditing and replay.
- Operates safely within source and target system limits.

This architecture is intentionally designed as a Modular Monolith for the POC while remaining evolution-ready for future microservice adoption.

---

# 2. Problem Understanding

The synchronization process is not a simple ETL pipeline.

The system must:

- Trigger sync jobs.
- Create task-level execution tracking.
- Handle monthly chunked data retrieval.
- Apply domain-specific transformations.
- Detect deltas.
- Respect external system limits.
- Handle daylight-saving transitions.
- Publish selectively.
- Persist synchronization state.
- Support re-runnable execution.

Most importantly:

The platform sits between two systems that we do not control.

```text
Orion
   |
   V

Data Sync Service

   |
   V

LedgerPoint
```

Therefore the architecture must absorb instability from both ends.

---

# 3. Architectural Drivers

## Functional Requirements

- Sync typeA and typeB series.
- Support complete and partial sync windows.
- Support scheduler-driven execution.
- Support manual execution.
- Support month-wise retrieval.
- Support DST handling.
- Support change detection.
- Support retries.
- Support reruns without duplication.
- Job and task visibility.

---

## Non-Functional Requirements

### Reliability

No successful synchronization data should be lost.

---

### Scalability

Must scale across:

- growing number of series
- growing sync frequency
- growing historical volumes

---

### Auditability

Must answer:

```text
What was synced?
When was it synced?
Why did it fail?
Which values changed?
```

---

### Observability

Operations team must know:

```text
Job Progress

Task Failures

Rate Limit Issues

Backlogs

Target Failures
```

---

# 4. Key Constraints From External Systems

## Orion Constraints

### Authentication

Bearer token valid for:

```text
60 minutes
```

---

### Rate Limit

```text
5 requests / second
```

This is a critical architecture constraint.

No amount of horizontal scaling can exceed Orion throughput.

This is one of the primary reasons we are not introducing distributed fetch microservices.

The bottleneck already exists externally.

---

### Date Format

```text
M/d/yyyy h:mm:ss a
```

Prevailing local time.

---

### Timezone Handling

Timezone code mapping required.

---

## LedgerPoint Constraints

### API Key Authentication

---

### Maximum Batch Size

```text
10,000 records/request
```

Publisher must chunk payloads accordingly.

---

### Upsert Support

LedgerPoint uses:

```text
(key,date,interval)
```

as unique key.

This makes the target naturally idempotent.

---

# 5. Architecture Decision Record (ADR)

# ADR-001: Modular Monolith Instead Of Microservices

## Decision

The POC will be implemented as a Modular Monolith.

---

## Why Not Microservices?

Initially we considered:

```text
Fetch Service
      |
Kafka
      |
Validation Service
      |
Publisher Service
```

---

This architecture provides:

- independent service scaling
- event-driven processing
- replayability
- service isolation

However it also introduces:

- distributed tracing
- service discovery
- inter-service contracts
- deployment complexity
- operational overhead
- message serialization costs
- distributed failures

---

## Why Modular Monolith Is Better For This POC

The actual throughput bottlenecks are:

### Orion

```text
5 req/sec
```

### LedgerPoint

```text
10,000 rows/request
```

These are external bottlenecks.

Internal decomposition will not improve them.

---

Most processing is:

```text
Fetch
Transform
DST Handling
Diff
Publish
Persist
```

which is a strongly coupled business workflow.

Splitting into services would create complexity without meaningful throughput gain.

---

## Benefits Of Modular Monolith

### Simpler Delivery

Single deployment.

---

### Easier Debugging

Single process.

---

### Easier Transactions

No distributed consistency required.

---

### Faster POC Delivery

Focus remains on business logic.

---

### Lower Infrastructure Cost

No Kafka cluster.

No service mesh.

No distributed platform requirements.

---

# ADR-002: Design For Future Microservice Evolution

Choosing a monolith today does not mean rejecting microservices.

The architecture will remain modular.

Package structure:

```text
datasync-service

├── api
├── scheduler
├── job-engine
├── sync-engine
├── orion-client
├── ledgerpoint-client
├── transformation-engine
├── dst-engine
├── interval-engine
├── diff-engine
├── persistence
├── rules-engine
├── observability
└── common
```

Future extraction becomes straightforward.

---

# ADR-003: Hexagonal Architecture (Ports & Adapters) Within Each Module

## Decision

Each module in the Modular Monolith (§5) is internally structured as a hexagon: a framework-free
domain core, explicit inbound/outbound ports (interfaces), and adapters that implement those ports
against Spring, HTTP clients, and the database.

This is additive to ADR-001, not a replacement of it. ADR-001 decided *module granularity and
deployment shape*; ADR-003 decides *the internal dependency direction within and across those
modules*. The two questions are independent — choosing a modular monolith does not by itself
answer how modules should depend on infrastructure.

## The Debate

**Arguments for hexagonal:**

- The hardest, most failure-prone logic in this system — DST fall-back/spring-forward handling,
  interval assignment, unit mapping, rule overrides, change detection — is pure business logic with
  no inherent dependency on Spring, HTTP, or SQL. Isolating it behind ports makes it unit-testable
  with plain JUnit, no containers, no WireMock, no Spring context — fast feedback for the riskiest
  code in the system.
- The acceptance criteria (spec §11) are built around mocking Orion and LedgerPoint (WireMock /
  MockWebServer). Hexagonal ports formalize exactly that seam: `SeriesSourcePort` and
  `SeriesPublisherPort` are what those tests already assume exists.
- ADR-002 already commits to "design for future microservice evolution". Hexagonal ports are what
  make that credible: extracting `orion-client` into its own service later is a matter of writing a
  new adapter that still implements `SeriesSourcePort` — the orchestration and domain logic do not
  change.
- Low marginal cost here: the module boundaries in §5 already exist and already separate
  integration concerns (`orion-client`, `ledgerpoint-client`, `persistence`) from domain logic
  (`transformation-engine`, `dst-engine`, `interval-engine`, `diff-engine`). Hexagonal just turns the
  existing boundary into an enforced dependency rule instead of a naming convention.

**Arguments against (considered and rejected for this POC):**

- Extra interfaces/DTOs/mapping code for a POC team that, per ADR-001, is explicitly optimizing for
  delivery speed.
- Full hexagonal rigor (mapping structs at every boundary, zero leakage of persistence annotations
  into the domain) can become ceremony for a short-lived POC.

**Resolution:** apply hexagonal rules only where they pay for themselves — the domain services and
the two external-system integrations — and skip ceremony elsewhere (e.g. simple CRUD-shaped lookups
can use a thin Spring Data interface directly as the "port" rather than hand-rolling a wrapper for
every repository). This captures the benefit (testable domain, swappable integrations) without
maximizing boilerplate.

## Layer / Package Mapping

| Hexagonal layer | Existing §5 packages | Responsibility |
|---|---|---|
| Domain core (framework-free) | `transformation-engine`, `dst-engine`, `interval-engine`, `diff-engine`, plus a new `domain` package (Job/Task/Series/SeriesValue models, enums) | DST rules, interval math, unit mapping, diff classification, rule resolution. No Spring/JPA/HTTP imports. |
| Inbound ports | new `domain.port.in` | `TriggerSyncUseCase`, `GetJobUseCase` |
| Inbound adapters | `api`, `scheduler` | REST controllers/DTOs; future cron trigger |
| Application / orchestration | `job-engine`, `sync-engine` | Implements inbound ports; coordinates outbound ports per series Task; owns per-series parallelism (ADR-004) |
| Outbound ports | new `domain.port.out` | `SeriesSourcePort`, `SeriesPublisherPort`, `JobRepositoryPort`, `SeriesRepositoryPort`, `SeriesValuePort`, `RuleOverrideRepositoryPort`, `ClockPort` |
| Outbound adapters | `orion-client`, `ledgerpoint-client`, `persistence`, `rules-engine` | Token cache + rate limiter + retry (Orion); batching + retry (LedgerPoint); JPA entities + Spring Data + mapping (persistence) |
| Cross-cutting | `observability`, `common` | Metrics, correlation IDs, shared exceptions/enums |

## Enforcement

An ArchUnit dependency-rule test asserting that the `domain` package never imports from
`adapter.*` or `org.springframework.*` is planned for the first implementation slice. Not written in
this documentation-only pass, but recorded here so it is not forgotten.

---

# ADR-004: Virtual Threads Instead Of A Reactive Stack For Series Parallelism

## Decision

Series-level parallelism (§17) is implemented with Java 21 virtual threads
(`Executors.newVirtualThreadPerTaskExecutor()`), one virtual thread per series Task, calling Orion
and LedgerPoint through a synchronous, blocking HTTP client (Spring `RestClient`). A single shared
Resilience4j `RateLimiter` (5 permits/sec) and `Retry` (3 attempts, exponential backoff from 300 ms,
limited to 429/5xx/timeouts per §8) guard every outbound call regardless of which virtual thread
issues it.

## Why Not Reactive (WebClient / Project Reactor)

A reactive stack would also solve high-concurrency I/O, but at a cost that is not justified here:

- The actual throughput ceiling is external (Orion 5 req/s, LedgerPoint 10k rows/batch — §4). No
  amount of non-blocking I/O increases a rate limit enforced by someone else's server. Reactive
  buys scale headroom this system is explicitly not allowed to use.
- Reactive code is harder to debug and test, and the team's stated priority (ADR-001) is POC
  delivery speed and simplicity.
- Virtual threads give "one logical thread per series Task" — a direct, readable mapping to the
  domain concept of a Task — while still scaling to hundreds/thousands of concurrent blocking calls
  cheaply, which matches this workload's actual shape (many independent, mostly I/O-bound series
  Tasks, globally rate-limited).

## Consequence

The shared rate limiter is a single, testable, framework-agnostic component (an outbound-adapter
concern, not domain logic) that every series Task's Orion calls must acquire a permit from —
enforced in code, not only as architectural intent.

---

# 6. High Level Architecture

```text
             REST Caller
                  |
                  V

           Sync Controller
                  |
                  V

             Job Manager
                  |
                  V

        Parallel Series Workers
                  |
                  V

           Orion Integration
                  |
                  V

       Transformation Pipeline
                  |
                  V

          Delta Detection
                  |
                  V

          LedgerPoint Client
                  |
                  V

             Persistence
                  |
                  V

             TimescaleDB
```

---

# 7. Processing Flow

For every Series:

```text
Resolve Window

Chunk By Month

Fetch Orion Data

Parse Dates

Convert To UTC

Apply DST Rules

Assign Intervals

Map Units

Apply Rules

Detect Changes

Prepare Payload

Publish To LedgerPoint

Persist Successful Results

Update Task Status
```

---

# 8. Job Management Model

## Job

Represents one sync execution.

Example:

```text
Sync all typeA series
```

---

States:

```text
RUNNING

COMPLETED

COMPLETED_WITH_FAILURES

FAILED
```

---

## Task

Represents one series within a job.

States:

```text
PENDING

SUCCESS

SUCCESS_WITH_WARNINGS

FAILED

SKIPPED
```

---

Isolation Principle:

Failure of one Task must never stop the Job.

---

# 9. Data Storage Strategy

## Why TimescaleDB

The central domain entity is:

```text
Series Value
```

which is time-series data.

Typical queries:

```text
Last Synced Data

Value History

Diff Detection

Reprocessing

Auditing
```

TimescaleDB provides:

### Hypertables

Efficient partitioning by time.

---

### Compression

Historical data storage optimization.

---

### Time-Based Query Efficiency

Important for future growth.

---

### Scope For The POC

TimescaleDB is used as a PostgreSQL extension, not a separate engine — so there is no extra SQL
dialect, JPA, or driver cost versus plain PostgreSQL, and Testcontainers has a ready
`timescale/timescaledb` image for integration tests. For the POC, exactly one hypertable is
created — `series_value`, partitioned on `start_instant` with a monthly chunk interval. Compression
policies and continuous aggregates are deliberately **not** configured yet; they are recorded here
as future work once real retention/query patterns are observed, rather than tuned speculatively.

---

# 10. Persistence Model

Tables:

```text
job

job_task

job_task_chunk

series

series_value

series_rule_override

task_warning
```

Full column-level definitions, keys, and indexes are in `database-design.md`.

Two tables originally suggested here have been deliberately dropped, with reasoning:

- **`sync_checkpoint`** — merged into `series.last_synced_date`. A separate checkpoint table would
  duplicate the same high-watermark already required on `series`; the new `job_task_chunk` table
  (below) covers finer-grained, per-month resumability instead.
- **`job_execution_log`** — replaced by structured JSON logs carrying `jobId`, `seriesName`, and
  `X-Correlation-Id` (already required by §8 Logging). Writing a DB row per log line would add write
  amplification without adding information the observability stack doesn't already capture.

One table was added beyond the original list:

- **`job_task_chunk`** — one row per series per month-chunk processed within a Task, recording
  Orion call count, fetch/transform/publish/persist status, and value counts. This makes acceptance
  criterion #2 ("3-month window → exactly 3 Orion calls") directly queryable, and gives a job a
  resume point if it fails partway through a multi-month series instead of needing to re-fetch
  already-succeeded months.

---

# 11. Checkpoint Strategy

The service never blindly re-fetches data.

Two approaches:

### Explicit Window

Caller provides:

```text
startDate
endDate
```

---

### Computed Window

If omitted:

```text
start = last successful sync

end = latestDataDate
```

---

For first run:

```text
today - 1 year
```

as per specification.

---

# 12. Change Detection Strategy

Comparison key:

```text
series
+
startInstant
+
repeatedHour
```

Classification:

```text
NEW

UPDATED

UNCHANGED
```

Only:

```text
NEW
UPDATED
```

are published.

---

Benefits:

- Reduced target traffic
- Faster jobs
- Natural idempotency

---

# 13. Daylight Saving Time Engine

This is one of the most critical business rules.

Dedicated module:

```text
DstAdjustmentService
```

---

## Spring Forward

23-hour day.

Missing hour must be filled.

Example:

```text
02:00 missing

copy prior interval value
```

DECISION (option B): the copied ("filler") row is derived on every run and is never stored in
`series_value` - its `start_instant` equals the next real interval's, which is its primary key.
`ChangeDetector` gives a filler the classification of the interval it copies, so it is sent to
LedgerPoint only when that interval is new or updated (a re-run of the day sends nothing). Fillers
count in `valuesSent` but not in the fetched/new/updated/unchanged chunk counts.

---

## Fall Back

25-hour day.

Repeated hour.

Example:

```text
2

2x
```

Both values preserved.

---

# 14. Orion Client Design

Responsibilities:

- Token acquisition
- Token refresh
- Request throttling
- Retry handling

---

## Token Strategy

Cached token.

Refresh upon:

```text
Expiration

OR

401 response
```

Retry original request once.

---

## Rate Limiter

Global process limiter:

```text
5 requests/sec
```

No worker can bypass this.

---

# 15. LedgerPoint Publisher Design

Responsibilities:

- Build payloads
- Batch payloads
- Retry failures
- Handle target limits

---

## Batch Size

Maximum:

```text
10,000 rows
```

Large datasets split automatically.

---

## Idempotency

LedgerPoint already supports:

```text
(key,date,interval)
```

upserts.

This makes repeated payloads safe.

---

# 16. Failure Handling

## Orion 429

Action:

```text
Read Retry-After

Pause

Retry
```

---

## Orion 5xx

Retry.

---

## Network Timeout

Retry.

---

## Orion 401

Refresh token.

Retry once.

---

## Unsupported Timezone

Task FAILED.

---

## Unsupported Granularity

Task FAILED.

---

## Stale Series

Task FAILED.

No publishing attempted.

---

## LedgerPoint 400

Task FAILED.

No retries.

---

## LedgerPoint 409

Task FAILED.

Unknown series.

No retries.

---

## LedgerPoint 5xx

Retry.

Exponential backoff.

---

# 17. Scaling Strategy

## Important Observation

Scaling is constrained by Orion.

Theoretical example:

```text
50 application instances
```

still cannot exceed:

```text
5 requests/sec
```

towards Orion.

---

Therefore scaling focus is:

### Series-Level Parallelism

Multiple series processed concurrently. See ADR-004 for the concrete Java 21 virtual-thread
implementation.

---

### Controlled Throughput

Rate limiter guarantees compliance.

---

### Horizontal Deployment

Multiple ECS tasks may be deployed.

However distributed locking must prevent duplicate job execution.

---

# Concurrency & Job Locking

Rather than an external lock service (Redis/ZooKeeper), duplicate job execution for the same
`seriesType` is prevented at the database level:

```text
CREATE UNIQUE INDEX uq_job_running_per_type
  ON job (series_type)
  WHERE status = 'RUNNING';
```

Triggering a sync always inserts the new `job` row (status `RUNNING`) as the first step. If another
job for the same `series_type` is already running, the insert raises a unique-constraint violation,
which the service maps straight to the spec's `409 — a job for this series type is already running`
(§7.1). This works unmodified across multiple application instances (e.g. several ECS tasks) because
the constraint lives in the shared database, not in process memory — no additional distributed-lock
component is required for the POC.

A Postgres advisory lock (`pg_try_advisory_lock`) is noted as a future alternative only if
concurrency control is ever needed at a finer grain than `series_type` (e.g. per-series), which the
spec does not currently require.

---

# 18. Why Database Buffering Is Not Primary In This POC

In our earlier architecture discussions we considered:

```text
Queue
 ->
Validation
 ->
Database
 ->
Publisher
```

This pattern is excellent for telemetry platforms.

---

However this specification requires:

```text
Publish

THEN

Persist successful sync state
```

because persisted values represent:

```text
Successfully synchronized records
```

not merely fetched records.

---

Therefore persistence tracks sync success rather than acting primarily as a downstream buffer.

---

# 19. Future Evolution Path

When traffic grows significantly:

Introduce Kafka.

Architecture becomes:

```text
Orion

   |
   V

Sync Service

   |
   V

Kafka

   |
   +----- Billing

   |
   +----- Analytics

   |
   +----- Forecasting

   |
   +----- Publisher

   |
   +----- Audit
```

---

# When To Move To Microservices

Move only when one or more become true:

### Different Teams Own Components

---

### Independent Scaling Needed

Example:

```text
Publisher
100x larger than fetch
```

---

### Multiple Consumers Appear

```text
Analytics

Billing

Forecasting
```

---

### Near Real-Time Requirements

Replace batch synchronization.

---

### Event Replay Becomes Mandatory

Kafka becomes highly valuable.

---

# 20. Observability

Metrics:

## Jobs

```text
Job Duration

Job Success Rate

Task Success Rate
```

---

## Orion

```text
429 Count

5xx Count

Latency
```

---

## LedgerPoint

```text
Publish Throughput

Failures

Retries
```

---

## Sync

```text
Values Read

Values Changed

Values Posted

Values Skipped
```

---

## Business

```text
Stale Series Count

Gap Count

DST Adjustments
```

---

# 21. AWS Deployment

```text
API Gateway

ECS Fargate

Spring Boot

TimescaleDB
(PostgreSQL)

Secrets Manager

CloudWatch

OpenTelemetry

Grafana

Prometheus
```

---

# 22. Final Recommendation

For the POC we deliberately choose:

✅ Modular Monolith

✅ Hexagonal Ports & Adapters Per Module

✅ TimescaleDB (Minimally Scoped)

✅ Job/Task Execution Model

✅ Series-Level Parallel Processing With Virtual Threads

✅ Orion Global Rate Limiting

✅ LedgerPoint Batch Publishing

✅ DST-Aware Processing

✅ Change Detection

✅ DB-Level Job Locking (Unique Partial Index)

✅ Strong Observability

✅ Future Kafka Evolution Path

This architecture minimizes complexity today while preserving a clear path toward event-driven microservices tomorrow. It optimizes for delivery speed, correctness, maintainability, and operational simplicity while remaining fully aligned with the actual constraints imposed by Orion and LedgerPoint.