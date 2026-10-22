# Plan: Finalize Hexagonal Modular-Monolith Architecture + DB Design for Data Sync Service

## Context / Answers from user
- Hexagonal scope: user wants architect's recommendation
- DB engine: user wants architect's recommendation
- Stack: Java 21 + Spring Boot 3.x + Maven (single module)
- Deliverable NOW: architecture.md finalized (new ADRs) + new database-design.md. No code/scaffolding yet.
- Concurrency lock: DB-based (unique partial index / advisory lock) - user confirmed DB approach

## Recommendation 1: Hexagonal WITHIN the modular monolith (not instead of it)
Modular monolith (ADR-001, deployment/module granularity) and hexagonal (ports & adapters,
dependency-direction pattern) are orthogonal and complementary - not alternatives.
Map existing package list to hexagonal layers:
- Domain core (framework-free, pure Java, immutable records): domain model (Job, Task, Series,
  SeriesValue, Interval, ChangeType, FailureReason), transformation-engine, dst-engine,
  interval-engine, diff-engine, rules-resolution logic.
- Inbound ports (use-case interfaces): TriggerSyncUseCase, GetJobUseCase.
- Inbound adapters: api (REST controllers, DTOs, RFC7807 error mapping), future scheduler.
- Outbound ports (SPI interfaces): SeriesSourcePort, SeriesPublisherPort, JobRepositoryPort,
  SeriesRepositoryPort, SeriesValuePort, RuleOverrideRepositoryPort, ClockPort.
- Outbound adapters: orion-client (implements SeriesSourcePort: token cache, rate limiter, retry),
  ledgerpoint-client (implements SeriesPublisherPort: batching, retry), persistence (JPA entities +
  Spring Data repos + adapter classes implementing *RepositoryPort).
- Application/orchestration layer (job-engine + sync-engine): JobOrchestrationService implements
  the inbound ports, coordinates outbound ports per series task.
Why: keeps business rules (DST, interval, diff, rules) 100% unit-testable without Spring/DB/HTTP;
preserves ADR-002 future microservice extraction (adapters are already isolated behind ports -
extracting orion-client later is a transport swap, not a domain rewrite); pairs naturally with
spec's acceptance criteria which mock Orion/LedgerPoint (WireMock = swap outbound adapter).
Enforcement: add an ArchUnit test (documented as a future test, not written now) asserting domain
package has zero dependencies on adapter/Spring packages.

## Recommendation 2: Keep TimescaleDB, scoped minimally for POC
TimescaleDB is Postgres + extension, so zero extra JPA/SQL dialect cost vs plain Postgres, and
Testcontainers has a ready `timescale/timescaledb` image. series_value is the one genuinely
time-series-shaped table (growing across years x up to 96 intervals/day x N series), so hypertable
partitioning is the right fit even at POC scale, and it avoids a late, risky migration if volumes
grow before stakeholders revisit the decision. Scope for POC: enable hypertable on series_value
only, default/monthly chunk_time_interval, no compression policy yet (documented as future work,
not configured now). Flyway for migrations (simpler Spring Boot default vs Liquibase).

## Recommendation 3: Java 21 virtual threads for series-level parallelism (new ADR)
Avoid reactive/WebClient complexity. Use `Executors.newVirtualThreadPerTaskExecutor()` to run one
virtual thread per series Task; blocking synchronous HTTP client (Spring RestClient) calls Orion/
LedgerPoint cheaply on virtual threads. Global Orion throughput capped by a single shared
Resilience4j RateLimiter (5 req/s) that all virtual threads acquire a permit from before calling
Orion - directly satisfies the spec's "no amount of parallelism can exceed Orion throughput" driver
without any reactive stack. Retries: Resilience4j Retry (3 attempts, exponential backoff 300ms) on
429/5xx/timeouts only, matching spec table 8.

## Recommendation 4: Concurrency lock = DB unique partial index (not advisory lock as primary)
Add `CREATE UNIQUE INDEX uq_job_running_per_type ON job(series_type) WHERE status = 'RUNNING'`.
Trigger-sync inserts a RUNNING job row first; a unique-violation maps directly to the spec's
"409 - a job for this series type is already running" (§7.1). Self-documenting (the lock IS a
visible row), survives process restarts, no extra infra. Document Postgres advisory lock only as a
noted future alternative if per-series (not per-type) concurrency control is ever needed.

## Database design (new file: database-design.md)
Tables (building on spec §9 + architecture.md §10, with deviations noted):
- `job`: id PK, series_type, status, requested_series_name (nullable), requested_start_date/
  requested_end_date (nullable, caller override), started_at, finished_at, correlation_id.
  Unique partial index as above.
- `job_task`: id PK, job_id FK, series_id FK, status, failure_reason, failure_detail, values_sent,
  started_at, finished_at. Index (job_id), (series_id, status).
- `job_task_chunk` (NEW vs spec's minimal schema): id PK, job_task_id FK, chunk_start_date,
  chunk_end_date, orion_call_count, status (PENDING/FETCHED/TRANSFORMED/PUBLISHED/PERSISTED/FAILED),
  values_fetched/new/updated/unchanged counts, ledgerpoint_batch_id. Gives per-month audit trail
  (acceptance criteria #2: "3 months -> exactly 3 Orion calls" becomes directly queryable) and a
  resume point if a job fails partway through a multi-month series. Unique (job_task_id,
  chunk_start_date).
- `task_warning` (from architecture.md §10): id PK, job_task_chunk_id FK, warning_type
  (GAP_MISSING/DST_SPRING_FILL/DST_FALL_REPEAT), interval_date, interval_number, detail.
- `series`: id PK, name (unique), series_type, unit (last observed from Orion values call), 
  time_zone_code, earliest_data_date, latest_data_date, last_updated_date (Orion staleness source),
  last_synced_date (service high-watermark - replaces architecture.md's separate `sync_checkpoint`
  table, avoids redundancy), created_at/updated_at.
- `series_rule_override` (§6.6): id PK, orion_series_name (unique), ledgerpoint_key_override
  (nullable), skip (bool), active (bool), updated_at.
- `series_value` (TimescaleDB hypertable on start_instant, monthly chunks): series_id FK,
  start_instant (timestamptz UTC), repeated_hour (bool), value (numeric 19,6), interval_number
  (varchar 5, pre-computed e.g. "2x" for audit/replay without recompute), local_date (date, for
  fast day-level LedgerPoint-shape queries), synced_at. PRIMARY KEY (series_id, start_instant,
  repeated_hour) - matches spec's exact diff key. Index (series_id, local_date).
Deviation vs architecture.md §10: DROP `job_execution_log` table - use structured JSON logs with
jobId/seriesName/correlationId (already required by spec §8) instead; job/job_task/job_task_chunk
rows remain the DB source of truth for audit questions, logs cover operational trace, avoiding
write amplification on every log line.

## Steps
1. Update `architecture.md`:
   - Add ADR-003 "Hexagonal Architecture within the Modular Monolith" (the debate + recommendation
     above), replacing/augmenting the plain package list in current §5 with the ports/adapters
     mapping.
   - Add ADR-004 "Virtual Threads over Reactive for Series Parallelism".
   - Refine the TimescaleDB section (§9) to state the minimal-POC scoping decision explicitly.
   - Add a "Concurrency & Job Locking" section documenting the unique partial index mechanism and
     how it produces the spec's 409 response.
   - Update §10 persistence table list to match the finalized schema (add job_task_chunk, drop
     sync_checkpoint/job_execution_log, note reasons).
2. *(parallel with 1)* Create `database-design.md`: full schema as designed above, including a
   mermaid ER diagram, each table's columns/types/keys/indexes, the hypertable declaration
   rationale, and explicit mapping back to spec acceptance criteria (§11) and failure reason codes
   (§8.1) showing every reason code has a column path to be recorded.
3. *(depends on 1-2)* Cross-check both docs against spec §11 acceptance criteria one by one and
   note in database-design.md (or an appendix) how each criterion is satisfied by a
   table/column/mechanism - closes the loop between spec and design.

No code/project scaffolding in this pass (per user's chosen deliverable scope) - that is a
follow-up planning/build cycle once these docs are approved.

## Relevant files
- `d:\poc_vs\architecture.md` - add ADR-003, ADR-004, concurrency section, refine §9/§10.
- `d:\poc_vs\database-design.md` (new) - full DB design doc.

## Verification
1. Manual review: every failure reason code in spec §8.1 maps to a column/enum value in job_task.
2. Manual review: every acceptance criterion in spec §11 has a corresponding mechanism described in
   architecture.md or database-design.md.
3. Manual review: hexagonal package mapping covers every existing package name from architecture.md
   §5 (no orphaned concept).

## Decisions
- Hexagonal applied within modular monolith, not as a replacement (see Recommendation 1).
- Keep TimescaleDB, scoped minimally for POC (hypertable only, no compression policy yet).
- Flyway for migrations.
- Virtual threads + blocking RestClient instead of reactive stack for series parallelism.
- Concurrency lock: unique partial index on job(series_type) WHERE status='RUNNING'.
- Drop `sync_checkpoint` (merged into series.last_synced_date) and `job_execution_log` (replaced by
  structured logging) from architecture.md's originally suggested table list.
- Add `job_task_chunk` table (not in original spec/architecture.md) for per-month audit/resume.

## Status: IMPLEMENTED (docs-only pass)
- architecture.md updated: ADR-003 (hexagonal), ADR-004 (virtual threads), §9 TimescaleDB POC
  scoping, §10 persistence table list, new "Concurrency & Job Locking" section, §22 summary bullets.
- database-design.md created: ER diagram, full table defs (job, job_task, job_task_chunk,
  task_warning, series, series_rule_override, series_value hypertable), enums, acceptance-criteria
  traceability table, deviations section.
- Next (not started): Maven project scaffolding with hexagonal package layout, Flyway migration
  files, first vertical slice (Orion auth + list-series).

## Vertical Slice 1 (DONE)
- Maven/Spring Boot 3.3.4/Java 21 scaffold, Flyway V1 schema (all 7 tables + hypertable).
- Domain: SeriesType, TimeZoneCode, SeriesDescriptor, UnsupportedTimeZoneException.
- Ports: RefreshSeriesCatalogUseCase, SeriesSourcePort, SeriesRepositoryPort, ClockPort.
- Orion adapter: token cache (60min+refresh-on-401), Resilience4j rate limiter+retry via
  Decorators (needed `resilience4j-all` dependency, not `resilience4j-decorators` - that
  artifact does not exist on Maven Central).
- Persistence adapter for `series` table (upsert/findByType).
- NOTE on naming confusion raised by user: `SeriesCatalogController` at
  `GET /api/internal/series-catalog/{seriesType}` is a **deliberately temporary walking-skeleton
  endpoint I invented** - it is NOT from the spec. The real spec endpoint is
  `POST /api/series/{seriesType}/sync` (spec §7.1), which triggers a full Job. I used a
  different path/verb specifically so it wouldn't be mistaken for the final endpoint. Next slice
  should retire/replace it with the real trigger endpoint once Job/Task exists.
- Tests: HexagonalArchitectureTest (ArchUnit, PASS), OrionClientAdapterTest (WireMock, PASS).
  SeriesRepositoryAdapterIT and SeriesCatalogControllerIT (Testcontainers) written but not yet
  confirmed passing by user (needs Docker). Known fix already applied by user: import
  AutoConfigureTestDatabase from `...test.autoconfigure.jdbc` not `...orm.jpa`.

## Vertical Slice 2 (DONE, compiles clean; full `mvn test` run pending - terminal in this
## environment intermittently drops PATH for mvn.cmd, needs `$env:Path += ';C:\apache-maven-3.9.16\bin'`)
- Retired SeriesCatalogController/SeriesCatalogEntryResponse/SeriesCatalogControllerIT (the
  confusing temporary endpoint) - replaced by the real spec endpoints.
- Domain: JobStatus, TaskStatus, FailureReason enums; Job, JobTask, JobSummary, JobDetail records.
  SeriesDescriptor now carries a nullable `id` (populated after persistence) so JobTask can
  reference a series.
- Domain exceptions: JobAlreadyRunningException (-> 409), JobNotFoundException (-> 404).
- Ports: JobRepositoryPort, TaskRepositoryPort (out); TriggerSyncUseCase, GetJobUseCase (in).
- Application: SyncOrchestrationService (composes RefreshSeriesCatalogUseCase + the two new
  ports). Scope boundary clearly documented in its Javadoc: creates Job+PENDING Tasks only; job
  intentionally stays RUNNING (not faked COMPLETED) until the fetch/transform/diff/publish
  pipeline (roadmap items 5-9) exists. Empty-series edge case closes the job as COMPLETED
  immediately (legitimate, not a stub).
- Persistence: JobEntity/JobJpaRepository/JobRepositoryAdapter (saveAndFlush +
  DataIntegrityViolationException -> JobAlreadyRunningException, enforcing the unique partial
  index); JobTaskEntity/JobTaskJpaRepository/TaskRepositoryAdapter.
- Web: SyncController (`POST /api/series/{seriesType}/sync`), JobController
  (`GET /api/jobs/{jobId}`), JobSummaryResponse/JobDetailResponse/JobTaskResponse DTOs,
  GlobalExceptionHandler extended for the two new exceptions.
- Tests: SyncControllerIT (Testcontainers+WireMock) replaces SeriesCatalogControllerIT - covers
  job+task creation and the 409-on-duplicate-running-job case (using typeB, self-contained, to
  avoid cross-test DB state coupling since jobs can't close themselves yet).
- `mvn -q compile` confirmed clean. `mvn test` (full suite incl. Testcontainers) not yet
  confirmed in this session - rerun and report before relying on it.

## Vertical Slice 3 (DONE - not yet compiled/tested by user)
- Domain model additions: RawSeriesValue, NormalizedSeriesValue, ChangeType,
  SeriesValueKey, ClassifiedSeriesValue.
- Domain exception: UnsupportedGranularityException (-> FailureReason.UNSUPPORTED_GRANULARITY).
- New domain.service package (pure, framework-free, java.time only):
  - IntervalAssigner (spec §6.3) - also has `validate(int)` for eager granularity checks.
  - UnitMapper (spec §6.5).
  - DstAdjustmentService (spec §6.3/§6.4) - uses java.time ZoneRules.getTransition() for
    gap/overlap detection instead of hardcoding "2 AM"; fall-back repeatedHour comes straight
    from Orion's flag, spring-forward gap is detected by scanning the day's candidate local
    times and filling from the previous interval.
  - ChangeDetector (spec §6.7) - pure function, takes a Map<SeriesValueKey,BigDecimal> of
    previously-stored values (persistence concern lives outside the domain).
- Tests reproduce spec §12 fixtures exactly (DstAdjustmentServiceTest spring-forward/fall-back),
  plus IntervalAssignerTest, UnitMapperTest, ChangeDetectorTest (incl. 1e-9 epsilon boundary).
- NOTE: user asked me to stop running long mvn compile/test commands myself - they will run and
  share output. Don't invoke run_in_terminal for mvn compile/test going forward unless asked.
- Known deferred scope (documented in DstAdjustmentService javadoc): arbitrary (non-DST) gap
  warning emission per spec §6.8 is NOT built yet - deferred to roadmap item 10
  (job_task_chunk/task_warning persistence), since it needs DB warning storage to be meaningful.

## Vertical Slice 4 (DONE - not yet compiled/tested by user)
- Domain: SeriesValuesResult, DateRange records.
- New domain.service.MonthChunker (spec §3.1 steps 1-2): chunkByMonth + normalizeEndToLastDayOfMonth,
  pure/framework-free. MonthChunkerTest directly verifies acceptance criterion #2 (3-month window
  -> exactly 3 chunks).
- SeriesSourcePort gained `fetchValues(seriesName, startPrevailing, endPrevailing)`.
- New Orion DTOs: OrionValueDto, OrionSeriesValuesResponse.
- OrionClientAdapter.fetchValues implemented: same shared rate limiter/retry/401-refresh pattern
  as listSeries (reuses the same "orion" RateLimiter/Retry instances - correct per ADR-004, one
  global budget across all Orion calls). GET /v1/series/{name}/values?start_prevailing=...&
  end_prevailing=..., dates formatted via OrionDateFormat.
- Test added: fetchValuesMapsTheFallBackDayExampleFromTheSpec in OrionClientAdapterTest,
  reproducing the exact spec §4.3 JSON example (incl. repeatedHour pair).
- Still NOT wired: nothing calls fetchValues yet (SyncOrchestrationService only lists series /
  creates tasks). That wiring is roadmap item 9 (full per-series pipeline), after series_value
  persistence (item 7) and LedgerPoint adapter (item 8) exist.

## Vertical Slice 5 (DONE - not yet compiled/tested by user)
- New port: SeriesValuePort (findExistingValues by seriesId+local-date range -> Map<SeriesValueKey,
  BigDecimal>; upsertAll(seriesId, List<NormalizedSeriesValue>)).
- New persistence: SeriesValueId (@IdClass composite key matching series_value's
  (series_id,start_instant,repeated_hour) PK), SeriesValueEntity, SeriesValueJpaRepository,
  SeriesValueRepositoryAdapter.
- New IT: SeriesValueRepositoryAdapterIT - proves upsert idempotency against real TimescaleDB AND
  wires the read-back Map directly into ChangeDetector, explicitly validating acceptance criteria
  #6 (re-run -> UNCHANGED) and #7 (one changed value -> UPDATED) end-to-end through real
  persistence (not just in-memory unit test).
- Still NOT wired into SyncOrchestrationService - that's roadmap item 9 (full pipeline), after
  the LedgerPoint adapter (item 8) exists.

## Vertical Slice 6 (DONE - not yet compiled/tested by user)
- New domain model: LedgerPointPublishResult, LedgerPointValidationError.
- New domain exceptions: LedgerPointAuthException (401/403 -> must fail whole Job, not just
  Task - orchestration in item 9 must handle this specially), LedgerPointUnknownSeriesException
  (409 -> UNKNOWN_SERIES), LedgerPointValidationException (400 -> TARGET_REJECTED, carries
  parsed row-level errors), LedgerPointTransientException (5xx/network -> retried).
- New domain.service utilities: BatchSplitter (generic list partitioning, 10k row limit),
  BatchLabelBuilder (spec §6.6 {INS|UPD}_{key}_{unit}_{tz} label for logs only, not sent to
  LedgerPoint - batch is INS only if every value is NEW, else UPD).
- New port: SeriesPublisherPort.publish(key, unitCode, timeZoneCode,
  List<ClassifiedSeriesValue>, correlationId) -> LedgerPointPublishResult. Caller must pre-filter
  to NEW/UPDATED only (UNCHANGED never sent) - port doesn't filter itself.
- New adapter.out.ledgerpoint package: LedgerPointProperties, DTOs (ValueRow/BatchResponse/
  Error/ErrorResponse), LedgerPointClientAdapter - splits payload via BatchSplitter, Resilience4j
  Retry only (NO rate limiter - spec only gives Orion a published rate limit), uses
  retrieve()/onStatus() pattern matching OrionClientAdapter (deliberately avoided RestClient's
  .exchange() API since it's unverified in this codebase - stuck to the proven working pattern).
  400 error body parsed via injected Jackson ObjectMapper inside the onStatus handler.
- RestClientConfig gained a second bean `ledgerPointRestClient`. application.yml gained
  `ledgerpoint.*` properties + `resilience4j.retry.instances.ledgerpoint`.
- Tests: BatchSplitterTest, BatchLabelBuilderTest, LedgerPointClientAdapterTest (WireMock,
  no Spring: success parse, 400->validation errors parsed, 409, 401, 5xx retry-then-success,
  batch splitting into 3 requests for 5 rows at maxBatchSize=2).
- Still NOT wired into SyncOrchestrationService - that's roadmap item 9 (full pipeline).

## Vertical Slice 7 (DONE - not yet compiled/tested by user) - roadmap item 9, full pipeline wiring
- PREREQUISITE FIX: moved OrionAuthException/OrionTransientException/OrionClientException from
  adapter.out.orion to domain.exception (same pattern as LedgerPoint exceptions) - the
  application layer needs to catch these per-Task, and catching adapter-package exceptions from
  application would have violated the ArchUnit "application must not depend on adapters" rule.
  Updated all references: OrionClientAdapter, OrionTokenCache, GlobalExceptionHandler,
  OrionClientAdapterTest, application.yml retry/ignore-exceptions FQNs.
- SeriesDescriptor gained `lastSyncedDate` (LocalDate, nullable) - needed for window resolution
  (spec §3.1). Updated all 4 construction sites (OrionClientAdapter maps null, SeriesRepositoryAdapter
  reads/writes it, 2 IT tests needed an extra null arg).
- SeriesRepositoryPort gained `updateLastSyncedDate(seriesId, LocalDate)`.
- TaskRepositoryPort gained `updateTaskStatus(taskId, status, failureReason, failureDetail, valuesSent)`
  - TaskRepositoryAdapter now also takes ClockPort (sets startedAt on first transition, finishedAt
    always).
- VirtualThreadConfig gained a raw `ExecutorService virtualThreadExecutor()` bean
  (destroyMethod=shutdown) - used via `invokeAll` for true "submit all, block for all" semantics
  (cleaner than hand-rolled CompletableFutures for this synchronous-HTTP-call use case).
- New `application.SeriesSyncWorker` (sync-engine): processes ONE series end-to-end - chunk by
  month -> fetchValues -> IntervalAssigner.validate -> DstAdjustmentService.normalize -> diff via
  SeriesValuePort+ChangeDetector -> publish (NEW/UPDATED only) -> persist only after publish
  succeeds -> updateLastSyncedDate -> updateTaskStatus. ALL expected exceptions caught here and
  mapped to exact FailureReason codes; LedgerPointAuthException is the one case that sets
  TaskOutcome.jobShouldFail=true (spec §8 "fail Job immediately").
- New package-private `application.TaskOutcome` record (status, jobShouldFail) - internal
  communication between worker and orchestrator, not a port/domain concept.
- Rewrote `SyncOrchestrationService.triggerSync`: creates Job+Tasks (as before), then runs every
  Task concurrently via `virtualThreadExecutor.invokeAll(...)` (ADR-004), blocks for all results,
  computes final JobStatus (FAILED only if jobShouldFail flag set by any task; COMPLETED_WITH_FAILURES
  if any task FAILED; else COMPLETED), marks job finished, returns final JobSummary. Window
  resolution (`resolveWindow`) implements spec §3.1 exactly (caller override > lastSyncedDate >
  today-1yr; end > series.latestDataDate > today; always normalized to end of month).
- Tests: SeriesSyncWorkerTest (Mockito, no Spring/Docker) covers success/unchanged/NO_DATA/
  UNSUPPORTED_GRANULARITY/LedgerPointAuthException-escalation. SyncControllerIT rewritten:
  main test now stubs Orion values + LedgerPoint publish and asserts job reaches COMPLETED with
  task SUCCESS/valuesSent=1; the 409 test had to change approach since the pipeline is now
  synchronous end-to-end within one HTTP call (a sequential second call can no longer catch a
  job mid-RUNNING) - now uses a WireMock fixed delay on the LedgerPoint stub plus a concurrent
  first request (CompletableFuture) to genuinely race the unique-partial-index lock.
- Explicit scope boundaries carried into this slice (documented in SeriesSyncWorker's Javadoc,
  unchanged from before): no series_rule_override (item 11), no job_task_chunk/task_warning
  persistence or non-DST gap warnings (item 10), no staleness check (item 12). Task only ever
  reaches SUCCESS or FAILED in this slice - never SUCCESS_WITH_WARNINGS or SKIPPED yet.
- Superseded later: the trigger is now asynchronous (202 Accepted + Location, poll
  GET /api/jobs/{id}); the "synchronous end-to-end within one HTTP call" notes above no longer
  apply. Overrides, staleness, Retry-After, job abort on LedgerPoint auth failure, Micrometer
  metrics, extra ArchUnit rules and the PENDING chunk stage were added afterwards.

## Vertical Slice 8 (DONE - not yet compiled/tested by user) - roadmap item 10, chunk/warning persistence
- New domain model: ChunkStatus, WarningType (GAP_MISSING is the only one actually emitted;
  DST_SPRING_FORWARD_FILLED/DST_FALL_BACK_REPEATED reserved, unused for now since DST adjustments
  are NOT warnings per spec §6.4 - only genuine data gaps are, per §6.8), MissingInterval.
- New domain.service.GapDetector: runs AFTER DstAdjustmentService.normalize, so the spring-forward
  hour is already filled and never reported as missing. For every date in [chunkStart,chunkEnd],
  checks the base (non-'x') interval set 1..N (N depends on granularity) is complete; reports one
  MissingInterval per gap, including entire dates with zero data at all. GapDetectorTest covers:
  no gaps, partial day, whole missing date, repeated-hour entries not counted twice.
- New ChunkRepositoryPort (createChunk/updateChunkStatus/recordWarning) + persistence
  (JobTaskChunkEntity, TaskWarningEntity, their JPA repos, ChunkRepositoryAdapter). orion_call_count
  is always set to 1 per chunk (one logical fetchValues call; internal HTTP retries aren't visible
  at this layer).
- SeriesSyncWorker restructured: extracted processChunk(...) which creates a job_task_chunk row
  up front, and on ANY RuntimeException persists it as FAILED (with zeroed counts) before
  rethrowing (so process()'s existing catch blocks still map the right FailureReason) - on success
  persists PERSISTED with full counts (valuesFetched/New/Updated/Unchanged, ledgerPointBatchId).
  Task now finishes SUCCESS_WITH_WARNINGS (not SUCCESS) if ANY chunk had gaps - SUCCESS_WITH_WARNINGS
  is now reachable, matching spec §6.8 exactly. job_task/job status logic (determineFinalStatus)
  unaffected - SUCCESS_WITH_WARNINGS still only counts as "not FAILED" for job-level status.
- Tests: SeriesSyncWorkerTest rewritten/expanded - split the old "successfulSync..." test into a
  clean single-day/daily-granularity no-gaps case (plain SUCCESS) and a new sparse-hourly-day case
  asserting exactly 23 recordWarning calls + SUCCESS_WITH_WARNINGS; all tests now mock
  ChunkRepositoryPort.createChunk. SyncControllerIT's happy-path test narrowed to a single day +
  daily granularity (was a whole month + 1 hourly value, which would now correctly produce
  SUCCESS_WITH_WARNINGS instead of the plain SUCCESS the test asserted) - kept the E2E test focused
  on wiring correctness since gap-warning nuances are already unit-tested.
- Still deferred: series_rule_override (item 11, makes SKIPPED reachable), staleness check
  (item 12).

## Remaining Roadmap (next up starts at #11)
11. `series_rule_override` lookup (skip/key override) - makes SKIPPED reachable.
12. Staleness check (N days, default 5).
13. Acceptance-criteria test suite per spec §11 (incl. DST fixtures §12) - cross-check everything
    built so far against the full list.

## Further Considerations
1. Multi-instance deployment: unique partial index only enforces one RUNNING job per series_type
   process-wide (works regardless of instance count since it's DB-level) - no further locking
   needed even for multi-instance ECS. Confirmed sufficient, no open question.
2. Next planning cycle (not now): Maven project scaffolding with the hexagonal package layout,
   Flyway migration files, and a first vertical slice (Orion auth + list-series) - revisit once
   these two docs are approved.