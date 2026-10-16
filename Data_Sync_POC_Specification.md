# Data Sync Service — POC Specification & Dummy API Contracts

**Version:** 0.1 (POC)
**Audience:** Engineer building the Data Sync Service from scratch
**Status:** Draft — all system names, endpoints, hosts, credentials, and sample data below are **fictional**

---

## 1. Purpose

Build a small service — the **Data Sync Service** — that:

1. **Fetches** time-series data from an upstream system called **Orion**.
2. **Transforms and validates** that data (date normalization, interval assignment, daylight-saving handling, unit mapping, change detection).
3. **Posts** the resulting values to a downstream system called **LedgerPoint**.
4. **Tracks** each run as a *Job* with one *Task* per series, so failures are visible and re-runnable.

This document defines:

- The dummy **Orion** contract (source)
- The dummy **LedgerPoint** contract (target)
- The **Data Sync Service** API you are expected to expose
- The transformation rules between source and target
- Persistence, error handling, and acceptance criteria

> **Scope for the POC:** Series of type `typeA` and `typeB` only. Additional dataset shapes are listed as stretch goals in §10.

---

## 2. Systems & Glossary

| Name | Role | Notes |
|---|---|---|
| **Orion** | Source system | Owns series definitions and series values. Token-authenticated REST API. |
| **LedgerPoint** | Target system | Accepts values in a flat `date / interval / key / value` format. |
| **Data Sync Service** | The service you build | Orchestrates fetch → transform → post, and records job history. |

| Term | Meaning |
|---|---|
| **Series** | A named time series, e.g. `SERIES_ALPHA_001`. |
| **Series type** | `typeA` or `typeB`. Both are processed identically for the POC. |
| **Granularity** | Time step of a series' values, in minutes: `15`, `60`, or `1440` (daily). |
| **Interval** | 1-based ordinal of a value within its local day (period-ending style). Hourly → `1..24`; 15-min → `1..96`; daily → `1`. |
| **Prevailing time** | Local wall-clock time *including* daylight-saving shifts. |
| **Repeated hour** | On the autumn "fall back" day the 01:00–02:00 hour occurs twice; the second occurrence is flagged `repeatedHour = true`. |
| **Job** | One execution of a sync request (e.g. "sync all `typeA` series"). |
| **Task** | One series processed within a job. |

---

## 3. End-to-End Flow

```
 ┌────────────┐   1. POST /api/series/{type}/sync
 │  Caller /  │ ─────────────────────────────────────────┐
 │ Scheduler  │                                          ▼
 └────────────┘                               ┌─────────────────────┐
                                              │  Data Sync Service  │
                                              │                     │
   ┌──────────┐  2. POST /auth/token          │  a. create Job      │
   │          │ ◄──────────────────────────── │                     │
   │          │  3. GET  /v1/series?type=..   │  b. list series     │
   │  Orion   │ ◄──────────────────────────── │                     │
   │          │  4. GET  /v1/series/{name}/   │  c. for each series:│
   │          │          values?start&end     │     - create Task   │
   │          │ ◄──────────────────────────── │     - fetch (monthly│
   └──────────┘                               │       chunks)       │
                                              │     - transform     │
   ┌───────────┐ 5. POST /api/v2/series/      │     - diff vs. last │
   │LedgerPoint│         values               │       sync          │
   │           │ ◄─────────────────────────── │     - post          │
   └───────────┘                              │     - mark Task     │
                                              │  d. close Job       │
                                              └─────────────────────┘
```

### 3.1 Processing steps per series

1. **Resolve date window**
   - If the caller supplies `startDate`/`endDate`, use them.
   - Otherwise: `start` = last successfully synced date for that series; if the series has never been synced, fall back to **1 year prior** to today. `end` = the series' `latestDataDate` from Orion.
   - Normalize `end` to the **last day of its month**.
2. **Chunk by month.** Call Orion once per calendar month in the window (keeps payloads small and failures isolated).
3. **Map** Orion values to the internal model (§6).
4. **Diff** against previously synced values → classify each point as **new**, **updated**, or **unchanged**. Only new + updated points are sent.
5. **Fill gaps / daylight-saving adjustments** (§6.4).
6. **Assign intervals** (§6.3).
7. **Post** to LedgerPoint (§5).
8. **Persist** synced values and update Task status.

---

## 4. Source Contract — Orion API (dummy)

**Base URL:** `https://orion.example.com`
**Format:** JSON, UTF-8
**Auth:** Bearer token obtained from `/auth/token`. Tokens are valid for **60 minutes**.
**Rate limit:** 5 requests / second per client. Exceeding returns `429` with `Retry-After` header (seconds).
**Date format (all Orion fields):** `M/d/yyyy h:mm:ss a` in **prevailing local time** of the series, e.g. `3/10/2025 2:00:00 AM`.

### 4.1 Get access token

`POST /auth/token`

**Request**
```json
{
  "login": "svc-data-sync",
  "password": "<injected-at-runtime>"
}
```

**Response `200 OK`**
```json
{
  "accessToken": "test-token-abc123",
  "expiresInSeconds": 3600
}
```

| Status | Meaning |
|---|---|
| 200 | Token issued |
| 401 | Bad credentials |
| 500 | Orion internal error |

All subsequent calls send: `Authorization: Bearer <accessToken>`.

---

### 4.2 List series by type

`GET /v1/series?type={typeA|typeB}`

**Response `200 OK`**
```json
{
  "series": [
    {
      "seriesName": "SERIES_ALPHA_001",
      "seriesType": "typeA",
      "timeZone": 6,
      "lastUpdatedDate": "10/12/2026 6:00:00 PM",
      "earliestDataDate": "1/1/2020 12:00:00 AM",
      "latestDataDate": "10/13/2026 11:00:00 PM"
    },
    {
      "seriesName": "SERIES_BRAVO_002",
      "seriesType": "typeA",
      "timeZone": 5,
      "lastUpdatedDate": "10/12/2026 7:00:00 PM",
      "earliestDataDate": "6/1/2018 12:00:00 AM",
      "latestDataDate": "10/12/2026 11:00:00 PM"
    }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `seriesName` | string | Unique series identifier |
| `seriesType` | string | `typeA` or `typeB` |
| `timeZone` | int | Timezone code (see §6.2) |
| `lastUpdatedDate` | string (date) | When Orion last received new data for this series |
| `earliestDataDate` | string (date) | First available value |
| `latestDataDate` | string (date) | Last available value |

| Status | Meaning |
|---|---|
| 200 | OK (empty `series` array is valid) |
| 400 | Unknown `type` |
| 401 | Missing/expired token |
| 429 | Rate limited |

---

### 4.3 Fetch series values

`GET /v1/series/{seriesName}/values?start_prevailing={date}&end_prevailing={date}`

| Query param | Required | Example |
|---|---|---|
| `start_prevailing` | yes | `11/1/2026 12:00:00 AM` (URL-encoded) |
| `end_prevailing` | yes | `11/30/2026 11:59:59 PM` (URL-encoded) |

**Response `200 OK`** (hourly example, abbreviated; includes the fall-back day)
```json
{
  "seriesName": "SERIES_ALPHA_001",
  "unit": "UNIT_A",
  "granularity": 60,
  "timeZone": 6,
  "asOfDate": "10/12/2026 6:00:00 PM",
  "values": [
    { "startDate": "11/1/2026 12:00:00 AM", "repeatedHour": false, "value": 31.25 },
    { "startDate": "11/1/2026 1:00:00 AM",  "repeatedHour": false, "value": 29.80 },
    { "startDate": "11/1/2026 1:00:00 AM",  "repeatedHour": true,  "value": 29.10 },
    { "startDate": "11/1/2026 2:00:00 AM",  "repeatedHour": false, "value": 28.75 }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `seriesName` | string | Echo of path param |
| `unit` | string | Unit code: `UNIT_A`, `UNIT_B`, `UNIT_C`, `UNIT_D`, or other |
| `granularity` | int | Minutes per value: `15`, `60`, `1440` |
| `timeZone` | int | Timezone code (§6.2) |
| `asOfDate` | string (date) | As-of timestamp of this data snapshot |
| `values[].startDate` | string (date) | Interval start, prevailing time |
| `values[].repeatedHour` | bool | `true` only for the repeated hour on the fall-back day |
| `values[].value` | number | Value (up to 6 decimal places) |

| Status | Meaning |
|---|---|
| 200 | OK (empty `values` is valid — means no data in range) |
| 400 | Invalid / reversed date range |
| 401 | Missing/expired token |
| 404 | Unknown series name |
| 429 | Rate limited |
| 5xx | Transient Orion error — retry |

---

## 5. Target Contract — LedgerPoint API (dummy)

**Base URL:** `https://ledgerpoint.example.com`
**Format:** JSON, UTF-8
**Auth:** `X-Api-Key: <injected-at-runtime>` header
**Correlation:** send `X-Correlation-Id` (UUID) on every call; LedgerPoint echoes it back.
**Batch limit:** max **10,000** values per request — split larger payloads.

### 5.1 Upsert values

`POST /api/v2/series/values`

**Request body** — a flat array of value rows:
```json
[
  { "date": "2026-11-01", "interval": "1",  "key": "SERIES_ALPHA_001", "value": "31.25" },
  { "date": "2026-11-01", "interval": "2",  "key": "SERIES_ALPHA_001", "value": "29.80" },
  { "date": "2026-11-01", "interval": "2x", "key": "SERIES_ALPHA_001", "value": "29.10" },
  { "date": "2026-11-01", "interval": "3",  "key": "SERIES_ALPHA_001", "value": "28.75" }
]
```

| Field | Type | Description |
|---|---|---|
| `date` | string `yyyy-MM-dd` | Local day |
| `interval` | string | 1-based interval within the day; suffix `x` marks the repeated hour (§6.4) |
| `key` | string | LedgerPoint series key (Orion name, or override from rules — §6.6) |
| `value` | string | Plain decimal string, no scientific notation (e.g. `"0.000125"`, not `"1.25E-4"`) |

LedgerPoint treats `(key, date, interval)` as the unique key and **upserts** — re-sending the same row is idempotent.

**Response `200 OK`**
```json
{
  "batchId": "f7c1e2a4-1b2c-4d5e-9f00-1234567890ab",
  "received": 4,
  "inserted": 3,
  "updated": 1
}
```

**Response `400 Bad Request`**
```json
{
  "batchId": null,
  "errors": [
    { "index": 2, "field": "interval", "message": "Interval '2x' is only valid on a fall-back day" }
  ]
}
```

| Status | Meaning | Service action |
|---|---|---|
| 200 | Accepted | Mark Task `SUCCESS` |
| 400 | Validation failure (whole batch rejected) | Mark Task `FAILED`, store error body as failure detail, **do not retry** |
| 401/403 | Auth failure | Fail Job immediately |
| 409 | Series `key` not configured in LedgerPoint | Mark Task `FAILED` (reason `UNKNOWN_SERIES`) |
| 5xx | Transient error | Retry (§8) |

### 5.2 (Optional) Get series metadata

`GET /api/v2/series/{key}`

```json
{
  "key": "SERIES_ALPHA_001",
  "unit": "U1",
  "timeZone": "TZ_C",
  "lastSyncedDate": "2026-10-12"
}
```
Can be used as a sanity check or as an alternative source for "last synced date" (§3.1).

---

## 6. Transformation Rules

### 6.1 Dates

- Parse Orion dates with pattern `M/d/yyyy h:mm:ss a` (US locale).
- Store internally as UTC `Instant` + the series' timezone code.
- LedgerPoint `date` is the **local day** (`yyyy-MM-dd`) of the value.

### 6.2 Timezone codes

| Orion `timeZone` | LedgerPoint `timeZone` | IANA zone |
|---|---|---|
| 5 | `TZ_E` | `America/New_York` |
| 6 | `TZ_C` | `America/Chicago` |
| 7 | `TZ_M` | `America/Denver` |
| 8 | `TZ_P` | `America/Los_Angeles` |

Unknown codes → Task `FAILED` with reason `UNSUPPORTED_TIMEZONE`.

### 6.3 Interval assignment

| Granularity | Intervals per normal day | Formula (local time) |
|---|---|---|
| 1440 (daily) | 1 | always `1` |
| 60 (hourly) | 24 | `hour + 1` (period-ending) |
| 15 (quarter-hour) | 96 | `hour * 4 + minute / 15 + 1` |

Any other granularity → Task `FAILED` with reason `UNSUPPORTED_GRANULARITY`.

### 6.4 Daylight-saving handling

- **Fall-back day (25 hours):** the value with `repeatedHour = true` gets the **same interval number** as the first occurrence, with an `x` suffix (e.g. `2x`). For 15-min series, each of the 4 repeated quarter-hours gets the suffix.
- **Spring-forward day (23 hours):** Orion returns no value for the skipped hour. Fill the missing interval by **copying the previous interval's value**, so LedgerPoint always receives a contiguous interval set. Never emit an `x` interval on a spring-forward day.

### 6.5 Unit mapping

| Orion `unit` | LedgerPoint unit code |
|---|---|
| `UNIT_A` | `U1` |
| `UNIT_B` | `U1` |
| `UNIT_C` | `U2` |
| `UNIT_D` | `U3` |
| anything else | pass through unchanged |

### 6.6 Naming / rule overrides

- Default LedgerPoint `key` = Orion `seriesName`.
- A simple rules table (DB or config) may map an Orion series to a different LedgerPoint key, or flag it **skip**. Skipped series → Task `SKIPPED`.
- Batch label (for logs / LedgerPoint audit) = `{INS|UPD}_{key}_{unitCode}_{timeZone}`, e.g. `UPD_SERIES_ALPHA_001_U1_TZ_C`.

### 6.7 Change detection

Compare each fetched value against the last stored value for `(series, startInstant, repeatedHour)`:

| Condition | Classification | Sent to LedgerPoint? |
|---|---|---|
| No stored value | **new** | yes |
| Stored, `|old − new| > 1e-9` | **updated** | yes |
| Stored, equal | unchanged | no |

### 6.8 Gap filling

Within the requested window, if an interval (other than the spring-forward hour) is missing, **do not invent data** — log a warning on the Task and continue. Tasks with gaps finish as `SUCCESS_WITH_WARNINGS`.

### 6.9 Staleness check

If `lastUpdatedDate` is older than **N days** (configurable, default `5`), mark the Task `FAILED` with reason `STALE_SERIES` and do not post.

---

## 7. Data Sync Service API (what you expose)

### 7.1 Trigger a sync

`POST /api/series/{seriesType}/sync?seriesName={optional}&startDate={optional}&endDate={optional}`

| Param | Type | Notes |
|---|---|---|
| `seriesType` (path) | `typeA` \| `typeB` | |
| `seriesName` | string | Sync just one series |
| `startDate` / `endDate` | ISO-8601 instant, e.g. `2026-01-01T00:00:00Z` | Override the computed window |

**Response `200 OK`**
```json
{
  "jobId": 1042,
  "seriesType": "typeA",
  "status": "COMPLETED_WITH_FAILURES",
  "startedAt": "2026-10-13T06:00:00Z",
  "finishedAt": "2026-10-13T06:04:12Z",
  "taskCounts": {
    "SUCCESS": 41,
    "SUCCESS_WITH_WARNINGS": 2,
    "SKIPPED": 3,
    "FAILED": 1
  }
}
```

| Status | Meaning |
|---|---|
| 200 | Job ran (check `status` / `taskCounts`) |
| 400 | Invalid type or date range (`startDate` > `endDate`) |
| 409 | A job for this series type is already running |
| 500 | Unexpected failure before job creation |

### 7.2 Get job details

`GET /api/jobs/{jobId}`

```json
{
  "jobId": 1042,
  "status": "COMPLETED_WITH_FAILURES",
  "tasks": [
    { "seriesName": "SERIES_ALPHA_001", "status": "SUCCESS", "valuesSent": 744 },
    { "seriesName": "SERIES_BRAVO_002", "status": "FAILED", "failureReason": "STALE_SERIES",
      "failureDetail": "Last update 2026-10-01 is older than 5 days" }
  ]
}
```

---

## 8. Resilience & Error Handling

| Concern | Requirement |
|---|---|
| **Token refresh** | Cache the Orion token; refresh on expiry or on a `401` (retry the call once after refresh). |
| **Rate limiting** | Client-side limiter at Orion's published rate (5 req/s); honor `Retry-After` on `429`. |
| **Retries** | 3 attempts, exponential backoff starting at 300 ms, for `429`, `5xx`, and network timeouts only. Never retry `400`/`404`/`409`. |
| **Timeouts** | Connect 5 s, read 60 s. |
| **Isolation** | A failure on one series must not stop the job; mark that Task `FAILED` and continue. |
| **Idempotency** | Re-running a job for the same window must produce no duplicates (LedgerPoint upserts; the service's diff skips unchanged values). |
| **Partial months** | Persist synced values only after LedgerPoint returns `200` for that chunk. |
| **Secrets** | Credentials/API keys come from environment variables — never committed, never logged. |
| **Logging** | Include `jobId`, `seriesName`, and `X-Correlation-Id` on every log line. Do not log tokens or full payloads. |

### 8.1 Failure reason codes

| Code | Description |
|---|---|
| `SOURCE_UNAVAILABLE` | Orion retries exhausted |
| `NO_DATA` | Orion returned no values in window |
| `STALE_SERIES` | Staleness check failed |
| `UNSUPPORTED_GRANULARITY` | Granularity not in {15, 60, 1440} |
| `UNSUPPORTED_TIMEZONE` | Unknown timezone code |
| `TARGET_REJECTED` | LedgerPoint returned 400 |
| `UNKNOWN_SERIES` | LedgerPoint returned 409 |
| `TARGET_UNAVAILABLE` | LedgerPoint retries exhausted |

---

## 9. Suggested Persistence (minimal)

```
job
  id               BIGINT PK
  series_type      VARCHAR(20)
  status           VARCHAR(30)     -- RUNNING | COMPLETED | COMPLETED_WITH_FAILURES | FAILED
  started_at       DATETIME2
  finished_at      DATETIME2 NULL

job_task
  id               BIGINT PK
  job_id           BIGINT FK -> job.id
  series_name      VARCHAR(100)
  status           VARCHAR(30)     -- PENDING | SUCCESS | SUCCESS_WITH_WARNINGS | SKIPPED | FAILED
  failure_reason   VARCHAR(50) NULL
  failure_detail   VARCHAR(2000) NULL
  values_sent      INT

series
  id               BIGINT PK
  name             VARCHAR(100) UNIQUE
  series_type      VARCHAR(20)
  unit             VARCHAR(20)
  time_zone        INT
  granularity      INT
  last_synced_date DATE NULL

series_value
  series_id        BIGINT FK -> series.id
  start_instant    DATETIME2       -- UTC
  repeated_hour    BIT
  value            DECIMAL(19,6)
  synced_at        DATETIME2
  PRIMARY KEY (series_id, start_instant, repeated_hour)
```

Use a migration tool (e.g. Liquibase/Flyway). Use an in-memory DB (e.g. H2) for tests.

---

## 10. Out of Scope / Stretch Goals

- **Grouped datasets** — `GET /v1/datasets/{datasetName}/values?start_prevailing&end_prevailing` returning `{ datasetName, category, ownerName, granularity, timeZone, asOfDate, values[{startDate, repeatedHour, value}] }`.
- **Tiered datasets** — `GET /v1/tiered-datasets/{name}/values?...` returning `{ name, ownerName, timeZone, asOfDate, tiers[{ tierName, tierType, granularity, values[{startDate, repeatedHour, value}] }] }`.
- CSV/file-drop output as an alternative to the LedgerPoint POST.
- Holiday-calendar awareness (skip syncs on configured holidays).
- Scheduling (cron) — for the POC, a manual REST trigger is sufficient.

---

## 11. Acceptance Criteria

The POC is complete when, using **mocked** Orion and LedgerPoint (e.g. WireMock / MockWebServer):

1. `POST /api/series/typeA/sync` syncs every series returned by Orion and returns correct `taskCounts`.
2. A series with a window spanning 3 months results in exactly **3** Orion value calls.
3. Hourly data on a **fall-back** day produces 25 LedgerPoint rows, including one `"Nx"` interval.
4. Hourly data on a **spring-forward** day produces 24 LedgerPoint rows, with the skipped hour filled from the previous value and no `x` suffix.
5. 15-minute data produces intervals `1..96`; daily data produces interval `1`.
6. Re-running the same sync sends **0** values to LedgerPoint (all unchanged).
7. Changing one value in Orion and re-running sends exactly **1** value.
8. Orion `429` → service waits and retries; succeeds on retry.
9. Orion `401` mid-run → token refreshed, call retried once, job continues.
10. LedgerPoint `400` on one series → that Task is `FAILED` with `TARGET_REJECTED`; other series still succeed.
11. Series with a stale `lastUpdatedDate` → `FAILED` / `STALE_SERIES`, nothing posted.
12. Values are posted as plain decimal strings (no scientific notation).
13. No credentials appear in logs or in the repository.

---

## 12. Sample Test Fixtures

**Spring-forward day (`TZ_C`, 2026-03-08), hourly — Orion returns 23 values:**
```json
{ "startDate": "3/8/2026 1:00:00 AM", "repeatedHour": false, "value": 22.10 },
{ "startDate": "3/8/2026 3:00:00 AM", "repeatedHour": false, "value": 23.40 }
```
**Expected LedgerPoint rows:**
```json
{ "date": "2026-03-08", "interval": "2", "key": "SERIES_ALPHA_001", "value": "22.10" },
{ "date": "2026-03-08", "interval": "3", "key": "SERIES_ALPHA_001", "value": "22.10" },
{ "date": "2026-03-08", "interval": "4", "key": "SERIES_ALPHA_001", "value": "23.40" }
```

**Fall-back day (`TZ_C`, 2026-11-01), hourly — Orion returns 25 values:**
```json
{ "startDate": "11/1/2026 1:00:00 AM", "repeatedHour": false, "value": 29.80 },
{ "startDate": "11/1/2026 1:00:00 AM", "repeatedHour": true,  "value": 29.10 }
```
**Expected LedgerPoint rows:**
```json
{ "date": "2026-11-01", "interval": "2",  "key": "SERIES_ALPHA_001", "value": "29.80" },
{ "date": "2026-11-01", "interval": "2x", "key": "SERIES_ALPHA_001", "value": "29.10" }
```

---

*All system names, URLs, series names, units, and values in this document are illustrative and do not represent any real system.*
