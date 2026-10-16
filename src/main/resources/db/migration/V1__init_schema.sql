-- Core schema for the Data Sync Service (see database-design.md for full rationale)

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE job (
    id                      BIGSERIAL PRIMARY KEY,
    series_type             VARCHAR(20) NOT NULL,
    status                  VARCHAR(30) NOT NULL,
    requested_series_name   VARCHAR(100),
    requested_start_date    DATE,
    requested_end_date      DATE,
    started_at              TIMESTAMPTZ NOT NULL,
    finished_at             TIMESTAMPTZ,
    correlation_id          UUID NOT NULL
);

CREATE UNIQUE INDEX uq_job_running_per_type ON job (series_type) WHERE status = 'RUNNING';
CREATE INDEX ix_job_series_type_started_at ON job (series_type, started_at DESC);

CREATE TABLE series (
    id                  BIGSERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL UNIQUE,
    series_type         VARCHAR(20) NOT NULL,
    unit                VARCHAR(20),
    time_zone_code      INT NOT NULL,
    earliest_data_date  DATE,
    latest_data_date    DATE,
    last_updated_date   TIMESTAMPTZ,
    last_synced_date    DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE job_task (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES job (id),
    series_id       BIGINT NOT NULL REFERENCES series (id),
    status          VARCHAR(30) NOT NULL,
    failure_reason  VARCHAR(50),
    failure_detail  VARCHAR(2000),
    values_sent     INT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);

CREATE INDEX ix_job_task_job_id ON job_task (job_id);
CREATE INDEX ix_job_task_series_status ON job_task (series_id, status);

CREATE TABLE job_task_chunk (
    id                      BIGSERIAL PRIMARY KEY,
    job_task_id             BIGINT NOT NULL REFERENCES job_task (id),
    chunk_start_date        DATE NOT NULL,
    chunk_end_date          DATE NOT NULL,
    orion_call_count        INT NOT NULL DEFAULT 0,
    status                  VARCHAR(30) NOT NULL,
    values_fetched          INT NOT NULL DEFAULT 0,
    values_new              INT NOT NULL DEFAULT 0,
    values_updated          INT NOT NULL DEFAULT 0,
    values_unchanged        INT NOT NULL DEFAULT 0,
    ledgerpoint_batch_id    UUID
);

CREATE UNIQUE INDEX uq_chunk_task_start ON job_task_chunk (job_task_id, chunk_start_date);

CREATE TABLE task_warning (
    id                  BIGSERIAL PRIMARY KEY,
    job_task_chunk_id   BIGINT NOT NULL REFERENCES job_task_chunk (id),
    warning_type        VARCHAR(50) NOT NULL,
    interval_date        DATE,
    interval_number      VARCHAR(5),
    detail               VARCHAR(500)
);

CREATE INDEX ix_task_warning_chunk ON task_warning (job_task_chunk_id);

CREATE TABLE series_rule_override (
    id                          BIGSERIAL PRIMARY KEY,
    orion_series_name           VARCHAR(100) NOT NULL UNIQUE,
    ledgerpoint_key_override     VARCHAR(100),
    skip                         BOOLEAN NOT NULL DEFAULT FALSE,
    active                       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE series_value (
    series_id       BIGINT NOT NULL REFERENCES series (id),
    start_instant   TIMESTAMPTZ NOT NULL,
    repeated_hour   BOOLEAN NOT NULL DEFAULT FALSE,
    value           DECIMAL(19, 6) NOT NULL,
    interval_number VARCHAR(5) NOT NULL,
    local_date      DATE NOT NULL,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, start_instant, repeated_hour)
);

SELECT create_hypertable('series_value', 'start_instant', chunk_time_interval => INTERVAL '1 month');

CREATE INDEX ix_series_value_series_localdate ON series_value (series_id, local_date);
