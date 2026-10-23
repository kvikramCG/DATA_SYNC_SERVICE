-- Step 4: indexed lookup from a correlation id (found in a log line or a LedgerPoint support
-- ticket) to its job. job.correlation_id already exists (V1); only the index is new.
--
-- Deliberately NOT unique: before correlation ids became server-generated, a client could send the
-- same X-Correlation-Id on several requests, so existing rows may contain duplicates and a unique
-- index would fail this migration on an already-populated database.
CREATE INDEX ix_job_correlation_id ON job (correlation_id);
