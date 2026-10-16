package com.datasync.application;

import com.datasync.domain.model.TaskStatus;

/**
 * Internal result of {@link SeriesSyncWorker#process}. {@code jobShouldFail} is only ever
 * {@code true} for a LedgerPoint auth failure (401/403) - spec &sect;8 requires that to fail the
 * whole Job immediately, unlike every other failure type which only fails its own Task.
 */
record TaskOutcome(TaskStatus status, boolean jobShouldFail) {
}
