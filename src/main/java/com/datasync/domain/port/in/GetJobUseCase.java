package com.datasync.domain.port.in;

import com.datasync.domain.model.JobDetail;

/**
 * Inbound port for {@code GET /api/jobs/{jobId}} (spec &sect;7.2).
 */
public interface GetJobUseCase {

    JobDetail getJob(Long jobId);
}
