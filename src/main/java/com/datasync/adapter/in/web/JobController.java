package com.datasync.adapter.in.web;

import com.datasync.domain.port.in.GetJobUseCase;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inbound adapter for {@code GET /api/jobs/{jobId}} (spec &sect;7.2).
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final GetJobUseCase getJobUseCase;

    public JobController(GetJobUseCase getJobUseCase) {
        this.getJobUseCase = getJobUseCase;
    }

    @GetMapping("/{jobId}")
    public JobDetailResponse getJob(@PathVariable Long jobId) {
        return JobDetailResponse.from(getJobUseCase.getJob(jobId));
    }
}
