package com.datasync.adapter.in.web;

import com.datasync.domain.exception.JobAlreadyRunningException;
import com.datasync.domain.exception.JobNotFoundException;
import com.datasync.domain.exception.OrionAuthException;
import com.datasync.domain.exception.OrionClientException;
import com.datasync.domain.exception.OrionTransientException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps outbound-adapter and domain failures to RFC-7807 problem details at the REST boundary.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(JobAlreadyRunningException.class)
    public ProblemDetail handleJobAlreadyRunning(JobAlreadyRunningException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ProblemDetail handleJobNotFound(JobNotFoundException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(OrionAuthException.class)
    public ProblemDetail handleOrionAuth(OrionAuthException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, "Orion authentication failed");
    }

    @ExceptionHandler({OrionTransientException.class, OrionClientException.class})
    public ProblemDetail handleOrionFailure(RuntimeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, "Orion request failed: " + ex.getMessage());
    }

    @ExceptionHandler(RequestNotPermitted.class)
    public ProblemDetail handleRateLimited(RequestNotPermitted ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                "Orion rate limit exceeded, try again shortly");
    }
}
