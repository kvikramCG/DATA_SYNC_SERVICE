package com.datasync.domain.port.out;

import java.time.Instant;

/**
 * Outbound port abstracting the system clock, so adapters stay deterministically testable.
 */
public interface ClockPort {

    Instant now();
}
