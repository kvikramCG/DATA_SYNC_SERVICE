package com.datasync.adapter.out.ledgerpoint;

import com.datasync.adapter.out.ledgerpoint.dto.LedgerPointBatchResponseDto;
import com.datasync.adapter.out.ledgerpoint.dto.LedgerPointErrorResponseDto;
import com.datasync.adapter.out.ledgerpoint.dto.LedgerPointValueRowDto;
import com.datasync.domain.exception.LedgerPointAuthException;
import com.datasync.domain.exception.LedgerPointTransientException;
import com.datasync.domain.exception.LedgerPointUnknownSeriesException;
import com.datasync.domain.exception.LedgerPointValidationException;
import com.datasync.domain.model.ClassifiedSeriesValue;
import com.datasync.domain.model.LedgerPointPublishResult;
import com.datasync.domain.model.LedgerPointValidationError;
import com.datasync.domain.model.NormalizedSeriesValue;
import com.datasync.domain.port.out.SeriesPublisherPort;
import com.datasync.domain.service.BatchLabelBuilder;
import com.datasync.domain.service.BatchSplitter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Outbound adapter implementing {@link SeriesPublisherPort} against the LedgerPoint REST API
 * (spec &sect;5). Splits oversized payloads (&gt;10,000 rows) into multiple batches and retries
 * only transient (5xx/network) failures - never 400/409/401/403 (spec &sect;8).
 */
@Component
public class LedgerPointClientAdapter implements SeriesPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(LedgerPointClientAdapter.class);

    private final RestClient ledgerPointRestClient;
    private final LedgerPointProperties properties;
    private final ObjectMapper objectMapper;
    private final Retry retry;

    public LedgerPointClientAdapter(RestClient ledgerPointRestClient,
                                     LedgerPointProperties properties,
                                     ObjectMapper objectMapper,
                                     RetryRegistry retryRegistry) {
        this.ledgerPointRestClient = ledgerPointRestClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.retry = retryRegistry.retry("ledgerpoint");
    }

    @Override
    public LedgerPointPublishResult publish(String key, String unitCode, String timeZoneCode,
                                             List<ClassifiedSeriesValue> values, UUID correlationId) {
        List<List<ClassifiedSeriesValue>> batches = BatchSplitter.split(values, properties.getMaxBatchSize());

        int totalReceived = 0;
        int totalInserted = 0;
        int totalUpdated = 0;
        List<UUID> batchIds = new ArrayList<>();

        for (List<ClassifiedSeriesValue> batch : batches) {
            LedgerPointBatchResponseDto response = publishBatchWithRetry(key, unitCode, timeZoneCode, batch, correlationId);
            totalReceived += response.received();
            totalInserted += response.inserted();
            totalUpdated += response.updated();
            if (response.batchId() != null) {
                batchIds.add(UUID.fromString(response.batchId()));
            }
        }
        return new LedgerPointPublishResult(totalReceived, totalInserted, totalUpdated, batchIds);
    }

    private LedgerPointBatchResponseDto publishBatchWithRetry(String key, String unitCode, String timeZoneCode,
                                                               List<ClassifiedSeriesValue> batch, UUID correlationId) {
        Supplier<LedgerPointBatchResponseDto> decorated =
                Decorators.ofSupplier(() -> publishBatch(key, unitCode, timeZoneCode, batch, correlationId))
                        .withRetry(retry)
                        .decorate();
        return decorated.get();
    }

    private LedgerPointBatchResponseDto publishBatch(String key, String unitCode, String timeZoneCode,
                                                      List<ClassifiedSeriesValue> batch, UUID correlationId) {
        List<LedgerPointValueRowDto> rows = batch.stream()
                .map(classified -> toRow(key, classified.value()))
                .toList();
        String label = BatchLabelBuilder.build(
                batch.stream().map(ClassifiedSeriesValue::changeType).toList(), key, unitCode, timeZoneCode);
        log.debug("Publishing LedgerPoint batch {} ({} rows)", label, rows.size());

        try {
            return ledgerPointRestClient.post()
                    .uri("/api/v2/series/values")
                    .header("X-Api-Key", properties.getApiKey())
                    .header("X-Correlation-Id", correlationId.toString())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(rows)
                    .retrieve()
                    .onStatus(status -> status.value() == 401 || status.value() == 403, (req, res) -> {
                        throw new LedgerPointAuthException("LedgerPoint authentication failed: " + res.getStatusCode());
                    })
                    .onStatus(status -> status.value() == 409, (req, res) -> {
                        throw new LedgerPointUnknownSeriesException("LedgerPoint series key not configured: " + key);
                    })
                    .onStatus(status -> status.value() == 400, (req, res) -> {
                        throw new LedgerPointValidationException("LedgerPoint rejected the batch", readErrors(res.getBody()));
                    })
                    .onStatus(HttpStatusCode::is5xxServerError, (req, res) -> {
                        throw new LedgerPointTransientException("LedgerPoint transient error: " + res.getStatusCode());
                    })
                    .body(LedgerPointBatchResponseDto.class);
        } catch (ResourceAccessException networkException) {
            throw new LedgerPointTransientException("LedgerPoint network error: " + networkException.getMessage());
        }
    }

    private List<LedgerPointValidationError> readErrors(java.io.InputStream body) {
        try {
            LedgerPointErrorResponseDto errorBody = objectMapper.readValue(body, LedgerPointErrorResponseDto.class);
            if (errorBody == null || errorBody.errors() == null) {
                return List.of();
            }
            return errorBody.errors().stream()
                    .map(e -> new LedgerPointValidationError(e.index(), e.field(), e.message()))
                    .toList();
        } catch (IOException e) {
            log.warn("Could not parse LedgerPoint 400 error body: {}", e.getMessage());
            return List.of();
        }
    }

    private LedgerPointValueRowDto toRow(String key, NormalizedSeriesValue value) {
        return new LedgerPointValueRowDto(
                value.localDate().toString(),
                value.intervalNumber(),
                key,
                value.value().toPlainString());
    }
}
