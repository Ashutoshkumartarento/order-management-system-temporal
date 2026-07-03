package com.example.ordermanagement.api.dto.response;

import java.time.Instant;
import java.util.UUID;

/**
 * REST Response DTO for individual domain events.
 * Exposes the raw event structure for audit/debugging.
 */
public record EventHistoryResponse(
        UUID eventId,
        String aggregateId,
        String eventType,
        long version,
        Instant occurredAt,
        Object payload   // Raw event — serialized as JSON
) {}
