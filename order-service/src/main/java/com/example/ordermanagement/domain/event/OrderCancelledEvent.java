package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: OrderCancelledEvent
 *
 * Terminal event — once appended, the order is in CANCELLED state.
 * This can result from:
 *   1. Customer sending a CancelOrder signal to Temporal
 *   2. Saga compensation after inventory/payment/shipping failure
 *
 * The 'cancellationReason' distinguishes these cases, which is important
 * for metrics (how many orders cancelled by customers vs system failures).
 */
public record OrderCancelledEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String cancellationReason,
        String cancelledBy  // "CUSTOMER" | "SYSTEM_COMPENSATION"
) implements DomainEvent {

    @JsonCreator
    public static OrderCancelledEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("cancellationReason") String cancellationReason,
            @JsonProperty("cancelledBy") String cancelledBy) {
        return new OrderCancelledEvent(eventId, aggregateId, version, occurredAt, cancellationReason, cancelledBy);
    }

    public static OrderCancelledEvent create(OrderId orderId, String reason, String cancelledBy, long version) {
        return new OrderCancelledEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), reason, cancelledBy);
    }
}
