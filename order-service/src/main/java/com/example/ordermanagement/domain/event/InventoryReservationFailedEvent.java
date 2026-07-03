package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: InventoryReservationFailedEvent
 *
 * Raised when inventory reservation fails after all Temporal retries are exhausted.
 * This event triggers the compensation path in the saga:
 *   → OrderCancelled event follows
 *
 * WHY RECORD FAILURES AS EVENTS?
 * In traditional CRUD, a failure just means "nothing happened".
 * In Event Sourcing, failures are also facts. Knowing THAT a reservation failed,
 * WHEN it failed, and WHY (reason field) is valuable audit information.
 * It also drives state — after this event, the order cannot proceed.
 */
public record InventoryReservationFailedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String reason
) implements DomainEvent {

    @JsonCreator
    public static InventoryReservationFailedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("reason") String reason) {
        return new InventoryReservationFailedEvent(eventId, aggregateId, version, occurredAt, reason);
    }

    public static InventoryReservationFailedEvent create(OrderId orderId, String reason, long version) {
        return new InventoryReservationFailedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), reason);
    }
}
