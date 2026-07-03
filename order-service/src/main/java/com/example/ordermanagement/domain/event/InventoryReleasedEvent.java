package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: InventoryReleasedEvent
 *
 * Raised during compensation when previously reserved inventory is released.
 * This is the "undo" step in the Saga pattern.
 *
 * SAGA COMPENSATION:
 * If payment fails after inventory was reserved, we must release the inventory.
 * Temporal calls the ReleaseInventory activity, and on success this event is appended.
 * The order then proceeds to CANCELLED state.
 */
public record InventoryReleasedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String reservationId,
        String reason
) implements DomainEvent {

    @JsonCreator
    public static InventoryReleasedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("reservationId") String reservationId,
            @JsonProperty("reason") String reason) {
        return new InventoryReleasedEvent(eventId, aggregateId, version, occurredAt, reservationId, reason);
    }

    public static InventoryReleasedEvent create(OrderId orderId, String reservationId, String reason, long version) {
        return new InventoryReleasedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), reservationId, reason);
    }
}
