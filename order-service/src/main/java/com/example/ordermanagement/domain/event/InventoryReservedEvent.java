package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: InventoryReservedEvent
 *
 * Raised by the application service after the InventoryActivity
 * in the Temporal workflow succeeds.
 *
 * TEMPORAL <-> EVENT SOURCING BRIDGE:
 * Temporal orchestrates the reservation, but the domain state change
 * (order moving to INVENTORY_RESERVED) is recorded here as a domain event.
 * This separates orchestration concerns (Temporal) from state concerns (Event Store).
 */
public record InventoryReservedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String reservationId
) implements DomainEvent {

    @JsonCreator
    public static InventoryReservedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("reservationId") String reservationId) {
        return new InventoryReservedEvent(eventId, aggregateId, version, occurredAt, reservationId);
    }

    public static InventoryReservedEvent create(OrderId orderId, String reservationId, long version) {
        return new InventoryReservedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), reservationId);
    }
}
