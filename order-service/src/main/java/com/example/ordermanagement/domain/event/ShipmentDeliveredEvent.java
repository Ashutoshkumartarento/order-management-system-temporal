package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: ShipmentDeliveredEvent
 *
 * Raised when delivery is confirmed.
 * In a real system this could come from a webhook callback from the carrier.
 * Here the Temporal ShippingActivity simulates it.
 */
public record ShipmentDeliveredEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String shipmentId,
        Instant deliveredAt
) implements DomainEvent {

    @JsonCreator
    public static ShipmentDeliveredEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("shipmentId") String shipmentId,
            @JsonProperty("deliveredAt") Instant deliveredAt) {
        return new ShipmentDeliveredEvent(eventId, aggregateId, version, occurredAt, shipmentId, deliveredAt);
    }

    public static ShipmentDeliveredEvent create(OrderId orderId, String shipmentId, long version) {
        return new ShipmentDeliveredEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), shipmentId, Instant.now());
    }
}
