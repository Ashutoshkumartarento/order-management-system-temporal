package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: ShipmentCreatedEvent
 *
 * Raised when the ShippingActivity creates a shipment with the carrier.
 * The trackingNumber allows the customer to track the package.
 */
public record ShipmentCreatedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String shipmentId,
        String trackingNumber,
        String carrier
) implements DomainEvent {

    @JsonCreator
    public static ShipmentCreatedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("shipmentId") String shipmentId,
            @JsonProperty("trackingNumber") String trackingNumber,
            @JsonProperty("carrier") String carrier) {
        return new ShipmentCreatedEvent(eventId, aggregateId, version, occurredAt, shipmentId, trackingNumber, carrier);
    }

    public static ShipmentCreatedEvent create(OrderId orderId, String shipmentId, String trackingNumber, String carrier, long version) {
        return new ShipmentCreatedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), shipmentId, trackingNumber, carrier);
    }
}
