package com.example.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message envelope for shipping-service domain events.
 *
 * Published to topic: shipping.events
 * Key: orderId (guarantees per-order ordering within a partition)
 *
 * Consumed by:
 *   - notification-service-group (shipment tracking, delivery confirmation)
 *   - order-service-group        (delivery confirmation → closes the saga)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ShippingEventMessage.ShipmentCreatedMessage.class,   name = "ShipmentCreated"),
    @JsonSubTypes.Type(value = ShippingEventMessage.ShipmentDeliveredMessage.class, name = "ShipmentDelivered"),
})
public sealed interface ShippingEventMessage permits
        ShippingEventMessage.ShipmentCreatedMessage,
        ShippingEventMessage.ShipmentDeliveredMessage {

    UUID eventId();
    String orderId();
    Instant occurredAt();

    record ShipmentCreatedMessage(
            UUID eventId,
            String orderId,
            String shipmentId,
            String trackingNumber,
            String carrier,
            Instant occurredAt
    ) implements ShippingEventMessage {}

    record ShipmentDeliveredMessage(
            UUID eventId,
            String orderId,
            String shipmentId,
            Instant deliveredAt,
            Instant occurredAt
    ) implements ShippingEventMessage {}
}
