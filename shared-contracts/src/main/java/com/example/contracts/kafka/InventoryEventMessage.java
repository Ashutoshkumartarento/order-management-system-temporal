package com.example.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message envelope for inventory-service domain events.
 *
 * Published to topic: inventory.events
 * Key: orderId (guarantees per-order ordering within a partition)
 *
 * Consumed by:
 *   - notification-service-group (alerts on reservation failure)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = InventoryEventMessage.InventoryReservedMessage.class,           name = "InventoryReserved"),
    @JsonSubTypes.Type(value = InventoryEventMessage.InventoryReservationFailedMessage.class,  name = "InventoryReservationFailed"),
    @JsonSubTypes.Type(value = InventoryEventMessage.InventoryReleasedMessage.class,           name = "InventoryReleased"),
})
public sealed interface InventoryEventMessage permits
        InventoryEventMessage.InventoryReservedMessage,
        InventoryEventMessage.InventoryReservationFailedMessage,
        InventoryEventMessage.InventoryReleasedMessage {

    UUID eventId();
    String orderId();
    Instant occurredAt();

    record InventoryReservedMessage(
            UUID eventId,
            String orderId,
            String reservationId,
            Instant occurredAt
    ) implements InventoryEventMessage {}

    record InventoryReservationFailedMessage(
            UUID eventId,
            String orderId,
            String reason,
            Instant occurredAt
    ) implements InventoryEventMessage {}

    record InventoryReleasedMessage(
            UUID eventId,
            String orderId,
            String reservationId,
            String reason,
            Instant occurredAt
    ) implements InventoryEventMessage {}
}
