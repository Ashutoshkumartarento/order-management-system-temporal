package com.example.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message envelope for all order-related domain events.
 *
 * Published to topic: order.events
 * Key: orderId (guarantees ordering per order within a partition)
 *
 * WHY AN ENVELOPE?
 * We wrap the domain event payload inside this message so that
 * downstream consumers (notification-service etc.) can read the
 * type discriminator without needing to know the full domain model.
 *
 * The 'payload' field contains the JSON of the specific event.
 * Consumers deserialize based on 'eventType'.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderEventMessage.OrderCreatedMessage.class,         name = "OrderCreated"),
    @JsonSubTypes.Type(value = OrderEventMessage.OrderConfirmedMessage.class,       name = "OrderConfirmed"),
    @JsonSubTypes.Type(value = OrderEventMessage.OrderCancelledMessage.class,       name = "OrderCancelled"),
    @JsonSubTypes.Type(value = OrderEventMessage.PaymentCompletedMessage.class,     name = "PaymentCompleted"),
    @JsonSubTypes.Type(value = OrderEventMessage.PaymentFailedMessage.class,        name = "PaymentFailed"),
    @JsonSubTypes.Type(value = OrderEventMessage.ShipmentCreatedMessage.class,      name = "ShipmentCreated"),
    @JsonSubTypes.Type(value = OrderEventMessage.ShipmentDeliveredMessage.class,    name = "ShipmentDelivered"),
    @JsonSubTypes.Type(value = OrderEventMessage.InventoryReservedMessage.class,    name = "InventoryReserved"),
    @JsonSubTypes.Type(value = OrderEventMessage.InventoryReleasedMessage.class,    name = "InventoryReleased"),
    @JsonSubTypes.Type(value = OrderEventMessage.ItemAddedMessage.class,            name = "ItemAdded"),
    @JsonSubTypes.Type(value = OrderEventMessage.ItemRemovedMessage.class,          name = "ItemRemoved"),
    @JsonSubTypes.Type(value = OrderEventMessage.RefundCompletedMessage.class,      name = "RefundCompleted"),
})
public sealed interface OrderEventMessage permits
        OrderEventMessage.OrderCreatedMessage,
        OrderEventMessage.OrderConfirmedMessage,
        OrderEventMessage.OrderCancelledMessage,
        OrderEventMessage.PaymentCompletedMessage,
        OrderEventMessage.PaymentFailedMessage,
        OrderEventMessage.ShipmentCreatedMessage,
        OrderEventMessage.ShipmentDeliveredMessage,
        OrderEventMessage.InventoryReservedMessage,
        OrderEventMessage.InventoryReleasedMessage,
        OrderEventMessage.ItemAddedMessage,
        OrderEventMessage.ItemRemovedMessage,
        OrderEventMessage.RefundCompletedMessage {

    UUID eventId();
    String orderId();
    Instant occurredAt();

    record OrderCreatedMessage(UUID eventId, String orderId, String customerId,
                               String shippingAddress, Instant occurredAt)
            implements OrderEventMessage {}

    record OrderConfirmedMessage(UUID eventId, String orderId, String customerId,
                                 double totalAmount, String workflowId, Instant occurredAt)
            implements OrderEventMessage {}

    record OrderCancelledMessage(UUID eventId, String orderId, String reason,
                                 String cancelledBy, Instant occurredAt)
            implements OrderEventMessage {}

    record PaymentCompletedMessage(UUID eventId, String orderId, String transactionId,
                                   double amount, Instant occurredAt)
            implements OrderEventMessage {}

    record PaymentFailedMessage(UUID eventId, String orderId, String reason,
                                boolean retryable, Instant occurredAt)
            implements OrderEventMessage {}

    record ShipmentCreatedMessage(UUID eventId, String orderId, String shipmentId,
                                  String trackingNumber, String carrier, Instant occurredAt)
            implements OrderEventMessage {}

    record ShipmentDeliveredMessage(UUID eventId, String orderId, String shipmentId,
                                    Instant deliveredAt, Instant occurredAt)
            implements OrderEventMessage {}

    record InventoryReservedMessage(UUID eventId, String orderId, String reservationId,
                                    Instant occurredAt)
            implements OrderEventMessage {}

    record InventoryReleasedMessage(UUID eventId, String orderId, String reservationId,
                                    String reason, Instant occurredAt)
            implements OrderEventMessage {}

    record ItemAddedMessage(UUID eventId, String orderId, Instant occurredAt)
            implements OrderEventMessage {}

    record ItemRemovedMessage(UUID eventId, String orderId, Instant occurredAt)
            implements OrderEventMessage {}

    record RefundCompletedMessage(UUID eventId, String orderId, Instant occurredAt)
            implements OrderEventMessage {}
}
