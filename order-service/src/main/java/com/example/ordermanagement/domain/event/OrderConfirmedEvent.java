package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.Money;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: OrderConfirmedEvent
 *
 * Raised when the customer confirms the order.
 * At this point:
 * - No more item modifications are allowed
 * - The Temporal OrderFulfillmentWorkflow is started
 * - The total amount is captured as a snapshot
 *
 * WHY CAPTURE TOTAL HERE?
 * While we CAN always recalculate the total by replaying all ItemAdded/Removed events,
 * capturing it at confirmation provides a quick reference without full replay.
 * This is a pragmatic denormalization — the canonical source of truth is still the events.
 */
public record OrderConfirmedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        Money totalAmount,
        String workflowId
) implements DomainEvent {

    @JsonCreator
    public static OrderConfirmedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("totalAmount") Money totalAmount,
            @JsonProperty("workflowId") String workflowId) {
        return new OrderConfirmedEvent(eventId, aggregateId, version, occurredAt, totalAmount, workflowId);
    }

    public static OrderConfirmedEvent create(OrderId orderId, Money totalAmount, String workflowId, long version) {
        return new OrderConfirmedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), totalAmount, workflowId);
    }
}
