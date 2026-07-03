package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.Money;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: RefundCompletedEvent
 *
 * Raised during the shipment-failure compensation path.
 * If shipment fails after payment was taken, we must refund the customer.
 * Temporal calls the RefundPayment activity, and on success this event is appended.
 */
public record RefundCompletedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String refundTransactionId,
        Money amountRefunded
) implements DomainEvent {

    @JsonCreator
    public static RefundCompletedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("refundTransactionId") String refundTransactionId,
            @JsonProperty("amountRefunded") Money amountRefunded) {
        return new RefundCompletedEvent(eventId, aggregateId, version, occurredAt, refundTransactionId, amountRefunded);
    }

    public static RefundCompletedEvent create(OrderId orderId, String refundTxId, Money amount, long version) {
        return new RefundCompletedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), refundTxId, amount);
    }
}
