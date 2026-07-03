package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.Money;
import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: PaymentCompletedEvent
 *
 * Raised when the PaymentActivity successfully processes payment.
 * Captures the transaction ID from the payment provider for audit/reconciliation.
 */
public record PaymentCompletedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String transactionId,
        Money amountCharged
) implements DomainEvent {

    @JsonCreator
    public static PaymentCompletedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("amountCharged") Money amountCharged) {
        return new PaymentCompletedEvent(eventId, aggregateId, version, occurredAt, transactionId, amountCharged);
    }

    public static PaymentCompletedEvent create(OrderId orderId, String transactionId, Money amount, long version) {
        return new PaymentCompletedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), transactionId, amount);
    }
}
