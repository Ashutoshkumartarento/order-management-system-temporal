package com.example.ordermanagement.domain.event;

import com.example.ordermanagement.domain.valueobject.OrderId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Event: PaymentFailedEvent
 *
 * Raised after all payment retries are exhausted.
 * Triggers compensation: release inventory → cancel order.
 *
 * The 'retryable' flag indicates whether a RetryPayment signal
 * from the customer could restart the payment attempt.
 */
public record PaymentFailedEvent(
        UUID eventId,
        String aggregateId,
        long version,
        Instant occurredAt,
        String reason,
        boolean retryable
) implements DomainEvent {

    @JsonCreator
    public static PaymentFailedEvent of(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("version") long version,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("reason") String reason,
            @JsonProperty("retryable") boolean retryable) {
        return new PaymentFailedEvent(eventId, aggregateId, version, occurredAt, reason, retryable);
    }

    public static PaymentFailedEvent create(OrderId orderId, String reason, boolean retryable, long version) {
        return new PaymentFailedEvent(UUID.randomUUID(), orderId.toString(), version, Instant.now(), reason, retryable);
    }
}
