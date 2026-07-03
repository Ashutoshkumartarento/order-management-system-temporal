package com.example.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message envelope for payment-service domain events.
 *
 * Published to topic: payment.events
 * Key: orderId (guarantees per-order ordering within a partition)
 *
 * Consumed by:
 *   - notification-service-group (payment receipts, failure alerts)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PaymentEventMessage.PaymentChargedMessage.class,  name = "PaymentCharged"),
    @JsonSubTypes.Type(value = PaymentEventMessage.PaymentFailedMessage.class,   name = "PaymentFailed"),
    @JsonSubTypes.Type(value = PaymentEventMessage.PaymentRefundedMessage.class, name = "PaymentRefunded"),
})
public sealed interface PaymentEventMessage permits
        PaymentEventMessage.PaymentChargedMessage,
        PaymentEventMessage.PaymentFailedMessage,
        PaymentEventMessage.PaymentRefundedMessage {

    UUID eventId();
    String orderId();
    Instant occurredAt();

    record PaymentChargedMessage(
            UUID eventId,
            String orderId,
            String transactionId,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) implements PaymentEventMessage {}

    record PaymentFailedMessage(
            UUID eventId,
            String orderId,
            String reason,
            boolean retryable,
            Instant occurredAt
    ) implements PaymentEventMessage {}

    record PaymentRefundedMessage(
            UUID eventId,
            String orderId,
            String originalTransactionId,
            String refundTransactionId,
            BigDecimal amount,
            Instant occurredAt
    ) implements PaymentEventMessage {}
}
