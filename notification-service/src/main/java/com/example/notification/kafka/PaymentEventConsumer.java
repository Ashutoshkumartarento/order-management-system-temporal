package com.example.notification.kafka;

import com.example.contracts.kafka.PaymentEventMessage;
import com.example.notification.service.EventIdempotencyService;
import com.example.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * PaymentEventConsumer — Idempotent Kafka consumer for payment.events
 *
 * Handles direct payment events from payment-service, most importantly
 * PaymentRefundedMessage which is not republished on order.events.
 * Deduplicates by eventId using Redis (TTL=24h) via EventIdempotencyService.
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final NotificationService     notificationService;
    private final EventIdempotencyService idempotencyService;

    public PaymentEventConsumer(NotificationService notificationService,
                                EventIdempotencyService idempotencyService) {
        this.notificationService = notificationService;
        this.idempotencyService  = idempotencyService;
    }

    @KafkaListener(
            topics = "payment.events",
            groupId = "notification-service-group",
            containerFactory = "paymentKafkaListenerContainerFactory")
    public void onPaymentEvent(PaymentEventMessage message) {
        if (!idempotencyService.isFirstOccurrence(message.eventId())) {
            return;
        }

        try {
            switch (message) {
                case PaymentEventMessage.PaymentChargedMessage m ->
                    notificationService.notifyPaymentCharged(m);

                case PaymentEventMessage.PaymentFailedMessage m ->
                    notificationService.notifyPaymentChargeFailed(m);

                case PaymentEventMessage.PaymentRefundedMessage m ->
                    notificationService.notifyPaymentRefunded(m);
            }
        } catch (Exception e) {
            // Revert the idempotency mark so the retry attempt is not skipped.
            // The container's DefaultErrorHandler will retry (FixedBackOff: 2 retries, 2s apart).
            // After all retries are exhausted, DeadLetterPublishingRecoverer routes
            // the message to payment.events.DLT for manual investigation.
            idempotencyService.deleteOccurrence(message.eventId());
            log.error("Failed to process {} for event {} — reverting idempotency mark, will retry/DLQ",
                    message.getClass().getSimpleName(), message.eventId(), e);
            throw e;
        }
    }
}