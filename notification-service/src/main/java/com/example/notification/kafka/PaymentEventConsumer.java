package com.example.notification.kafka;

import com.example.contracts.kafka.PaymentEventMessage;
import com.example.notification.service.EventIdempotencyService;
import com.example.notification.service.NotificationService;
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
            // Don't rethrow — mark as processed to avoid infinite retry loop
            // In production: send to DLQ for manual investigation
        }
    }
}