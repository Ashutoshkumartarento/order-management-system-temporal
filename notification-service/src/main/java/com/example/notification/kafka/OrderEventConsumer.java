package com.example.notification.kafka;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.notification.service.EventIdempotencyService;
import com.example.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * OrderEventConsumer — Idempotent Kafka consumer for order.events
 *
 * Deduplicates by eventId using Redis (TTL=24h) via EventIdempotencyService.
 * Consumer group = notification-service-group — each message processed once per group.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final NotificationService       notificationService;
    private final EventIdempotencyService   idempotencyService;

    public OrderEventConsumer(NotificationService notificationService,
                              EventIdempotencyService idempotencyService) {
        this.notificationService = notificationService;
        this.idempotencyService  = idempotencyService;
    }

    @KafkaListener(
            topics = "order.events",
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEvent(OrderEventMessage message) {
        if (!idempotencyService.isFirstOccurrence(message.eventId())) {
            return;
        }

        try {
            switch (message) {
                case OrderEventMessage.OrderCreatedMessage m ->
                    notificationService.notifyOrderCreated(m);

                case OrderEventMessage.OrderConfirmedMessage m ->
                    notificationService.notifyOrderConfirmed(m);

                case OrderEventMessage.OrderCancelledMessage m ->
                    notificationService.notifyOrderCancelled(m);

                case OrderEventMessage.PaymentCompletedMessage m ->
                    notificationService.notifyPaymentCompleted(m);

                case OrderEventMessage.PaymentFailedMessage m ->
                    notificationService.notifyPaymentFailed(m);

                case OrderEventMessage.ShipmentCreatedMessage m ->
                    notificationService.notifyShipmentCreated(m);

                case OrderEventMessage.ShipmentDeliveredMessage m ->
                    notificationService.notifyShipmentDelivered(m);

                case OrderEventMessage.InventoryReservedMessage m -> {}
                case OrderEventMessage.InventoryReleasedMessage m -> {}
                case OrderEventMessage.ItemAddedMessage m -> {}
                case OrderEventMessage.ItemRemovedMessage m -> {}
                case OrderEventMessage.RefundCompletedMessage m -> {}
            }
        } catch (Exception e) {
            // Revert the idempotency mark so the retry attempt is not skipped.
            // The container's DefaultErrorHandler will retry (FixedBackOff: 2 retries, 2s apart).
            // After all retries are exhausted, DeadLetterPublishingRecoverer routes
            // the message to order.events.DLT for manual investigation.
            idempotencyService.deleteOccurrence(message.eventId());
            log.error("Failed to process {} for order {} — reverting idempotency mark, will retry/DLQ",
                    message.getClass().getSimpleName(), message.eventId(), e);
            throw e;
        }
    }
}
