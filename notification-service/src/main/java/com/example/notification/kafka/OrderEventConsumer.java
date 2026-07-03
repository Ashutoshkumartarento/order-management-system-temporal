package com.example.notification.kafka;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.notification.service.EventIdempotencyService;
import com.example.notification.service.NotificationService;
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

                case OrderEventMessage.InventoryReservedMessage m -> {
                    // Inventory reserved notification
                }

                case OrderEventMessage.InventoryReleasedMessage m -> {
                    // Inventory released notification
                }
            }
        } catch (Exception e) {
            // Don't rethrow — mark as processed to avoid infinite retry loop
            // In production: send to DLQ for manual investigation
        }
    }
}
