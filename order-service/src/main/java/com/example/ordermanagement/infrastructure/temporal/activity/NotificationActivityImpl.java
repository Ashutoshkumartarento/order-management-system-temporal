package com.example.ordermanagement.infrastructure.temporal.activity;

import com.example.contracts.kafka.OrderEventMessage;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * NotificationActivityImpl — Publishes to Kafka notification.requests topic
 *
 * WHAT CHANGED FROM MONOLITH:
 * Before: System.out.println() statements only.
 * Now:    Publishes an OrderEventMessage to Kafka.
 *         notification-service consumes from order.events topic.
 *
 * We reuse the existing OrderEventMessage types where applicable,
 * so notification-service only needs to subscribe to order.events.
 *
 * Notifications are BEST-EFFORT:
 * - Activity has maxAttempts=2 (not 5 or 10)
 * - Failures do NOT abort the workflow
 * - A notification failing to publish doesn't cancel a delivered order
 */
@Component
@ActivityImpl(taskQueues = "ORDER_FULFILLMENT_QUEUE")
public class NotificationActivityImpl implements NotificationActivity {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NotificationActivityImpl.class);

    private final KafkaTemplate<String, OrderEventMessage> kafkaTemplate;
    private static final String TOPIC = "order.events";

    public NotificationActivityImpl(KafkaTemplate<String, OrderEventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendOrderConfirmedNotification(String orderId) {
        log.debug("sendOrderConfirmedNotification: order={} — no-op, OrderEventKafkaPublisher already published", orderId);
        // The OrderConfirmedMessage is already published by OrderEventKafkaPublisher
        // when the OrderConfirmedEvent is persisted. This is a no-op to avoid duplication.
    }

    @Override
    public void sendOrderShippedNotification(String orderId, String trackingNumber) {
        // ShipmentCreatedMessage already published via OrderEventKafkaPublisher
        log.debug("sendOrderShippedNotification: order={} tracking={} — no-op, already published via KafkaPublisher", orderId, trackingNumber);
    }

    @Override
    public void sendOrderDeliveredNotification(String orderId) {
        // ShipmentDeliveredMessage already published via OrderEventKafkaPublisher
        log.debug("sendOrderDeliveredNotification: order={} — no-op, already published via KafkaPublisher", orderId);
    }

    @Override
    public void sendOrderCancelledNotification(String orderId, String reason) {
        // OrderCancelledMessage already published via OrderEventKafkaPublisher
        log.debug("sendOrderCancelledNotification: order={} reason={} — no-op, already published via KafkaPublisher", orderId, reason);
    }

    @Override
    public void sendPaymentFailedNotification(String orderId, String reason) {
        // PaymentFailedMessage already published via OrderEventKafkaPublisher
        log.debug("sendPaymentFailedNotification: order={} reason={} — no-op, already published via KafkaPublisher", orderId, reason);
    }
}
