package com.example.ordermanagement.infrastructure.kafka;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.ordermanagement.domain.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * OrderEventKafkaPublisher — Outbox Pattern Implementation
 *
 * OUTBOX PATTERN EXPLAINED:
 * 1. Domain event is persisted to event_store (inside DB transaction)
 * 2. Spring's TransactionSynchronizationManager queues a post-commit callback
 * 3. AFTER the DB transaction commits, this method is called
 * 4. We publish to Kafka
 *
 * WHY THIS MATTERS:
 * If we published to Kafka INSIDE the transaction and then the DB rolled back,
 * consumers would receive an event for a change that never happened — a phantom event.
 *
 * If we published AFTER commit but crashed before publishing, the event is lost.
 * For full guarantee in production, use Debezium CDC (Change Data Capture) to
 * stream from the event_store table directly to Kafka.
 * For this PoC, AFTER_COMMIT is sufficient and demonstrates the pattern.
 *
 * ORDERING GUARANTEE:
 * Message key = orderId → Kafka routes all messages for the same order
 * to the same partition → guaranteed ordering per order.
 */
@Component
public class OrderEventKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventKafkaPublisher.class);

    public static final String TOPIC = "order.events";

    private final KafkaTemplate<String, OrderEventMessage> kafkaTemplate;

    public OrderEventKafkaPublisher(KafkaTemplate<String, OrderEventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Called AFTER the DB transaction commits.
     * Spring publishes a DomainEvent as an application event from OrderRepositoryAdapter.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onDomainEvent(DomainEvent domainEvent) {
        OrderEventMessage message = toMessage(domainEvent);
        if (message == null) {
            return; // Not all events need Kafka publication
        }

        log.debug("Publishing {} to Kafka for order {}", domainEvent.eventType(), domainEvent.aggregateId());

        kafkaTemplate.send(TOPIC, domainEvent.aggregateId(), message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish {} to Kafka for order {}: {}",
                                domainEvent.eventType(), domainEvent.aggregateId(), ex.getMessage());
                    } else {
                        log.debug("Published {} to Kafka for order {} [partition={}, offset={}]",
                                domainEvent.eventType(), domainEvent.aggregateId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    private OrderEventMessage toMessage(DomainEvent event) {
        return switch (event) {
            case OrderCreatedEvent e -> new OrderEventMessage.OrderCreatedMessage(
                    e.eventId(), e.aggregateId(), e.customerId().toString(),
                    e.shippingAddress(), e.occurredAt());

            case OrderConfirmedEvent e -> new OrderEventMessage.OrderConfirmedMessage(
                    e.eventId(), e.aggregateId(), null,
                    e.totalAmount().amount().doubleValue(), e.workflowId(), e.occurredAt());

            case OrderCancelledEvent e -> new OrderEventMessage.OrderCancelledMessage(
                    e.eventId(), e.aggregateId(), e.cancellationReason(),
                    e.cancelledBy(), e.occurredAt());

            case PaymentCompletedEvent e -> new OrderEventMessage.PaymentCompletedMessage(
                    e.eventId(), e.aggregateId(), e.transactionId(),
                    e.amountCharged().amount().doubleValue(), e.occurredAt());

            case PaymentFailedEvent e -> new OrderEventMessage.PaymentFailedMessage(
                    e.eventId(), e.aggregateId(), e.reason(), e.retryable(), e.occurredAt());

            case ShipmentCreatedEvent e -> new OrderEventMessage.ShipmentCreatedMessage(
                    e.eventId(), e.aggregateId(), e.shipmentId(),
                    e.trackingNumber(), e.carrier(), e.occurredAt());

            case ShipmentDeliveredEvent e -> new OrderEventMessage.ShipmentDeliveredMessage(
                    e.eventId(), e.aggregateId(), e.shipmentId(),
                    e.deliveredAt(), e.occurredAt());

            case InventoryReservedEvent e -> new OrderEventMessage.InventoryReservedMessage(
                    e.eventId(), e.aggregateId(), e.reservationId(), e.occurredAt());

            case InventoryReleasedEvent e -> new OrderEventMessage.InventoryReleasedMessage(
                    e.eventId(), e.aggregateId(), e.reservationId(), e.reason(), e.occurredAt());

            // These events are internal domain detail — no external Kafka message needed
            case ItemAddedEvent ignored -> null;
            case ItemRemovedEvent ignored -> null;
            case InventoryReservationFailedEvent ignored -> null;
            case RefundCompletedEvent ignored -> null;
        };
    }
}
