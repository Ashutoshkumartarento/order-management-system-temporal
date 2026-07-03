package com.example.ordermanagement.infrastructure.projection;

import com.example.ordermanagement.domain.event.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Maintains the order_summary read model by listening to domain events.
 *
 * Uses @TransactionalEventListener(AFTER_COMMIT) so the projection is only
 * updated after the event_store transaction has committed — same guarantee
 * as the Kafka publisher. @Transactional(REQUIRES_NEW) opens a fresh
 * transaction for the projection write since the original transaction is gone.
 *
 * In production, this would instead consume from the order.events Kafka topic
 * using a dedicated consumer group. That enables projection replay from
 * offset 0 if the read model is ever dropped and rebuilt. For this PoC,
 * the Spring event approach is equivalent in behaviour.
 */
@Component
public class OrderProjectionUpdater {

    private final OrderSummaryRepository repository;

    public OrderProjectionUpdater(OrderSummaryRepository repository) {
        this.repository = repository;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void on(DomainEvent event) {
        switch (event) {
            case OrderCreatedEvent e -> repository.insert(
                    e.aggregateId(), e.customerId().toString(), e.shippingAddress(), e.occurredAt());

            case ItemAddedEvent e -> repository.incrementItemCount(e.aggregateId(), e.occurredAt());

            case ItemRemovedEvent e -> repository.decrementItemCount(e.aggregateId(), e.occurredAt());

            case OrderConfirmedEvent e -> repository.confirmOrder(
                    e.aggregateId(), e.totalAmount().amount(), e.workflowId(), e.occurredAt());

            case InventoryReservedEvent e -> repository.updateStatus(
                    e.aggregateId(), "INVENTORY_RESERVED", e.occurredAt());

            case PaymentCompletedEvent e -> repository.completePayment(e.aggregateId(), e.occurredAt());

            case PaymentFailedEvent e -> repository.failPayment(e.aggregateId(), e.occurredAt());

            case RefundCompletedEvent e -> repository.refundPayment(e.aggregateId(), e.occurredAt());

            case ShipmentCreatedEvent e -> repository.createShipment(
                    e.aggregateId(), e.trackingNumber(), e.occurredAt());

            case ShipmentDeliveredEvent e -> repository.deliverOrder(e.aggregateId(), e.occurredAt());

            case OrderCancelledEvent e -> repository.cancelOrder(
                    e.aggregateId(), e.cancellationReason(), e.occurredAt());

            // No projection change needed for these
            case InventoryReservationFailedEvent ignored -> {}
            case InventoryReleasedEvent ignored -> {}
        }
    }
}