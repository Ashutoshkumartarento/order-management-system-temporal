package com.example.ordermanagement.infrastructure.kafka;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.ordermanagement.infrastructure.projection.OrderSummaryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Builds and maintains the order_summary read model by consuming order.events.
 *
 * Consumer group: order-projection-group
 * Offset reset: earliest — allows full replay from offset 0 if the
 * order_summary table is ever dropped or needs to be rebuilt.
 *
 * Replaces the previous @TransactionalEventListener approach which was
 * in-process and had no replay capability.
 */
@Component
public class OrderProjectionConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderProjectionConsumer.class);

    private final OrderSummaryRepository repository;

    public OrderProjectionConsumer(OrderSummaryRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topics = "order.events",
            groupId = "order-projection-group",
            containerFactory = "projectionListenerContainerFactory")
    public void onOrderEvent(OrderEventMessage message) {
        try {
            switch (message) {
                case OrderEventMessage.OrderCreatedMessage m ->
                        repository.insert(m.orderId(), m.customerId(), m.shippingAddress(), m.occurredAt());

                case OrderEventMessage.ItemAddedMessage m ->
                        repository.incrementItemCount(m.orderId(), m.occurredAt());

                case OrderEventMessage.ItemRemovedMessage m ->
                        repository.decrementItemCount(m.orderId(), m.occurredAt());

                case OrderEventMessage.OrderConfirmedMessage m ->
                        repository.confirmOrder(m.orderId(), BigDecimal.valueOf(m.totalAmount()),
                                m.workflowId(), m.occurredAt());

                case OrderEventMessage.InventoryReservedMessage m ->
                        repository.updateStatus(m.orderId(), "INVENTORY_RESERVED", m.occurredAt());

                case OrderEventMessage.PaymentCompletedMessage m ->
                        repository.completePayment(m.orderId(), m.occurredAt());

                case OrderEventMessage.PaymentFailedMessage m ->
                        repository.failPayment(m.orderId(), m.occurredAt());

                case OrderEventMessage.RefundCompletedMessage m ->
                        repository.refundPayment(m.orderId(), m.occurredAt());

                case OrderEventMessage.ShipmentCreatedMessage m ->
                        repository.createShipment(m.orderId(), m.trackingNumber(), m.occurredAt());

                case OrderEventMessage.ShipmentDeliveredMessage m ->
                        repository.deliverOrder(m.orderId(), m.occurredAt());

                case OrderEventMessage.OrderCancelledMessage m ->
                        repository.cancelOrder(m.orderId(), m.reason(), m.occurredAt());

                case OrderEventMessage.InventoryReleasedMessage ignored -> {}
            }
        } catch (DuplicateKeyException e) {
            // OrderCreated replayed — row already exists, safe to skip
            log.debug("Projection already applied for order {} ({}), skipping (idempotent)",
                    message.orderId(), message.getClass().getSimpleName());
        }
    }
}
