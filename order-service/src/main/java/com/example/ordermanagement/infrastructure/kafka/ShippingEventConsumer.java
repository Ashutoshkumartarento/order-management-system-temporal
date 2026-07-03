package com.example.ordermanagement.infrastructure.kafka;

import com.example.contracts.kafka.ShippingEventMessage;
import com.example.ordermanagement.application.service.OrderCommandService;
import com.example.ordermanagement.domain.valueobject.OrderId;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes shipping.events to close the saga when delivery is confirmed.
 *
 * This is the event-driven complement to the Temporal activity path:
 * - Temporal path: ShippingActivityImpl.confirmDelivery() → recordShipmentDelivered()
 * - Kafka path:    ShipmentDeliveredMessage → recordOrderDelivered() (idempotent)
 *
 * Both paths converge at OrderCommandService.recordOrderDelivered(), which is
 * guarded against duplicate delivery recording.
 *
 * In production: replace in-memory processedEventIds with Redis TTL store.
 */
@Component
public class ShippingEventConsumer {

    private final OrderCommandService orderCommandService;

    // In-memory deduplication store (use Redis in production)
    private final Set<UUID> processedEventIds = ConcurrentHashMap.newKeySet();

    public ShippingEventConsumer(OrderCommandService orderCommandService) {
        this.orderCommandService = orderCommandService;
    }

    @KafkaListener(
            topics = "shipping.events",
            groupId = "order-service-group",
            containerFactory = "shippingKafkaListenerContainerFactory")
    public void onShippingEvent(ShippingEventMessage message) {
        if (!processedEventIds.add(message.eventId())) {
            return;
        }

        switch (message) {
            case ShippingEventMessage.ShipmentDeliveredMessage m -> {
                try {
                    orderCommandService.recordOrderDelivered(
                            OrderId.of(m.orderId()), m.shipmentId());
                } catch (Exception e) {
                    // Order already delivered (via Temporal activity path) or
                    // in an unexpected state — safe to swallow, saga is closed
                }
            }
            case ShippingEventMessage.ShipmentCreatedMessage ignored -> {
                // ShipmentCreated is recorded by Temporal via ShippingActivityImpl
            }
        }
    }
}