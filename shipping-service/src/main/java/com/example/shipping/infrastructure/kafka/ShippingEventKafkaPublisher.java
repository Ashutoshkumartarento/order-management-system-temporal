package com.example.shipping.infrastructure.kafka;

import com.example.contracts.kafka.ShippingEventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Publishes shipping domain events to Kafka AFTER the DB transaction commits.
 * Key = orderId ensures per-order ordering within the shipping.events partition.
 * Consumed by: notification-service-group and order-service-group.
 */
@Component
public class ShippingEventKafkaPublisher {

    public static final String TOPIC = "shipping.events";

    private final KafkaTemplate<String, ShippingEventMessage> kafkaTemplate;

    public ShippingEventKafkaPublisher(KafkaTemplate<String, ShippingEventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onShippingEvent(ShippingEventMessage event) {
        kafkaTemplate.send(TOPIC, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    // Temporal retries handle downstream failures; no local retry needed here
                });
    }
}