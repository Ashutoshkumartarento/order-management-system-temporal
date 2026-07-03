package com.example.inventory.infrastructure.kafka;

import com.example.contracts.kafka.InventoryEventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Publishes inventory domain events to Kafka AFTER the DB transaction commits.
 * Key = orderId ensures per-order ordering within the inventory.events partition.
 */
@Component
public class InventoryEventKafkaPublisher {

    public static final String TOPIC = "inventory.events";

    private final KafkaTemplate<String, InventoryEventMessage> kafkaTemplate;

    public InventoryEventKafkaPublisher(KafkaTemplate<String, InventoryEventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onInventoryEvent(InventoryEventMessage event) {
        kafkaTemplate.send(TOPIC, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    // Temporal retries handle downstream failures; no local retry needed here
                });
    }
}