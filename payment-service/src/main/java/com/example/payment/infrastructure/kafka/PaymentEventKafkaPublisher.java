package com.example.payment.infrastructure.kafka;

import com.example.contracts.kafka.PaymentEventMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Publishes payment domain events to Kafka AFTER the DB transaction commits.
 * Key = orderId ensures per-order ordering within the payment.events partition.
 */
@Component
public class PaymentEventKafkaPublisher {

    public static final String TOPIC = "payment.events";

    private final KafkaTemplate<String, PaymentEventMessage> kafkaTemplate;

    public PaymentEventKafkaPublisher(KafkaTemplate<String, PaymentEventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onPaymentEvent(PaymentEventMessage event) {
        kafkaTemplate.send(TOPIC, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    // Temporal retries handle downstream failures; no local retry needed here
                });
    }
}