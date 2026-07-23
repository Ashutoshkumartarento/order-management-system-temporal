package com.example.inventory.infrastructure.outbox;

import com.example.contracts.kafka.InventoryEventMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Outbox relay for inventory.events.
 *
 * Fast path: triggered AFTER_COMMIT via OutboxFlushSignal — publishes immediately.
 * Safety net: @Scheduled every 30s catches anything the fast path missed (e.g. crash
 *             between DB commit and Kafka send).
 *
 * markPublished uses WHERE published_at IS NULL to guard against concurrent relay runs.
 */
@Component
public class InventoryOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(InventoryOutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, InventoryEventMessage> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public InventoryOutboxRelay(OutboxRepository outboxRepository,
                                KafkaTemplate<String, InventoryEventMessage> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate    = kafkaTemplate;
        this.objectMapper     = objectMapper;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onFlushSignal(OutboxFlushSignal signal) {
        publishPending();
    }

    @Scheduled(fixedDelay = 30_000)
    public void publishPending() {
        List<OutboxEntry> pending = outboxRepository.findUnpublished();
        if (pending.isEmpty()) return;

        log.debug("Outbox relay: {} pending inventory event(s)", pending.size());
        for (OutboxEntry entry : pending) {
            try {
                InventoryEventMessage message =
                        objectMapper.readValue(entry.payload(), InventoryEventMessage.class);
                kafkaTemplate.send(entry.topic(), entry.aggregateId(), message)
                        .get(10, TimeUnit.SECONDS);
                int updated = outboxRepository.markPublished(entry.getId(), Instant.now());
                if (updated > 0) {
                    log.debug("Published {} for order {}", entry.eventType(), entry.aggregateId());
                }
            } catch (Exception e) {
                log.error("Outbox relay failed for entry {} ({}): {}",
                        entry.getId(), entry.eventType(), e.getMessage());
            }
        }
    }
}
