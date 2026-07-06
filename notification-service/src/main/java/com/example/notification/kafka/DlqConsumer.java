package com.example.notification.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * DlqConsumer — reads messages that exhausted all retries from the Dead Letter Topics.
 *
 * Spring Kafka's DeadLetterPublishingRecoverer routes to {originalTopic}.DLT after
 * FixedBackOff(2s, 2 retries) is exhausted. Messages arrive here as raw bytes with
 * exception context in headers added automatically by Spring Kafka.
 *
 * Current behaviour: logs at ERROR level for alerting/investigation.
 * Production extension: write to an incidents table, send a PagerDuty/Slack alert,
 * or re-queue to a manual-review topic.
 */
@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    @KafkaListener(
            topics = {"order.events.DLT", "payment.events.DLT"},
            groupId = "notification-service-dlq-group",
            containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, byte[]> record) {
        String originalTopic     = header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        String originalPartition = header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        String originalOffset    = header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        String exceptionClass    = header(record, KafkaHeaders.DLT_EXCEPTION_FQCN);
        String exceptionMessage  = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);

        log.error(
                "DLQ message received — action required! " +
                "original-topic={}, partition={}, offset={}, key={}, " +
                "exception={}: {}",
                originalTopic, originalPartition, originalOffset, record.key(),
                exceptionClass, exceptionMessage);
    }

    private String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : "n/a";
    }
}
