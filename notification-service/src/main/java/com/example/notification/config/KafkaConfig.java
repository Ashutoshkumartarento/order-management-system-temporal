package com.example.notification.config;

import com.example.contracts.kafka.OrderEventMessage;
import com.example.contracts.kafka.PaymentEventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ── DLQ producer (raw bytes — preserves original message payload) ──────

    @Bean
    public KafkaTemplate<String, byte[]> dlqKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public DefaultErrorHandler dlqErrorHandler(KafkaTemplate<String, byte[]> dlqKafkaTemplate) {
        // Publishes failed messages to {originalTopic}.dlq after 3 attempts (2 retries)
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(dlqKafkaTemplate);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(2000L, 2));  // 2 retries, 2s apart
        // Deserialization failures go straight to DLQ — no retry
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }

    // ── Order events consumer ──────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, OrderEventMessage> consumerFactory() {
        JsonDeserializer<OrderEventMessage> deserializer =
                new JsonDeserializer<>(OrderEventMessage.class, false);
        deserializer.addTrustedPackages("com.example.contracts.kafka");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEventMessage>
            kafkaListenerContainerFactory(DefaultErrorHandler dlqErrorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEventMessage>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(dlqErrorHandler);
        return factory;
    }

    // ── Payment events consumer ────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, PaymentEventMessage> paymentConsumerFactory() {
        JsonDeserializer<PaymentEventMessage> deserializer =
                new JsonDeserializer<>(PaymentEventMessage.class, false);
        deserializer.addTrustedPackages("com.example.contracts.kafka");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEventMessage>
            paymentKafkaListenerContainerFactory(DefaultErrorHandler dlqErrorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, PaymentEventMessage>();
        factory.setConsumerFactory(paymentConsumerFactory());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(dlqErrorHandler);
        return factory;
    }
}
