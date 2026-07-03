package com.example.ordermanagement.config;

import com.example.contracts.kafka.ShippingEventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for shipping.events.
 * order-service-group consumes delivery confirmation to close the saga.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Bean
    public DefaultErrorHandler shippingDlqErrorHandler() {
        // Publishes failed shipping.events messages to shipping.events.dlq after 3 attempts
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(2000L, 2));
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }

    @Bean
    public ConsumerFactory<String, ShippingEventMessage> shippingConsumerFactory() {
        JsonDeserializer<ShippingEventMessage> deserializer =
                new JsonDeserializer<>(ShippingEventMessage.class, false);
        deserializer.addTrustedPackages("com.example.contracts.kafka");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ShippingEventMessage>
            shippingKafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ShippingEventMessage>();
        factory.setConsumerFactory(shippingConsumerFactory());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(shippingDlqErrorHandler());
        return factory;
    }
}