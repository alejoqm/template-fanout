package com.templatefanout.engagement.update.processor.config;

import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the engagement-update-processor.
 *
 * Key decisions:
 *  - MANUAL_IMMEDIATE ack mode: offsets committed only after the service confirms
 *    the event is recorded. Prevents event loss on crash.
 *  - ErrorHandlingDeserializer wraps JsonDeserializer so malformed payloads don't
 *    kill the consumer thread; they surface as recoverable records instead.
 *  - Exponential backoff with cap: transient downstream failures retry, but a
 *    permanently broken record eventually stops consuming resources.
 *  - max.poll.records kept low (10): each event can fan out to thousands of
 *    engagement tasks, so we don't want to hold a huge batch in memory.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Bean
    public ConsumerFactory<String, TemplatePublishEvent> templatePublishConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Small batches: one event fans out to many engagement tasks.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // Deserialization with error handling wrapper.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "com.templatefanout.engagement.update.processor.domain");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TemplatePublishEvent>
    templatePublishContainerFactory() {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, TemplatePublishEvent>();
        factory.setConsumerFactory(templatePublishConsumerFactory());

        // Manual ack: commit only after the event is durably recorded.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Concurrency: one consumer thread per partition, capped so a single
        // regional pod doesn't monopolise partitions.
        factory.setConcurrency(3);

        // Retry with exponential backoff before delegating to the recoverer.
        var backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(30000L);
        backOff.setMaxElapsedTime(120000L);

        var errorHandler = new DefaultErrorHandler(backOff);
        // Do not retry poison-pill deserialization errors endlessly.
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}