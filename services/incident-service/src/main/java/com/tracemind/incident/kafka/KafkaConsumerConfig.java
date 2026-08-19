package com.tracemind.incident.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tracemind.incident.contract.CanonicalSignalV1;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Same Jackson-2-must-be-explicit story as connector-service's
 * KafkaProducerConfig. Also: useHeaders=false on the JsonDeserializer is
 * required here specifically - the producer (connector-service) sends
 * __TypeId__ headers naming its own package
 * (com.tracemind.connector.contract.CanonicalSignalV1), which doesn't exist
 * on this classpath. Ignoring headers and always targeting this service's
 * own copy of the contract class is what makes that work.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    public ConsumerFactory<String, CanonicalSignalV1> consumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper kafkaObjectMapper) {
        JsonDeserializer<CanonicalSignalV1> valueDeserializer =
                new JsonDeserializer<>(CanonicalSignalV1.class, kafkaObjectMapper, false);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CanonicalSignalV1> kafkaListenerContainerFactory(
            ConsumerFactory<String, CanonicalSignalV1> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, CanonicalSignalV1> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
