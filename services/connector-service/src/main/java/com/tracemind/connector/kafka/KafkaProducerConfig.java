package com.tracemind.connector.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tracemind.connector.contract.CanonicalSignalV1;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Spring Kafka's JsonSerializer, when picked up purely via
 * spring.kafka.producer.value-serializer, builds its own ObjectMapper with no
 * JSR-310 module - it can't serialize the Instant fields on CanonicalSignalV1.
 * Wiring the serializer explicitly lets it share an ObjectMapper that has
 * JavaTimeModule registered.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, CanonicalSignalV1> producerFactory(KafkaProperties kafkaProperties) {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, CanonicalSignalV1> kafkaTemplate(
            ProducerFactory<String, CanonicalSignalV1> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
