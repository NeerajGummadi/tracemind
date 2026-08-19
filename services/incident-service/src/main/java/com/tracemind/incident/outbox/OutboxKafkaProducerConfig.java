package com.tracemind.incident.outbox;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Publishes the already-serialized outbox payload string as-is, so unlike
 * connector-service's producer there's no Jackson/Instant concern here - just
 * a plain String,String producer. Defined explicitly (rather than relying on
 * Boot's autoconfigured KafkaTemplate<Object,Object>) because that bean's
 * generic type won't satisfy an @Autowired KafkaTemplate<String,String>
 * injection point.
 */
@Configuration
public class OutboxKafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> outboxProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> outboxProducerFactory) {
        return new KafkaTemplate<>(outboxProducerFactory);
    }
}
