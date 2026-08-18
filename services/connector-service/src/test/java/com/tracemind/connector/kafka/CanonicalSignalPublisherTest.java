package com.tracemind.connector.kafka;

import com.tracemind.connector.contract.CanonicalSignalV1;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalSignalPublisherTest {

    @Test
    void publishFailsFastWhenBrokerIsUnreachable() {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1", // nothing listens here
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);
        KafkaTemplate<String, CanonicalSignalV1> kafkaTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        CanonicalSignalPublisher publisher = new CanonicalSignalPublisher(kafkaTemplate, 1000);

        CanonicalSignalV1 signal = new CanonicalSignalV1(
                "evt-test", "1.0", "PROMETHEUS", "X", "svc", "prod", "CRITICAL",
                Instant.now(), Instant.now(), Map.of(), Map.of());

        assertThatThrownBy(() -> publisher.publish(signal))
                .isInstanceOf(SignalPublishException.class);
    }
}
