package com.tracemind.incident.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.SignalRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {SignalConsumerListener.TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class SignalConsumerKafkaIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("tracemind")
            .withUsername("tracemind")
            .withPassword("tracemind");

    @DynamicPropertySource
    static void registerDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private SignalRepository signalRepository;

    @Autowired
    private IncidentRepository incidentRepository;

    /**
     * Publishes with a __TypeId__ header naming a class that only exists in
     * connector-service (com.tracemind.connector.contract.CanonicalSignalV1),
     * not on this classpath - proving useHeaders=false in KafkaConsumerConfig
     * genuinely avoids depending on that header, rather than happening to
     * work because the header would have resolved anyway.
     */
    @Test
    void consumesAndDeserializesDespiteForeignTypeHeader() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        String eventId = "evt-real-listener-test";
        String json = """
                {
                  "eventId": "%s",
                  "schemaVersion": "1.0",
                  "source": "PROMETHEUS",
                  "signalType": "DB_CONNECTION_PRESSURE",
                  "service": "payment-service",
                  "environment": "prod",
                  "severity": "CRITICAL",
                  "startedAt": %d.0,
                  "observedAt": %d.0,
                  "labels": {},
                  "attributes": {}
                }
                """.formatted(eventId, Instant.now().getEpochSecond(), Instant.now().getEpochSecond());

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    SignalConsumerListener.TOPIC, "prod:payment-service", json.getBytes(StandardCharsets.UTF_8));
            record.headers().add(new RecordHeader(
                    "__TypeId__",
                    "com.tracemind.connector.contract.CanonicalSignalV1".getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get();
        }

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(signalRepository.findAll()).anyMatch(s -> s.getEventId().equals(eventId)));
        assertThat(incidentRepository.findAll()).hasSize(1);
    }
}
