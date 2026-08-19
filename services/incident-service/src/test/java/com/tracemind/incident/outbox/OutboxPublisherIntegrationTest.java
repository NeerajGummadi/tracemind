package com.tracemind.incident.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.domain.OutboxEvent;
import com.tracemind.incident.repository.OutboxEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest
// Long enough that the real @Scheduled cron never fires during these tests -
// every claim/publish call here is explicit, not via the background poller.
@TestPropertySource(properties = "outbox.publisher.poll-interval-ms=3600000")
@EmbeddedKafka(
        partitions = 1,
        topics = {OutboxPublisher.TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OutboxPublisherIntegrationTest {

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
    private OutboxPublisher outboxPublisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterEach
    void cleanUp() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void singlePendingRowIsPublishedWithCorrectKeyAndPayload() throws Exception {
        String payload = payloadFor("INC-101");
        OutboxEvent event = OutboxEvent.pending("Incident", UUID.randomUUID(), "investigation.requested", payload);
        outboxEventRepository.save(event);

        boolean processed = outboxPublisher.claimAndPublishOne();

        assertThat(processed).isTrue();

        OutboxEvent reloaded = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(OutboxEvent.STATUS_PUBLISHED);
        assertThat(reloaded.getPublishedAt()).isNotNull();

        Map<String, String> consumed = consumeUntilKeysPresent(Set.of("INC-101"));
        assertJsonEquals(payload, consumed.get("INC-101"));
    }

    @Test
    void failedPublishLeavesRowPendingAndALaterRetrySucceeds() throws Exception {
        String payload = payloadFor("INC-202");
        OutboxEvent event = OutboxEvent.pending("Incident", UUID.randomUUID(), "investigation.requested", payload);
        outboxEventRepository.save(event);

        // Simulate a failed publish attempt using a broker that can't be reached, wrapped in a
        // real transaction from the same platform transaction manager @Transactional would use -
        // so the rollback-on-failure behavior being proven here is genuine, not simulated.
        var brokenProducerFactory = new DefaultKafkaProducerFactory<String, String>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.MAX_BLOCK_MS_CONFIG, 500));
        var brokenKafkaTemplate = new KafkaTemplate<>(brokenProducerFactory);

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        assertThatThrownBy(() -> transactionTemplate.execute(status -> {
            Optional<OutboxEvent> claimed = outboxEventRepository.claimOnePending();
            OutboxEvent claimedEvent = claimed.orElseThrow();
            brokenKafkaTemplate.send(OutboxPublisher.TOPIC, "INC-202", claimedEvent.getPayload())
                    .join(); // will fail against an unreachable broker
            claimedEvent.markPublished(Instant.now());
            return null;
        })).isInstanceOf(RuntimeException.class);

        OutboxEvent stillPending = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(stillPending.getStatus()).isEqualTo(OutboxEvent.STATUS_PENDING);
        assertThat(stillPending.getPublishedAt()).isNull();

        // Retry with the real, working publisher against the same still-PENDING row.
        boolean processed = outboxPublisher.claimAndPublishOne();

        assertThat(processed).isTrue();
        OutboxEvent published = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(published.getStatus()).isEqualTo(OutboxEvent.STATUS_PUBLISHED);
        assertThat(published.getPublishedAt()).isNotNull();

        Map<String, String> consumed = consumeUntilKeysPresent(Set.of("INC-202"));
        assertJsonEquals(payload, consumed.get("INC-202"));
    }

    @Test
    void multiplePendingRowsAreProcessedWithoutCorruption() throws Exception {
        String payloadA = payloadFor("INC-301");
        String payloadB = payloadFor("INC-302");
        String payloadC = payloadFor("INC-303");
        outboxEventRepository.save(OutboxEvent.pending("Incident", UUID.randomUUID(), "investigation.requested", payloadA));
        outboxEventRepository.save(OutboxEvent.pending("Incident", UUID.randomUUID(), "investigation.requested", payloadB));
        outboxEventRepository.save(OutboxEvent.pending("Incident", UUID.randomUUID(), "investigation.requested", payloadC));

        outboxPublisher.poll();

        List<OutboxEvent> all = outboxEventRepository.findAll();
        assertThat(all).hasSize(3);
        assertThat(all).allMatch(e -> e.getStatus().equals(OutboxEvent.STATUS_PUBLISHED));
        assertThat(all).allMatch(e -> e.getPublishedAt() != null);

        Map<String, String> consumed = consumeUntilKeysPresent(Set.of("INC-301", "INC-302", "INC-303"));
        assertJsonEquals(payloadA, consumed.get("INC-301"));
        assertJsonEquals(payloadB, consumed.get("INC-302"));
        assertJsonEquals(payloadC, consumed.get("INC-303"));
    }

    private String payloadFor(String incidentId) {
        return """
                {"eventId":"evt-%s","schemaVersion":"1.0","incidentId":"%s","primaryService":"payment-service","environment":"prod","severity":"CRITICAL","firstObservedAt":1787110070.0,"lastObservedAt":1787110070.0,"triggerSignalIds":["evt-1"]}
                """.formatted(UUID.randomUUID(), incidentId).strip();
    }

    /** jsonb doesn't preserve original text (key order/whitespace) - compare parsed content, not raw strings. */
    private void assertJsonEquals(String expected, String actual) throws Exception {
        assertThat(objectMapper.readTree(actual)).isEqualTo(objectMapper.readTree(expected));
    }

    /**
     * Every test method shares the same embedded topic within this class, so leftover records
     * from earlier tests are expected - this polls until the keys THIS test cares about show up,
     * rather than asserting on the total record count ever published to the topic.
     */
    private Map<String, String> consumeUntilKeysPresent(Set<String> expectedKeys) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-" + UUID.randomUUID(), "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Map<String, String> collected = new HashMap<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer())) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, OutboxPublisher.TOPIC);
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    collected.put(record.key(), record.value());
                }
                assertThat(collected.keySet()).containsAll(expectedKeys);
            });
        }
        return collected;
    }
}
