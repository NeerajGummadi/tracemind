package com.tracemind.incident.service;

import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.IncidentSignalRepository;
import com.tracemind.incident.repository.OutboxEventRepository;
import com.tracemind.incident.repository.SignalRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest
class SignalIngestionServiceIntegrationTest {

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
    private SignalIngestionService signalIngestionService;

    @Autowired
    private SignalRepository signalRepository;

    @Autowired
    private IncidentRepository incidentRepository;

    @Autowired
    private IncidentSignalRepository incidentSignalRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @AfterEach
    void cleanUp() {
        // Each ingest() call runs in its own transaction, same as in production via the
        // listener - not wrapping the test in a rolled-back transaction, so state must be
        // cleared explicitly between tests, in FK-safe order.
        outboxEventRepository.deleteAll();
        incidentSignalRepository.deleteAll();
        incidentRepository.deleteAll();
        signalRepository.deleteAll();
    }

    @Test
    void firstSignalCreatesAnIncidentAndOutboxRow() {
        CanonicalSignalV1 signal = signalFor("evt-" + UUID.randomUUID(), "payment-service", "prod", Instant.now());

        signalIngestionService.ingest(signal);

        assertThat(signalRepository.findAll()).hasSize(1);
        assertThat(incidentRepository.findAll()).hasSize(1);
        assertThat(incidentRepository.findAll().get(0).getStatus()).isEqualTo("QUEUED");
        assertThat(incidentSignalRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll().get(0).getEventType()).isEqualTo("investigation.requested");
        assertThat(outboxEventRepository.findAll().get(0).getStatus()).isEqualTo("PENDING");
        assertThat(outboxEventRepository.findAll().get(0).getAggregateId())
                .isEqualTo(incidentRepository.findAll().get(0).getId());
    }

    @Test
    void duplicateEventIdDoesNotCreateDuplicateState() {
        String eventId = "evt-" + UUID.randomUUID();
        CanonicalSignalV1 signal = signalFor(eventId, "payment-service", "prod", Instant.now());

        signalIngestionService.ingest(signal);

        // Mirrors production: the listener catches DuplicateSignalException and acks anyway -
        // ingest() itself is expected to throw it on redelivery, by design (see
        // SignalIngestionService's javadoc on why the exception must propagate out of the
        // transactional method rather than being swallowed there).
        assertThatThrownBy(() -> signalIngestionService.ingest(signal))
                .isInstanceOf(DuplicateSignalException.class);

        assertThat(signalRepository.findAll()).hasSize(1);
        assertThat(incidentRepository.findAll()).hasSize(1);
        assertThat(incidentSignalRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll()).hasSize(1);
    }

    @Test
    void secondDistinctSignalForSameServiceWithinWindowCorrelatesToSameIncident() {
        Instant now = Instant.now();
        CanonicalSignalV1 first = signalFor("evt-" + UUID.randomUUID(), "fraud-service", "prod", now);
        CanonicalSignalV1 second = signalFor("evt-" + UUID.randomUUID(), "fraud-service", "prod", now.plusSeconds(60));

        signalIngestionService.ingest(first);
        signalIngestionService.ingest(second);

        assertThat(signalRepository.findAll()).hasSize(2);
        assertThat(incidentRepository.findAll()).hasSize(1);
        assertThat(incidentSignalRepository.findAll()).hasSize(2);
        // outbox row is only written on new-incident creation, not on correlation into an existing one
        assertThat(outboxEventRepository.findAll()).hasSize(1);
    }

    @Test
    void differentServiceCreatesADifferentIncident() {
        Instant now = Instant.now();
        CanonicalSignalV1 first = signalFor("evt-" + UUID.randomUUID(), "payment-service", "prod", now);
        CanonicalSignalV1 second = signalFor("evt-" + UUID.randomUUID(), "checkout-service", "prod", now.plusSeconds(60));

        signalIngestionService.ingest(first);
        signalIngestionService.ingest(second);

        assertThat(incidentRepository.findAll()).hasSize(2);
        assertThat(outboxEventRepository.findAll()).hasSize(2);
    }

    private CanonicalSignalV1 signalFor(String eventId, String service, String environment, Instant observedAt) {
        return new CanonicalSignalV1(
                eventId, "1.0", "PROMETHEUS", "DB_CONNECTION_PRESSURE", service, environment, "CRITICAL",
                observedAt, observedAt, Map.of(), Map.of());
    }
}
