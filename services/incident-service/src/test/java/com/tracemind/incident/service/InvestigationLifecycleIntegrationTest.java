package com.tracemind.incident.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.domain.Incident;
import com.tracemind.incident.domain.InvestigationRun;
import com.tracemind.incident.domain.OutboxEvent;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.IncidentSignalRepository;
import com.tracemind.incident.repository.InvestigationRunRepository;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Milestone M - the investigation-coalescing algorithm end to end: signal
 * ingestion (Case A/B) plus investigation.results.v1 consumption
 * (InvestigationResultService), against a real Postgres via testcontainers.
 * Kafka round-tripping of the payloads themselves is covered separately
 * (SignalConsumerKafkaIntegrationTest, OutboxPublisherIntegrationTest) - this
 * class exercises the two @Transactional services directly.
 */
@Testcontainers
@SpringBootTest
class InvestigationLifecycleIntegrationTest {

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
    private InvestigationResultService investigationResultService;

    @Autowired
    private SignalRepository signalRepository;

    @Autowired
    private IncidentRepository incidentRepository;

    @Autowired
    private IncidentSignalRepository incidentSignalRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private InvestigationRunRepository investigationRunRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterEach
    void cleanUp() {
        outboxEventRepository.deleteAll();
        incidentSignalRepository.deleteAll();
        investigationRunRepository.deleteAll();
        incidentRepository.deleteAll();
        signalRepository.deleteAll();
    }

    @Test
    void completedResultWithUnchangedVersionCompletesTheRunAndNeverAutoResolvesTheIncident() {
        signalIngestionService.ingest(signalFor("payment-service", Instant.now()));
        Incident incident = onlyIncident();
        InvestigationRun run = onlyRun();

        investigationResultService.handleResult(completedResultJson(run.getId()));

        InvestigationRun reloaded = investigationRunRepository.findById(run.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED);
        assertThat(reloaded.getCompletedAt()).isNotNull();

        Incident reloadedIncident = incidentRepository.findById(incident.getId()).orElseThrow();
        // The explicit invariant: AI completing must never itself change incident status.
        assertThat(reloadedIncident.getStatus()).isEqualTo("QUEUED");
        assertThat(reloadedIncident.isNeedsReinvestigation()).isFalse();
        assertThat(reloadedIncident.getCurrentInvestigationRunId()).isEqualTo(run.getId());
    }

    @Test
    void failedResultMarksTheRunFailedRegardlessOfVersion() {
        signalIngestionService.ingest(signalFor("payment-service", Instant.now()));
        InvestigationRun run = onlyRun();

        investigationResultService.handleResult(failedResultJson(run.getId(), "TIMEOUT"));

        InvestigationRun reloaded = investigationRunRepository.findById(run.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(InvestigationRun.STATUS_FAILED);
        assertThat(reloaded.getFailureReason()).isEqualTo("TIMEOUT");
    }

    @Test
    void correlatedAlertWhileRunningNeverLaunchesASecondConcurrentRun() {
        Instant now = Instant.now();
        signalIngestionService.ingest(signalFor("payment-service", now));
        signalIngestionService.ingest(signalFor("payment-service", now.plusSeconds(30)));

        // Only one investigation, ever, for both signals - the invariant this milestone exists to enforce.
        assertThat(investigationRunRepository.findAll()).hasSize(1);
        long runningCount = investigationRunRepository.findAll().stream()
                .filter(r -> InvestigationRun.STATUS_RUNNING.equals(r.getStatus())).count();
        assertThat(runningCount).isEqualTo(1);

        Incident incident = onlyIncident();
        assertThat(incident.getSignalVersion()).isEqualTo(2);
        assertThat(incident.isNeedsReinvestigation()).isTrue();
    }

    @Test
    void staleCompletionOfSupersededRunAutomaticallyLaunchesExactlyOneFollowUpRun() throws Exception {
        Instant now = Instant.now();
        CanonicalSignalV1 firstSignal = signalFor("payment-service", now);
        signalIngestionService.ingest(firstSignal);
        InvestigationRun firstRun = onlyRun();

        // A correlated alert arrives before the first investigation's result comes back.
        // observedAt is deliberately NOT later than firstSignal's (mirrors a real alert
        // source reporting its own startsAt, a different clock than this service's
        // ingestion time) - the follow-up's triggerSignalIds provenance must still be
        // derived from ingestion order (Signal.createdAt), not observedAt.
        CanonicalSignalV1 secondSignal = signalFor("payment-service", now);
        signalIngestionService.ingest(secondSignal);
        assertThat(investigationRunRepository.findAll()).hasSize(1); // still just firstRun

        // First investigation's result now arrives - but the incident has since moved to signalVersion 2.
        investigationResultService.handleResult(completedResultJson(firstRun.getId()));

        InvestigationRun reloadedFirstRun = investigationRunRepository.findById(firstRun.getId()).orElseThrow();
        assertThat(reloadedFirstRun.getStatus()).isEqualTo(InvestigationRun.STATUS_STALE);

        // Exactly one follow-up run was launched automatically, and needsReinvestigation was consumed.
        List<InvestigationRun> allRuns = investigationRunRepository.findAll();
        assertThat(allRuns).hasSize(2);
        InvestigationRun followUp = allRuns.stream().filter(r -> !r.getId().equals(firstRun.getId())).findFirst().orElseThrow();
        assertThat(followUp.getTriggerReason()).isEqualTo(InvestigationRun.TRIGGER_REASON_REINVESTIGATION);
        assertThat(followUp.getInputSignalVersion()).isEqualTo(2);
        assertThat(followUp.getStatus()).isEqualTo(InvestigationRun.STATUS_RUNNING);

        Incident incident = onlyIncident();
        assertThat(incident.getCurrentInvestigationRunId()).isEqualTo(followUp.getId());
        assertThat(incident.isNeedsReinvestigation()).isFalse();

        // Exactly one new outbox row (the follow-up's investigation.requested.v1) beyond the original.
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).hasSize(2);

        JsonNode followUpPayload = null;
        for (OutboxEvent event : outboxEvents) {
            JsonNode payload = objectMapper.readTree(event.getPayload());
            if (payload.get("investigationRunId").asText().equals(followUp.getId().toString())) {
                followUpPayload = payload;
            }
        }
        assertThat(followUpPayload).isNotNull();
        List<String> triggerSignalIds = objectMapper.convertValue(followUpPayload.get("triggerSignalIds"), List.class);
        // Provenance: the follow-up was caused by secondSignal, not firstSignal - and this
        // must hold even though both signals share the same observedAt (see comment above).
        assertThat(triggerSignalIds).containsExactly(secondSignal.eventId());
    }

    @Test
    void followUpRunCompletingWithMatchingVersionStaysCurrent() {
        Instant now = Instant.now();
        signalIngestionService.ingest(signalFor("payment-service", now));
        InvestigationRun firstRun = onlyRun();
        signalIngestionService.ingest(signalFor("payment-service", now.plusSeconds(30)));
        investigationResultService.handleResult(completedResultJson(firstRun.getId())); // -> STALE, launches follow-up

        InvestigationRun followUp = investigationRunRepository.findAll().stream()
                .filter(r -> !r.getId().equals(firstRun.getId())).findFirst().orElseThrow();

        investigationResultService.handleResult(completedResultJson(followUp.getId()));

        InvestigationRun reloadedFollowUp = investigationRunRepository.findById(followUp.getId()).orElseThrow();
        assertThat(reloadedFollowUp.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED);
        Incident incident = onlyIncident();
        assertThat(incident.isNeedsReinvestigation()).isFalse();
        assertThat(incident.getCurrentInvestigationRunId()).isEqualTo(followUp.getId());
    }

    @Test
    void duplicateResultForTheSameRunIsIgnored() {
        signalIngestionService.ingest(signalFor("payment-service", Instant.now()));
        InvestigationRun run = onlyRun();

        investigationResultService.handleResult(completedResultJson(run.getId()));
        Instant firstCompletedAt = investigationRunRepository.findById(run.getId()).orElseThrow().getCompletedAt();

        // A redelivered/duplicate message for the same run, even with a different outcome -
        // must never overwrite the already-recorded result.
        investigationResultService.handleResult(failedResultJson(run.getId(), "TIMEOUT"));

        InvestigationRun reloaded = investigationRunRepository.findById(run.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED);
        assertThat(reloaded.getCompletedAt()).isEqualTo(firstCompletedAt);
        assertThat(reloaded.getFailureReason()).isNull();
    }

    @Test
    void lateDuplicateResultForASupersededRunNeverOverwritesTheNewerRun() {
        Instant now = Instant.now();
        signalIngestionService.ingest(signalFor("payment-service", now));
        InvestigationRun firstRun = onlyRun();
        signalIngestionService.ingest(signalFor("payment-service", now.plusSeconds(30)));

        // firstRun's result arrives, is marked STALE, and launches the follow-up (secondRun).
        investigationResultService.handleResult(completedResultJson(firstRun.getId()));
        InvestigationRun secondRun = investigationRunRepository.findAll().stream()
                .filter(r -> !r.getId().equals(firstRun.getId())).findFirst().orElseThrow();

        // secondRun completes normally.
        investigationResultService.handleResult(completedResultJson(secondRun.getId()));

        // Now a late duplicate of firstRun's ORIGINAL result arrives (e.g. Kafka redelivery) -
        // "Run 1 finishes [again, as a duplicate] after Run 2" must never overwrite Run 2's state.
        investigationResultService.handleResult(completedResultJson(firstRun.getId()));

        InvestigationRun reloadedFirst = investigationRunRepository.findById(firstRun.getId()).orElseThrow();
        InvestigationRun reloadedSecond = investigationRunRepository.findById(secondRun.getId()).orElseThrow();
        assertThat(reloadedFirst.getStatus()).isEqualTo(InvestigationRun.STATUS_STALE); // unchanged, still terminal
        assertThat(reloadedSecond.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED); // untouched by the late duplicate

        Incident incident = onlyIncident();
        assertThat(incident.getCurrentInvestigationRunId()).isEqualTo(secondRun.getId());
        assertThat(incident.isNeedsReinvestigation()).isFalse();
        // No third run was ever launched by the late duplicate.
        assertThat(investigationRunRepository.findAll()).hasSize(2);
    }

    @Test
    void resultForAnUnknownInvestigationRunIdIsDiscardedWithoutError() {
        investigationResultService.handleResult(completedResultJson(UUID.randomUUID()));
        // No exception, no state created - this just proves the defensive lookup path.
        assertThat(investigationRunRepository.findAll()).isEmpty();
    }

    private Incident onlyIncident() {
        List<Incident> all = incidentRepository.findAll();
        assertThat(all).hasSize(1);
        return all.get(0);
    }

    private InvestigationRun onlyRun() {
        List<InvestigationRun> all = investigationRunRepository.findAll();
        assertThat(all).hasSize(1);
        return all.get(0);
    }

    private CanonicalSignalV1 signalFor(String service, Instant observedAt) {
        return new CanonicalSignalV1(
                "evt-" + UUID.randomUUID(), "1.0", "PROMETHEUS", "DB_CONNECTION_PRESSURE", service, "prod",
                "CRITICAL", observedAt, observedAt, Map.of(), Map.of());
    }

    private String completedResultJson(UUID investigationRunId) {
        return "{\"investigationRunId\":\"" + investigationRunId + "\",\"status\":\"COMPLETED\"}";
    }

    private String failedResultJson(UUID investigationRunId, String failureReason) {
        return "{\"investigationRunId\":\"" + investigationRunId + "\",\"status\":\"FAILED\",\"failureReason\":\"" + failureReason + "\"}";
    }
}
