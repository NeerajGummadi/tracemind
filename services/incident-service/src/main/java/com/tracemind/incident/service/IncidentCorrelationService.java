package com.tracemind.incident.service;

import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.domain.Incident;
import com.tracemind.incident.domain.IncidentSignal;
import com.tracemind.incident.domain.OutboxEvent;
import com.tracemind.incident.domain.Signal;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.IncidentSignalRepository;
import com.tracemind.incident.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Incident Correlation Domain (blueprint Section 4). Always called from
 * within SignalIngestionService's transaction - does not open its own.
 */
@Service
public class IncidentCorrelationService {

    private static final Duration CORRELATION_WINDOW = Duration.ofMinutes(5);

    private final IncidentRepository incidentRepository;
    private final IncidentSignalRepository incidentSignalRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public IncidentCorrelationService(
            IncidentRepository incidentRepository,
            IncidentSignalRepository incidentSignalRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.incidentRepository = incidentRepository;
        this.incidentSignalRepository = incidentSignalRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void correlateAndPersist(Signal signal, CanonicalSignalV1 canonicalSignal) {
        Instant windowStart = canonicalSignal.observedAt().minus(CORRELATION_WINDOW);
        List<Incident> candidates = incidentRepository.findCorrelationCandidates(
                canonicalSignal.environment(), canonicalSignal.service(), windowStart);

        boolean isNewIncident = candidates.isEmpty();
        Incident incident;
        if (isNewIncident) {
            incident = Incident.create(nextIncidentNumber(), canonicalSignal);
            incidentRepository.save(incident);
        } else {
            incident = candidates.get(0);
            incident.recordAdditionalSignal(canonicalSignal.observedAt(), canonicalSignal.severity());
        }

        incidentSignalRepository.save(new IncidentSignal(incident.getId(), signal.getId()));

        if (isNewIncident) {
            outboxEventRepository.save(OutboxEvent.investigationRequested(
                    incident, List.of(canonicalSignal.eventId()), objectMapper));
        }
    }

    private String nextIncidentNumber() {
        return "INC-" + incidentRepository.nextIncidentNumberSequenceValue();
    }
}
