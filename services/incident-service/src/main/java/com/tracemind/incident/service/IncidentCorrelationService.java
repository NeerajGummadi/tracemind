package com.tracemind.incident.service;

import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.domain.Incident;
import com.tracemind.incident.domain.IncidentSignal;
import com.tracemind.incident.domain.InvestigationRun;
import com.tracemind.incident.domain.Signal;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.IncidentSignalRepository;
import com.tracemind.incident.repository.InvestigationRunRepository;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Incident Correlation Domain (blueprint Section 4) plus Milestone M's
 * investigation-coalescing algorithm. Always called from within
 * SignalIngestionService's transaction - does not open its own.
 */
@Service
public class IncidentCorrelationService {

    private static final Duration CORRELATION_WINDOW = Duration.ofMinutes(5);

    private final IncidentRepository incidentRepository;
    private final IncidentSignalRepository incidentSignalRepository;
    private final InvestigationRunRepository investigationRunRepository;
    private final InvestigationRunLauncher investigationRunLauncher;

    public IncidentCorrelationService(
            IncidentRepository incidentRepository,
            IncidentSignalRepository incidentSignalRepository,
            InvestigationRunRepository investigationRunRepository,
            InvestigationRunLauncher investigationRunLauncher) {
        this.incidentRepository = incidentRepository;
        this.incidentSignalRepository = incidentSignalRepository;
        this.investigationRunRepository = investigationRunRepository;
        this.investigationRunLauncher = investigationRunLauncher;
    }

    public void correlateAndPersist(Signal signal, CanonicalSignalV1 canonicalSignal) {
        Instant windowStart = canonicalSignal.observedAt().minus(CORRELATION_WINDOW);
        List<Incident> candidates = incidentRepository.findCorrelationCandidates(
                canonicalSignal.environment(), canonicalSignal.service(), windowStart);

        boolean isNewIncident = candidates.isEmpty();
        Incident incident;
        if (isNewIncident) {
            // Case A: no incident found - create it and launch its first investigation.
            // Incident.create() pre-assigns the ID, so Spring Data's isNew() check sees a
            // non-null @Id and routes save() through merge() rather than persist() - merge()
            // returns a *different*, newly-managed instance, so the return value must be
            // captured and reused for every mutation from here on, or those mutations
            // (launch()'s setCurrentInvestigationRun, in particular) are silently lost.
            incident = incidentRepository.save(Incident.create(nextIncidentNumber(), canonicalSignal));
            incidentSignalRepository.save(new IncidentSignal(incident.getId(), signal.getId()));
            investigationRunLauncher.launch(incident, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT,
                    List.of(canonicalSignal.eventId()));
        } else {
            // Case B: incident already exists - append the signal, then either coalesce into
            // needsReinvestigation (a run is already active) or launch a fresh run.
            incident = candidates.get(0);
            incident.recordAdditionalSignal(canonicalSignal.observedAt(), canonicalSignal.severity());
            incidentSignalRepository.save(new IncidentSignal(incident.getId(), signal.getId()));

            if (hasActiveInvestigation(incident)) {
                incident.setNeedsReinvestigation(true);
            } else {
                investigationRunLauncher.launch(incident, InvestigationRun.TRIGGER_REASON_REINVESTIGATION,
                        List.of(canonicalSignal.eventId()));
                incident.setNeedsReinvestigation(false);
            }
        }
    }

    private boolean hasActiveInvestigation(Incident incident) {
        if (incident.getCurrentInvestigationRunId() == null) {
            return false;
        }
        Optional<InvestigationRun> currentRun = investigationRunRepository.findById(incident.getCurrentInvestigationRunId());
        return currentRun.isPresent() && currentRun.get().isActive();
    }

    private String nextIncidentNumber() {
        return "INC-" + incidentRepository.nextIncidentNumberSequenceValue();
    }
}
