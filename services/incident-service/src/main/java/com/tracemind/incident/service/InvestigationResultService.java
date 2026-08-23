package com.tracemind.incident.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.domain.Incident;
import com.tracemind.incident.domain.InvestigationRun;
import com.tracemind.incident.repository.IncidentRepository;
import com.tracemind.incident.repository.InvestigationRunRepository;
import com.tracemind.incident.repository.SignalRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Consumes investigation.results.v1 (Milestone M). Looked up by
 * investigationRunId, not incidentId - a result identifies exactly one run,
 * and an incident may have had several. The evidence/RCA/metrics payload is
 * stored verbatim (JsonNode field extraction, not a full parallel typed
 * mirror of the Python contract) since this service only needs to act on
 * investigationRunId/status/failureReason.
 */
@Service
public class InvestigationResultService {

    private static final Logger log = LoggerFactory.getLogger(InvestigationResultService.class);

    private static final String STATUS_COMPLETED = "COMPLETED";

    private final InvestigationRunRepository investigationRunRepository;
    private final IncidentRepository incidentRepository;
    private final SignalRepository signalRepository;
    private final InvestigationRunLauncher investigationRunLauncher;
    private final ObjectMapper objectMapper;

    public InvestigationResultService(
            InvestigationRunRepository investigationRunRepository,
            IncidentRepository incidentRepository,
            SignalRepository signalRepository,
            InvestigationRunLauncher investigationRunLauncher,
            ObjectMapper objectMapper) {
        this.investigationRunRepository = investigationRunRepository;
        this.incidentRepository = incidentRepository;
        this.signalRepository = signalRepository;
        this.investigationRunLauncher = investigationRunLauncher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleResult(String rawJson) {
        JsonNode node;
        try {
            node = objectMapper.readTree(rawJson);
        } catch (Exception e) {
            log.warn("Discarding malformed investigation.results.v1 message: {}", e.getMessage());
            return;
        }

        UUID runId;
        try {
            runId = UUID.fromString(node.get("investigationRunId").asText());
        } catch (Exception e) {
            log.warn("Discarding investigation.results.v1 message with missing/invalid investigationRunId");
            return;
        }

        Optional<InvestigationRun> maybeRun = investigationRunRepository.findById(runId);
        if (maybeRun.isEmpty()) {
            log.warn("Received result for unknown investigationRunId={}", runId);
            return;
        }
        InvestigationRun run = maybeRun.get();

        String status = node.get("status").asText();
        boolean transitioned;
        if (STATUS_COMPLETED.equals(status)) {
            Incident incident = incidentRepository.findById(run.getIncidentId())
                    .orElseThrow(() -> new IllegalStateException("InvestigationRun " + runId + " has no incident " + run.getIncidentId()));
            transitioned = (run.getInputSignalVersion() == incident.getSignalVersion())
                    ? run.markCompleted(Instant.now())
                    : run.markStale(Instant.now());
        } else {
            String failureReason = node.hasNonNull("failureReason") ? node.get("failureReason").asText() : null;
            transitioned = run.markFailed(failureReason, Instant.now());
        }

        if (!transitioned) {
            // Already terminal: a duplicate or out-of-order delivery for a run that was
            // already finalized. Idempotent no-op - never overwrite an existing outcome.
            log.info("Ignoring result for investigationRunId={} - already {}", runId, run.getStatus());
            return;
        }
        run.setResultPayload(rawJson);
        investigationRunRepository.save(run);

        Incident incident = incidentRepository.findById(run.getIncidentId())
                .orElseThrow(() -> new IllegalStateException("InvestigationRun " + runId + " has no incident " + run.getIncidentId()));

        // Out-of-order protection: only the run the incident currently considers
        // authoritative may drive follow-up decisions. A late result for a run that's
        // since been superseded (the incident has already moved on to a newer run) is
        // recorded on its own row above, but must never re-trigger or re-clear state.
        if (!run.getId().equals(incident.getCurrentInvestigationRunId())) {
            return;
        }

        if (incident.isNeedsReinvestigation()) {
            List<String> triggerSignalIds = signalRepository.findEventIdsForIncidentSince(incident.getId(), run.getCreatedAt());
            investigationRunLauncher.launch(incident, InvestigationRun.TRIGGER_REASON_REINVESTIGATION, triggerSignalIds);
            incident.setNeedsReinvestigation(false);
        }
    }
}
