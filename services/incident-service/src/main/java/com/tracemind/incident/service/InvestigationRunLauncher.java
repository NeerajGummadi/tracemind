package com.tracemind.incident.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.domain.Incident;
import com.tracemind.incident.domain.InvestigationRun;
import com.tracemind.incident.domain.OutboxEvent;
import com.tracemind.incident.repository.InvestigationRunRepository;
import com.tracemind.incident.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Shared "create a run, point the incident at it, write the outbox event"
 * plumbing (Milestone M) - used both when a signal arrives with no
 * investigation currently running (IncidentCorrelationService) and when a
 * follow-up run is launched after a result is consumed
 * (InvestigationResultService). Always called from within an existing
 * @Transactional method - does not open its own transaction.
 */
@Service
public class InvestigationRunLauncher {

    private final InvestigationRunRepository investigationRunRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public InvestigationRunLauncher(
            InvestigationRunRepository investigationRunRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.investigationRunRepository = investigationRunRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public InvestigationRun launch(Incident incident, String triggerReason, List<String> triggerSignalIds) {
        InvestigationRun run = InvestigationRun.create(incident.getId(), incident.getSignalVersion(), triggerReason);
        investigationRunRepository.save(run);
        incident.setCurrentInvestigationRun(run.getId());
        outboxEventRepository.save(OutboxEvent.investigationRequested(incident, run, triggerSignalIds, objectMapper));
        return run;
    }
}
