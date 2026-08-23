package com.tracemind.incident.repository;

import com.tracemind.incident.domain.Signal;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface SignalRepository extends JpaRepository<Signal, UUID> {

    /**
     * Milestone M - provenance for a follow-up InvestigationRun's triggerSignalIds: the
     * signals that arrived after the previous run started (and so motivated
     * needsReinvestigation). Deliberately compares against Signal.createdAt (this
     * service's own ingestion timestamp), not observedAt - observedAt is the alert
     * source's self-reported time (e.g. Alertmanager's startsAt), a different clock with
     * no guaranteed ordering relative to InvestigationRun.createdAt.
     */
    @Query("""
            SELECT s.eventId FROM Signal s
            JOIN IncidentSignal isig ON isig.id.signalId = s.id
            WHERE isig.id.incidentId = :incidentId
              AND s.createdAt > :since
            ORDER BY s.createdAt
            """)
    List<String> findEventIdsForIncidentSince(UUID incidentId, Instant since);
}
