package com.tracemind.incident.repository;

import com.tracemind.incident.domain.Incident;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface IncidentRepository extends JpaRepository<Incident, UUID> {

    /** Correlation criteria per blueprint Section 4: same environment, same primary service, still open, within window. */
    @Query("""
            SELECT i FROM Incident i
            WHERE i.environment = :environment
              AND i.primaryService = :primaryService
              AND i.status <> 'COMPLETED'
              AND i.lastObservedAt >= :windowStart
            ORDER BY i.lastObservedAt DESC
            """)
    List<Incident> findCorrelationCandidates(String environment, String primaryService, Instant windowStart);

    @Query(value = "SELECT nextval('incident_number_seq')", nativeQuery = true)
    long nextIncidentNumberSequenceValue();
}
