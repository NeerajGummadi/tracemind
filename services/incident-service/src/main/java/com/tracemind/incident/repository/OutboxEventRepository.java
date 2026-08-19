package com.tracemind.incident.repository;

import com.tracemind.incident.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Atomically claims one pending row for this worker: FOR UPDATE SKIP LOCKED means a
     * concurrent poller (another incident-service instance, or an overlapping poll cycle)
     * skips whatever row we're already holding rather than blocking on it or double-claiming it.
     * Written as literal SQL rather than via @Lock/@QueryHints to avoid relying on Hibernate's
     * lock-mode-to-SQL translation.
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<OutboxEvent> claimOnePending();
}
