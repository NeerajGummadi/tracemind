package com.tracemind.incident.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InvestigationRunTest {

    @Test
    void createStartsRunning() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);

        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_RUNNING);
        assertThat(run.getStartedAt()).isEqualTo(run.getCreatedAt());
        assertThat(run.getCompletedAt()).isNull();
        assertThat(run.getFailureReason()).isNull();
    }

    @Test
    void markCompletedTransitionsFromRunning() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);
        Instant completedAt = Instant.now();

        boolean transitioned = run.markCompleted(completedAt);

        assertThat(transitioned).isTrue();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED);
        assertThat(run.getCompletedAt()).isEqualTo(completedAt);
    }

    @Test
    void markStaleTransitionsFromRunning() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);

        boolean transitioned = run.markStale(Instant.now());

        assertThat(transitioned).isTrue();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_STALE);
    }

    @Test
    void markFailedTransitionsFromRunningAndRecordsReason() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);

        boolean transitioned = run.markFailed("TIMEOUT", Instant.now());

        assertThat(transitioned).isTrue();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_FAILED);
        assertThat(run.getFailureReason()).isEqualTo("TIMEOUT");
    }

    @Test
    void markCompletedIsANoOpOnAnAlreadyCompletedRun() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);
        Instant firstCompletion = Instant.now();
        run.markCompleted(firstCompletion);

        boolean transitioned = run.markCompleted(Instant.now().plusSeconds(60));

        assertThat(transitioned).isFalse();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_COMPLETED);
        // The second (duplicate/late) call must never overwrite the original outcome.
        assertThat(run.getCompletedAt()).isEqualTo(firstCompletion);
    }

    @Test
    void markStaleIsANoOpOnAnAlreadyFailedRun() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);
        run.markFailed("TIMEOUT", Instant.now());

        boolean transitioned = run.markStale(Instant.now());

        assertThat(transitioned).isFalse();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_FAILED);
    }

    @Test
    void markFailedIsANoOpOnAnAlreadyStaleRun() {
        InvestigationRun run = InvestigationRun.create(UUID.randomUUID(), 1, InvestigationRun.TRIGGER_REASON_NEW_INCIDENT);
        run.markStale(Instant.now());

        boolean transitioned = run.markFailed("TIMEOUT", Instant.now());

        assertThat(transitioned).isFalse();
        assertThat(run.getStatus()).isEqualTo(InvestigationRun.STATUS_STALE);
        assertThat(run.getFailureReason()).isNull();
    }
}
