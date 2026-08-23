package com.tracemind.incident.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Maps to the "investigation_runs" table (Milestone M). One row per AI
 * investigation attempt - distinct from Incident, which can outlive many
 * runs. Status transitions are guarded (only from an active state), so a
 * duplicate or out-of-order investigation.results.v1 message can never
 * overwrite an already-terminal run - see markCompleted/markStale/markFailed.
 */
@Entity
@Table(name = "investigation_runs")
public class InvestigationRun {

    public static final String STATUS_REQUESTED = "REQUESTED";
    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_STALE = "STALE";

    public static final String TRIGGER_REASON_NEW_INCIDENT = "NEW_INCIDENT";
    public static final String TRIGGER_REASON_REINVESTIGATION = "REINVESTIGATION";

    @Id
    private UUID id;

    @Column(name = "incident_id", nullable = false)
    private UUID incidentId;

    @Column(name = "input_signal_version", nullable = false)
    private int inputSignalVersion;

    @Column(nullable = false)
    private String status;

    @Column(name = "trigger_reason", nullable = false)
    private String triggerReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "failure_reason")
    private String failureReason;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "result_payload")
    private String resultPayload;

    protected InvestigationRun() {
        // JPA
    }

    private InvestigationRun(UUID id, UUID incidentId, int inputSignalVersion, String status, String triggerReason,
                              Instant createdAt, Instant startedAt) {
        this.id = id;
        this.incidentId = incidentId;
        this.inputSignalVersion = inputSignalVersion;
        this.status = status;
        this.triggerReason = triggerReason;
        this.createdAt = createdAt;
        this.startedAt = startedAt;
    }

    /**
     * There is no separate "investigation.started" event in this milestone's
     * topology (adding one would be a new Kafka contract, out of scope), so
     * REQUESTED and RUNNING are not independently observable from Incident
     * Service's side - a run is considered RUNNING (in-flight, blocking new
     * runs for its incident) from the instant Incident Service commits to
     * requesting it. REQUESTED remains a valid status value for forward
     * compatibility with a future ack-based transition.
     */
    public static InvestigationRun create(UUID incidentId, int inputSignalVersion, String triggerReason) {
        Instant now = Instant.now();
        return new InvestigationRun(UUID.randomUUID(), incidentId, inputSignalVersion, STATUS_RUNNING, triggerReason, now, now);
    }

    /** True while this run is REQUESTED or RUNNING - the "in-flight, blocks new runs for this incident" window. */
    public boolean isActive() {
        return STATUS_REQUESTED.equals(status) || STATUS_RUNNING.equals(status);
    }

    /** Returns false (no-op) if this run is already terminal - a duplicate or late result must never overwrite it. */
    public boolean markCompleted(Instant completedAt) {
        if (!isActive()) {
            return false;
        }
        this.status = STATUS_COMPLETED;
        this.completedAt = completedAt;
        return true;
    }

    /** Same guard as markCompleted - used when the run's RCA is superseded by a newer signalVersion. */
    public boolean markStale(Instant completedAt) {
        if (!isActive()) {
            return false;
        }
        this.status = STATUS_STALE;
        this.completedAt = completedAt;
        return true;
    }

    public boolean markFailed(String failureReason, Instant completedAt) {
        if (!isActive()) {
            return false;
        }
        this.status = STATUS_FAILED;
        this.failureReason = failureReason;
        this.completedAt = completedAt;
        return true;
    }

    public void setResultPayload(String resultPayload) {
        this.resultPayload = resultPayload;
    }

    public UUID getId() {
        return id;
    }

    public UUID getIncidentId() {
        return incidentId;
    }

    public int getInputSignalVersion() {
        return inputSignalVersion;
    }

    public String getStatus() {
        return status;
    }

    public String getTriggerReason() {
        return triggerReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getResultPayload() {
        return resultPayload;
    }
}
