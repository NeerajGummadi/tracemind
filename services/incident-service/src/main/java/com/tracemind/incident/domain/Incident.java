package com.tracemind.incident.domain;

import com.tracemind.incident.contract.CanonicalSignalV1;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Maps to the "incidents" table (blueprint Section 9). Status transitions
 * are kept behind explicit methods rather than raw setters (blueprint
 * Section 8: "statuses should not be modified arbitrarily").
 */
@Entity
@Table(name = "incidents")
public class Incident {

    public static final String STATUS_QUEUED = "QUEUED";

    @Id
    private UUID id;

    @Column(name = "incident_number", nullable = false, unique = true)
    private String incidentNumber;

    @Column(nullable = false)
    private String title;

    @Column(name = "primary_service", nullable = false)
    private String primaryService;

    @Column(nullable = false)
    private String environment;

    @Column(nullable = false)
    private String severity;

    @Column(nullable = false)
    private String status;

    @Column(name = "first_observed_at", nullable = false)
    private Instant firstObservedAt;

    @Column(name = "last_observed_at", nullable = false)
    private Instant lastObservedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Incident() {
        // JPA
    }

    private Incident(UUID id, String incidentNumber, String title, String primaryService, String environment,
                      String severity, String status, Instant firstObservedAt, Instant lastObservedAt,
                      Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.incidentNumber = incidentNumber;
        this.title = title;
        this.primaryService = primaryService;
        this.environment = environment;
        this.severity = severity;
        this.status = status;
        this.firstObservedAt = firstObservedAt;
        this.lastObservedAt = lastObservedAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    /**
     * Status starts at QUEUED, not DETECTED: correlation and the decision to
     * request an investigation happen in the same transaction as incident
     * creation, so there's no separate not-yet-triaged state in this flow.
     */
    public static Incident create(String incidentNumber, CanonicalSignalV1 triggeringSignal) {
        Instant now = Instant.now();
        String title = triggeringSignal.service() + " - " + triggeringSignal.signalType();
        return new Incident(
                UUID.randomUUID(),
                incidentNumber,
                title,
                triggeringSignal.service(),
                triggeringSignal.environment(),
                triggeringSignal.severity(),
                STATUS_QUEUED,
                triggeringSignal.observedAt(),
                triggeringSignal.observedAt(),
                now,
                now);
    }

    /** Attaches an additional correlated signal: bumps lastObservedAt and escalates severity if warranted. */
    public void recordAdditionalSignal(Instant signalObservedAt, String signalSeverity) {
        if (signalObservedAt.isAfter(this.lastObservedAt)) {
            this.lastObservedAt = signalObservedAt;
        }
        if (SeverityRanking.isHigherThan(signalSeverity, this.severity)) {
            this.severity = signalSeverity;
        }
        this.updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getIncidentNumber() {
        return incidentNumber;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getPrimaryService() {
        return primaryService;
    }

    public String getSeverity() {
        return severity;
    }

    public String getStatus() {
        return status;
    }

    public Instant getFirstObservedAt() {
        return firstObservedAt;
    }

    public Instant getLastObservedAt() {
        return lastObservedAt;
    }
}
