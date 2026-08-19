package com.tracemind.incident.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.contract.CanonicalSignalV1;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/** Maps to the "signals" table (blueprint Section 9). */
@Entity
@Table(name = "signals")
public class Signal {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String source;

    @Column(name = "signal_type", nullable = false)
    private String signalType;

    @Column(nullable = false)
    private String service;

    @Column(nullable = false)
    private String environment;

    @Column(nullable = false)
    private String severity;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "observed_at", nullable = false)
    private Instant observedAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Signal() {
        // JPA
    }

    private Signal(UUID id, String eventId, String source, String signalType, String service,
                    String environment, String severity, Instant startedAt, Instant observedAt,
                    String payload, Instant createdAt) {
        this.id = id;
        this.eventId = eventId;
        this.source = source;
        this.signalType = signalType;
        this.service = service;
        this.environment = environment;
        this.severity = severity;
        this.startedAt = startedAt;
        this.observedAt = observedAt;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public static Signal from(CanonicalSignalV1 canonicalSignal, ObjectMapper objectMapper) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(canonicalSignal);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize CanonicalSignalV1 for storage", e);
        }
        return new Signal(
                UUID.randomUUID(),
                canonicalSignal.eventId(),
                canonicalSignal.source(),
                canonicalSignal.signalType(),
                canonicalSignal.service(),
                canonicalSignal.environment(),
                canonicalSignal.severity(),
                canonicalSignal.startedAt(),
                canonicalSignal.observedAt(),
                payloadJson,
                Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }
}
