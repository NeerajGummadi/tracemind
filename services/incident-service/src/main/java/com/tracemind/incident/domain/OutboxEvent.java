package com.tracemind.incident.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.contract.InvestigationRequestedV1;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Maps to the "outbox_events" table (blueprint Sections 9-10). */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String EVENT_TYPE_INVESTIGATION_REQUESTED = "investigation.requested";

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
        // JPA
    }

    private OutboxEvent(UUID id, String aggregateType, UUID aggregateId, String eventType, String payload,
                         String status, Instant createdAt) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getEventType() {
        return eventType;
    }

    public String getStatus() {
        return status;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void markPublished(Instant publishedAt) {
        this.status = STATUS_PUBLISHED;
        this.publishedAt = publishedAt;
    }

    public static OutboxEvent pending(String aggregateType, UUID aggregateId, String eventType, String payload) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId, eventType, payload,
                STATUS_PENDING, Instant.now());
    }

    public static OutboxEvent investigationRequested(Incident incident, List<String> triggerSignalIds,
                                                       ObjectMapper objectMapper) {
        InvestigationRequestedV1 event = new InvestigationRequestedV1(
                "evt-" + UUID.randomUUID(),
                "1.0",
                incident.getIncidentNumber(),
                incident.getPrimaryService(),
                incident.getEnvironment(),
                incident.getSeverity(),
                incident.getFirstObservedAt(),
                incident.getLastObservedAt(),
                triggerSignalIds);
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize InvestigationRequestedV1 for storage", e);
        }
        return new OutboxEvent(
                UUID.randomUUID(),
                "Incident",
                incident.getId(),
                EVENT_TYPE_INVESTIGATION_REQUESTED,
                payloadJson,
                STATUS_PENDING,
                Instant.now());
    }
}
