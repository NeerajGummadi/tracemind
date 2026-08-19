package com.tracemind.incident.domain;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.util.UUID;

/** Maps to the "incident_signals" join table (blueprint Section 9). */
@Entity
@Table(name = "incident_signals")
public class IncidentSignal {

    @EmbeddedId
    private IncidentSignalId id;

    protected IncidentSignal() {
        // JPA
    }

    public IncidentSignal(UUID incidentId, UUID signalId) {
        this.id = new IncidentSignalId(incidentId, signalId);
    }
}
