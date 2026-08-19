package com.tracemind.incident.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class IncidentSignalId implements Serializable {

    @Column(name = "incident_id")
    private UUID incidentId;

    @Column(name = "signal_id")
    private UUID signalId;

    protected IncidentSignalId() {
        // JPA
    }

    public IncidentSignalId(UUID incidentId, UUID signalId) {
        this.incidentId = incidentId;
        this.signalId = signalId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IncidentSignalId that)) {
            return false;
        }
        return Objects.equals(incidentId, that.incidentId) && Objects.equals(signalId, that.signalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(incidentId, signalId);
    }
}
