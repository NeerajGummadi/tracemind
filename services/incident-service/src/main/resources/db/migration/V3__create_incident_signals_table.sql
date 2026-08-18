-- docs/architecture/engineering-blueprint.md, Section 9 - incident_signals

CREATE TABLE incident_signals (
    incident_id UUID REFERENCES incidents(id),
    signal_id UUID REFERENCES signals(id),
    PRIMARY KEY (incident_id, signal_id)
);
