-- Milestone M - one row per AI investigation attempt (distinct from the
-- incident itself, which can outlive many runs).

CREATE TABLE investigation_runs (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id),
    input_signal_version INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    trigger_reason VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    failure_reason VARCHAR,
    result_payload JSONB
);

CREATE INDEX idx_investigation_runs_incident_id ON investigation_runs(incident_id);
