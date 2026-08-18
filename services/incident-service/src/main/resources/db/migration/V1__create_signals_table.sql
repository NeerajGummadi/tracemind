-- docs/architecture/engineering-blueprint.md, Section 9 - signals

CREATE TABLE signals (
    id UUID PRIMARY KEY,
    event_id VARCHAR UNIQUE NOT NULL,
    source VARCHAR NOT NULL,
    signal_type VARCHAR NOT NULL,
    service VARCHAR NOT NULL,
    environment VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    started_at TIMESTAMP,
    observed_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL
);
