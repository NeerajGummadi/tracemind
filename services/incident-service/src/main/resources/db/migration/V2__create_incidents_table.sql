-- docs/architecture/engineering-blueprint.md, Section 9 - incidents

CREATE TABLE incidents (
    id UUID PRIMARY KEY,
    incident_number VARCHAR UNIQUE NOT NULL,
    title VARCHAR NOT NULL,
    primary_service VARCHAR NOT NULL,
    environment VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    first_observed_at TIMESTAMP NOT NULL,
    last_observed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
