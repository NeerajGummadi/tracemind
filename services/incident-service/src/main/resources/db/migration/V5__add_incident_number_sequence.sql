-- Not specified in the blueprint (Section 9 only defines incidents.incident_number
-- as VARCHAR UNIQUE NOT NULL, e.g. "INC-1001", without saying how it's generated).
-- A Postgres sequence is the simplest source for the numeric suffix.
CREATE SEQUENCE incident_number_seq;
