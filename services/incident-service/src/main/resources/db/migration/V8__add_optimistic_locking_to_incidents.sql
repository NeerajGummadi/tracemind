-- Milestone N, Test D - fixes a real lost-update race discovered under storm
-- load: SignalConsumerListener (signals.received.v1) and
-- InvestigationResultConsumerListener (investigation.results.v1) run on
-- independent Kafka consumer threads and can concurrently read-modify-write
-- the same incidents row (e.g. one thread incrementing signal_version while
-- the other sets current_investigation_run_id/needs_reinvestigation),
-- silently losing whichever update commits first. Hibernate's @Version
-- optimistic locking makes the second-committing transaction fail instead
-- of silently overwriting - both listeners already propagate uncaught
-- exceptions without acking, so Kafka redelivers and the retry succeeds
-- against the current row version. No new resilience mechanism needed.

ALTER TABLE incidents ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
