-- Optimization 3 (incident-service-bottleneck-analysis.md): IncidentRepository
-- .findCorrelationCandidates() filters on (environment, primary_service, status,
-- last_observed_at) with no supporting index, forcing a full sequential scan of
-- the incidents table on every single signal (confirmed via EXPLAIN ANALYZE:
-- ~2.7ms scanning 7,334 rows). Column order matches the query exactly: the two
-- equality predicates (environment, primary_service) lead, so Postgres can
-- narrow directly to the rows for one specific service+environment instead of
-- scanning the whole table; status (the inequality predicate) and
-- last_observed_at (the range predicate and ORDER BY column) follow, filtering
-- and sorting within that already-small subset. No change to the query itself
-- or to any application code.

CREATE INDEX idx_incidents_correlation
    ON incidents (environment, primary_service, status, last_observed_at);
