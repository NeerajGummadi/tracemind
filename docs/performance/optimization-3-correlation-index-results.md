# Optimization 3 — Incident Correlation Query Index

A single, targeted index fix following directly from `docs/performance/incident-service-bottleneck-
analysis.md`'s finding that `IncidentRepository.findCorrelationCandidates` performs a full
sequential scan on every signal. **No business logic, Kafka configuration, or HikariCP setting was
touched.** Optimization 1 (concurrency=3) and Optimization 2 (outbox poll-interval=50ms) remain
unchanged and were verified unaffected throughout.

---

## Before implementation

### 1. Exact query inspected

```java
@Query("""
        SELECT i FROM Incident i
        WHERE i.environment = :environment
          AND i.primaryService = :primaryService
          AND i.status <> 'COMPLETED'
          AND i.lastObservedAt >= :windowStart
        ORDER BY i.lastObservedAt DESC
        """)
List<Incident> findCorrelationCandidates(String environment, String primaryService, Instant windowStart);
```
(`services/incident-service/.../repository/IncidentRepository.java` — unchanged, not touched by
this optimization.)

### 2. Index column order verified against the predicates

| Query clause | Type | Index position |
|---|---|---|
| `environment = :environment` | equality | 1st |
| `primaryService = :primaryService` | equality | 2nd |
| `status <> 'COMPLETED'` | inequality | 3rd |
| `lastObservedAt >= :windowStart` (also the `ORDER BY ... DESC` column) | range/sort | 4th |

`(environment, primary_service, status, last_observed_at)` places the two equality predicates first
(standard composite-index practice — equality columns must lead), then the inequality predicate,
then the range/sort predicate — an exact match to the query's own predicate structure.

### 3. Why this should reduce the sequential-scan cost

With `environment` and `primary_service` as the leading, equal-match columns, Postgres can narrow
directly to the small number of index entries for one specific service+environment combination,
instead of scanning the entire `incidents` table (confirmed at 7,334 rows pre-optimization). The
remaining predicates (`status`, `last_observed_at`) then filter within that already-tiny subset.
This turns an O(table size) scan into an O(rows for this one service) scan — and per this project's
own workload design (a 5-minute correlation window, one or a handful of historical incidents per
service+environment), that subset is small regardless of how large the overall table grows.

### 4. Correlation algorithm — unchanged

No change to `IncidentCorrelationService`, `IncidentRepository`, or any business logic. This is a
pure schema addition.

## Implementation

`V9__add_incident_correlation_index.sql` (Flyway):

```sql
CREATE INDEX idx_incidents_correlation
    ON incidents (environment, primary_service, status, last_observed_at);
```

Applied automatically by Flyway on `incident-service` startup — verified via the migration log
(`"Migrating schema \"public\" to version \"9 - add incident correlation index\""`) and directly
against the running database:

```
Indexes:
    "incidents_pkey" PRIMARY KEY, btree (id)
    "idx_incidents_correlation" btree (environment, primary_service, status, last_observed_at)
    "incidents_incident_number_key" UNIQUE CONSTRAINT, btree (incident_number)
```

## Validation

### 1. Full test suite

**28/28 pass, `BUILD SUCCESS`** — the migration also applied cleanly in all 4 isolated Testcontainer
instances used by the integration tests, confirming it is a valid, general migration, not specific
to the long-lived dev database's current state.

### 2. `EXPLAIN ANALYZE` — confirms the intended index is used

| | Before (V8, no index) | After (V9) |
|---|---|---|
| Plan | `Seq Scan on incidents`, `Rows Removed by Filter: 7334` | **`Index Scan using idx_incidents_correlation on incidents`** |
| Buffers | `shared hit=1 read=214` | `shared hit=3 read=2` (or `hit=5` on a warm cache) |
| Execution Time | **2.748 ms** | **0.095 ms** (and 0.054ms on a second sample) |

**~29× faster on this exact query, ~42× fewer buffer reads.** Postgres pushes `environment`,
`primary_service`, and `last_observed_at` into the `Index Cond` (the index-narrowing condition) and
`status` into a `Filter` applied to the already-tiny matched set — exactly the mechanism predicted
in point 3 above.

### 3. Correlation-query latency, before vs. after (live load, same instrumentation used in the
bottleneck analysis, temporarily re-applied and removed again afterward)

| Metric | 900 RPS before | 900 RPS after | 1200 RPS before | 1200 RPS after |
|---|---|---|---|---|
| `correlationQuery` mean | 1.050 ms | **0.489 ms** | 1.012 ms | **0.452 ms** |
| `correlationQuery` typical p50/p95/p99 (bucketed) | <1-2ms / <2ms / <2-5ms | **<0.5-1ms / <1-2ms / <1-2ms** | (same order as 900) | (same order as 900) |
| `listenerTotal` (whole transaction) mean | 3.285 ms | **2.697 ms** | 3.187 ms | **2.418 ms** |

**Correlation-query mean improved ~53-55%** (roughly 2.2× faster) at both tiers — closely matching
the isolated `EXPLAIN ANALYZE` finding, now confirmed under live concurrent load. **Total
per-message transaction time improved ~18-24%**, consistent with the correlation query having been
roughly 30-40% of total per-message time (per the bottleneck analysis) and now contributing much
less.

### 4. Postgres CPU, before vs. after

| | 900 RPS before | 900 RPS after | 1200 RPS before | 1200 RPS after |
|---|---|---|---|---|
| Postgres CPU mean | 95.5% | **66.5%** (−30%) | 116.5% | **76.2%** (−35%) |
| Postgres CPU max | 170.6% | 133.4% | 160.95% | 136.81% |

A real, substantial reduction in Postgres CPU load at both tiers, consistent with eliminating a
full-table scan on every single signal.

### 5. Consumer processing throughput / Kafka lag behavior

| | 900 RPS before | 900 RPS after | 1200 RPS before | 1200 RPS after |
|---|---|---|---|---|
| Achieved RPS | 895.85 | 895.89 | 1193.78 | 1193.78 |
| HTTP p99 | 9.1ms | 9.68ms | 10.05ms | 9.56ms |
| Max lag during load | ~1,300 (oscillating 700-1,300) | **~800 early, then oscillating ~150-300** | **14,020, still climbing when load stopped** | **3,672, still climbing when load stopped** |
| Lag drain after load stops | drains to 0 within ~9s | drains to 0 within ~9s | had not started draining when the run ended | **drains to 0 within ~10s** |
| Approx. lag growth rate at 1200 RPS | ~14,020 / 40s ≈ **350/s** | ~3,672 / 40.7s ≈ **90/s** | — | **~74% reduction in lag growth rate (~3.9×)** |

**900 RPS**: already sustainable before, now sustainable with a visibly smaller, faster-clearing
backlog and markedly lower Postgres load — more comfortable headroom, not just the same ceiling.

**1200 RPS: still not sustainable** — lag still grows continuously throughout the 40-second load
window rather than stabilizing, which is the same qualitative "unsustainable" signature as before.
**However, the growth rate itself improved dramatically** (~350/s → ~90/s, roughly a 74% reduction),
and the peak lag reached for the identical offered load and duration dropped from 14,020 to 3,672 —
a ~74% reduction. This is a real, substantial improvement in *how close* the system now sits to the
1200 RPS boundary, not merely noise.

## Correctness

| | 900 RPS | 1200 RPS |
|---|---|---|
| Signals persisted | 36,000/36,000 (exact) | 48,000/48,000 (exact) |
| Incidents created | 100 | 100 |
| Duplicate `event_id`s | 0 | 0 |
| Kafka consumer lag after run | 0 on all partitions | 0 on all partitions |

Zero regressions. Correctness fully preserved.

## Does 1200 RPS become sustainable?

**No.** Per the stopping rule: the index materially improved both query/service time (~53-55%
faster correlation query, ~18-24% faster total per-message time, ~30-35% lower Postgres CPU) and lag
growth rate (~74% slower), but 1200 RPS still produces continuously growing, unbounded-within-the-
load-window consumer lag. **Conclusion: partition/consumer parallelism is now the next scaling
boundary**, exactly as the stopping rule anticipated. This is consistent with the bottleneck
analysis's Pattern A finding (parallelism-limited, with real headroom on every shared resource) —
the index removed a real, measurable inefficiency riding on top of that limit, but did not remove
the limit itself, since it did not change the number of parallel consumer threads.

## Exact improvement percentages

| Metric | Before | After | Improvement |
|---|---|---|---|
| Correlation query latency (EXPLAIN ANALYZE, isolated) | 2.748ms | 0.095ms | **~96.5% (~29×)** |
| Correlation query mean (live, 900 RPS) | 1.050ms | 0.489ms | **~53.4%** |
| Correlation query mean (live, 1200 RPS) | 1.012ms | 0.452ms | **~55.3%** |
| Total per-message (`listenerTotal`) mean (900 RPS) | 3.285ms | 2.697ms | **~17.9%** |
| Total per-message (`listenerTotal`) mean (1200 RPS) | 3.187ms | 2.418ms | **~24.1%** |
| Postgres CPU mean (900 RPS) | 95.5% | 66.5% | **~30.4%** |
| Postgres CPU mean (1200 RPS) | 116.5% | 76.2% | **~34.6%** |
| Peak consumer lag at 1200 RPS (identical 40s load) | 14,020 | 3,672 | **~73.8%** |
| Approx. lag growth rate at 1200 RPS | ~350/s | ~90/s | **~74.3%** |

## Cleanup confirmation

All diagnostic instrumentation used to obtain the "live, before/after" per-message timings
(temporarily re-applied, identical to the already-validated instrumentation from the bottleneck
analysis) was removed after data collection — verified via `mvn test` (28/28 passing before
instrumentation, immediately after adding it, and again after removing it) and via `git diff`:

```
$ git status --short services/incident-service/
 M services/incident-service/src/main/resources/application.yml
 M services/incident-service/src/test/java/com/tracemind/incident/service/InvestigationLifecycleIntegrationTest.java
 M services/incident-service/src/test/java/com/tracemind/incident/service/SignalIngestionServiceIntegrationTest.java
?? services/incident-service/src/main/resources/db/migration/V9__add_incident_correlation_index.sql
```

Only the pre-existing Optimization 2 changes plus the new, intentional `V9` migration remain. No
other file was touched. `incident-service` is running on this clean build; concurrency remains 3;
consumer lag is 0.

## Final verdict

**Optimization 3 accepted and kept.** The composite index is a correct, minimal, low-risk fix that
delivers a real, substantial improvement (correlation query ~2.2× faster live, ~29× faster in
isolation; Postgres CPU down ~30-35%; 1200 RPS lag growth rate down ~74%) with zero correctness
impact and no change to business logic, Kafka topology, consumer concurrency, or connection pool
configuration. It does not, and was not expected to, make 1200 RPS sustainable on its own — per the
stopping rule, that would require increasing partition/consumer parallelism, which is explicitly out
of scope here and was not attempted.

No further optimization was started. Stopping here per instruction.
