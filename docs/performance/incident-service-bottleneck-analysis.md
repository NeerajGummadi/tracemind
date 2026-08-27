# Incident Service Bottleneck Analysis — Why ~900 RPS, Not ~1200 RPS

Diagnostic-only investigation into where Incident Service spends its per-message processing time,
to determine why the 3-consumer configuration (Optimization 1) sustains ~900 signals/sec but not
~1200. **No optimization was performed. All temporary diagnostic instrumentation has been removed**
(verified via `mvn test` — 28/28 pass both before instrumentation and after removal — and `git
diff`, confirmed at the end of this document to contain only the pre-existing intentional
Optimization 1/2 changes).

---

## Step 1 — Exact hot path (from the current code, before any instrumentation)

```
SignalConsumerListener.onMessage(signal, ack)              [@KafkaListener, NOT @Transactional]
  ↓ signalIngestionService.ingest(signal)                   — crosses into a different bean's proxy
SignalIngestionService.ingest(canonicalSignal)              [@Transactional — THIS is the real
                                                              transaction boundary: begin here,
                                                              commit on normal return]
  ↓ signalRepository.saveAndFlush(signal)                   — signal insert, flushed immediately
  ↓ correlationService.correlateAndPersist(signal, ...)      — same bean call within the caller's
                                                                already-open transaction (its own
                                                                javadoc confirms: "does not open
                                                                its own")
IncidentCorrelationService.correlateAndPersist(...)          [no annotation - relies on the caller's
                                                               open transaction]
  ↓ incidentRepository.findCorrelationCandidates(env, service, windowStart)   — plain SELECT,
                                                                                 no lock
  ↓ Case A (no candidate): incidentRepository.save(Incident.create(...))     — INSERT
  ↓        incidentSignalRepository.save(new IncidentSignal(...))            — INSERT
  ↓        investigationRunLauncher.launch(...)              → InvestigationRunLauncher.launch()
                                                                 — investigationRunRepository.save()
                                                                   (INSERT) + outboxEventRepository
                                                                   .save() (INSERT)
  ↓ Case B (candidate exists): incident.recordAdditionalSignal(...)          — in-memory mutation,
                                                                                 dirty-checked at
                                                                                 flush, no explicit
                                                                                 call
  ↓        incidentSignalRepository.save(new IncidentSignal(...))            — INSERT
  ↓        hasActiveInvestigation(incident) → investigationRunRepository.findById(...)  — PK lookup
  ↓        either incident.setNeedsReinvestigation(true) (mutation only) or launch() again
[transaction commits on normal method return - Hibernate flushes dirty entities + INSERTs here]
  ↓ back in onMessage(): ack.acknowledge()
```

**Important correction to a prior assumption in this project**: unlike the Outbox Publisher
(where self-invocation was found to defeat `@Transactional`), **this path has no self-invocation
problem** — `SignalConsumerListener` and `SignalIngestionService` are different beans, so the call
genuinely crosses the Spring proxy and `@Transactional` on `ingest()` applies for real. **The
entire signal-insert → correlation → incident-write → outbox-write sequence is one real,
atomic transaction**, exactly as the code's structure implies.

**Locks / optimistic locking**: `Incident` carries `@Version` (added in Milestone N for the
signal-consumer vs. result-consumer race). `findCorrelationCandidates` takes **no explicit row
lock** (plain JPQL `SELECT`, no `FOR UPDATE`). **Whether multiple signals can contend on the same
incident row**: `signals.received.v1` is partitioned by `environment:service` (this project's
established invariant), so every signal for a given incident's key always lands on the *same*
partition and is processed by the *same* single consumer thread, in order — cross-thread
contention on one incident from the signal side is structurally prevented by partition-key
locality, not by any lock. The `@Version` field exists for the *other* known contention source
(the independent `investigation.results.v1` consumer thread), which is not live in this benchmark
(investigation-service is stopped, per this project's standing Test B isolation methodology).

**A priori candidates stated before benchmarking** (per instruction, not concluded yet at this
point): the correlation query (unindexed filter columns, growing table), the transaction commit
itself (unmeasured without invasive JDBC hooks), Case B's extra `hasActiveInvestigation` lookup,
and simple parallelism (3 threads × some fixed per-message cost).

## Step 2 — Instrumentation added (removed after data collection)

Lightweight `nanoTime` timing into in-memory bucketed histograms (no per-message logging), dumped
as an aggregate summary every 2 seconds by a self-contained scheduled reporter:

- `listenerTotal` — wraps the entire `signalIngestionService.ingest(signal)` call in the listener
  (also counts total messages processed and optimistic-lock conflicts via a catch-log-and-rethrow
  around `ObjectOptimisticLockingFailureException` — behavior unchanged, since it's always
  rethrown).
- `signalPersist` — wraps `signalRepository.saveAndFlush(signal)`.
- `correlationQuery` — wraps `findCorrelationCandidates(...)`.
- `incidentInsert` — wraps the Case A `incidentRepository.save(Incident.create(...))` call only.
- `investigationRunLaunch` — wraps the Case A `investigationRunLauncher.launch(...)` call
  (covers the investigation-run insert + outbox insert together).
- Hikari pool state (active/idle/waiting/total connections), sampled every 2 seconds via
  `HikariPoolMXBean` — a read-only, already-exposed management API.
- JVM GC logging enabled via a `-Xlog:gc*` startup flag (not a code or config-file change — a
  runtime JVM argument for this diagnostic session only).

**Stated limitation, not distorted to work around it**: the transaction *commit* itself (Hibernate's
final flush + JDBC commit) was **not** isolated as its own measurement — doing so reliably would
need a JDBC/transaction-synchronization interceptor, which was judged too invasive for "minimum
necessary instrumentation." `listenerTotal` is the closest available proxy: it spans begin-to-commit
for the whole transaction, so `listenerTotal − (signalPersist + correlationQuery + incidentInsert/
investigationRunLaunch when applicable)` is an upper-bound estimate of "everything else" (commit
flush, Case B's `hasActiveInvestigation` lookup, in-memory dirty-checking, JVM/deserialization
overhead) — reported as a bucket, not decomposed further.

**Per-request DB connection *acquisition* latency** specifically (as opposed to pool occupancy) was
not separately measurable without Micrometer (not on this project's classpath) or a DataSource
wrapper — the Hikari pool *state* (active/idle/waiting) was used instead as the available proxy,
and is sufficient to answer the question that matters (was the pool ever exhausted): it was not,
in either run (see Step 5).

Instrumentation did not measurably distort behavior: achieved RPS and p99 HTTP latency in both
instrumented runs (895.85/9.1ms at 900 RPS offered; 1193.78/10.05ms at 1200 RPS offered) match the
already-committed, non-instrumented baseline runs (896.43/9.07ms and 1194.23/9.12ms respectively) to
within normal run-to-run variance — no redesign was necessary.

## Step 3 — Workload shape (preserved, not changed)

Both runs used the same `service-pool-size=100`, `environments=prod,staging`, `--workers 4`
configuration as the already-established 900/1200 RPS benchmarks (only the `--service-prefix`
changed, per this project's standing rule, to avoid cross-run correlation).

| | 900 RPS run | 1200 RPS run |
|---|---|---|
| Signals sent | 36,000 | 48,000 |
| Distinct correlation keys possible | up to 200 (100 services × 2 environments) | up to 200 |
| Incidents actually created | **100** | **100** |
| Approx. signals per incident | 360 | 480 |

Both runs front-load all 100 incidents within the first ~1 second (this project's established
pattern), then every subsequent signal coalesces into one of those same 100 already-open incidents
for the rest of the run (investigation-service is stopped, so no run ever completes and no incident
ever re-opens the "launch a new investigation" path after its first signal) — meaning the *entire*
100-incident, 35,900+/47,900+-signal remainder of each run exercises Case B's coalescing path, not
Case A's incident-creation path. This is the same design this project has used throughout, preserved
here deliberately.

## Step 4/5 — Measurements

### Reproduction

| | Offered | Achieved | Wall time (40s target) | HTTP p99 | Max/end lag behavior |
|---|---|---|---|---|---|
| Control | 900 | 895.85 | 40.19s | 9.1ms | bounded, oscillating ~700-1,300, **drains to 0** within ~9s of load stopping |
| Failure boundary | 1200 | 1193.78 | 40.21s | 10.05ms | **grows continuously**, 0 → 14,020 and still climbing when load stopped, only then drains |

HTTP-layer latency stayed clean at both tiers (confirms, again, that the boundary is purely on the
Incident Service consumer side, not the connector/Kafka producer path — consistent with every prior
measurement in this project).

### Per-phase timing (aggregated across all 3 consumer threads; mean of the steady-state 2-second
reporting windows during each run)

| Phase | 900 RPS mean | 900 RPS typical p99 | 1200 RPS mean | 1200 RPS typical p99 |
|---|---|---|---|---|
| `listenerTotal` (whole transaction) | **~3.0-3.5ms** | <10-25ms | **~3.0-3.9ms** | <10-25ms |
| `correlationQuery` | ~1.0-1.3ms | <5ms | ~0.8-1.2ms | <2-5ms |
| `signalPersist` | ~0.5-0.76ms | <2ms | ~0.35-0.65ms | <1-2ms |
| `incidentInsert` (Case A only, n=100 both runs — the one-time front-loaded burst) | ~1.0ms | <10ms | (not separately re-measured; same code path, same table) | — |
| `investigationRunLaunch` (Case A only, n=100) | ~1.07ms | <10ms | — | — |

**The single most important result: per-message service time is statistically the same at 900 and
1200 RPS offered load.** There is no degradation, no growing tail, no phase that gets slower as
offered load increases from 900 to 1200 — the distributions overlap almost completely. This directly
rules out "the system is getting slower under higher load" as an explanation.

### Per-consumer / aggregate processing rate

Using the measured `listenerTotal` mean (~3.0-3.5ms per message, aggregated across all 3 threads
via shared counters — a valid estimate of typical per-message service time regardless of which
thread handled it):

- **Per-consumer-thread rate** ≈ 1000ms / 3.3ms ≈ **~300 messages/sec/thread**.
- **3-consumer aggregate capacity** ≈ 300 × 3 ≈ **~900 messages/sec**.

**This independently and directly explains the measured ~900 RPS sustainable ceiling** — not via
resource-saturation reasoning, but via simple arithmetic from directly measured per-message service
time. At 1200 RPS offered against a ~900/sec processing capacity, the excess (~300/sec) has nowhere
to go but into a continuously growing backlog — exactly the observed behavior, and exactly what
basic queueing theory (arrival rate > service rate ⇒ unbounded queue) predicts.

### Resource utilization

| | 900 RPS | 1200 RPS |
|---|---|---|
| incident-service CPU max/mean | 82.2% / 34.5% | 83.2% / 43.2% |
| Postgres CPU max/mean | 170.6% / 95.5% | 160.95% / 116.5% |
| Kafka broker CPU max/mean | 153.83% / 64.3% | 217.53% / 77.9% |
| Hikari pool (active/idle/total) | 0-4 active / 6-10 idle / 10 total | 3-4 active / 6-7 idle / 10 total, **waiting=0 throughout, at every sample, in both runs** |
| Optimistic-lock conflicts | **0** | **0** |
| GC pauses (whole diagnostic session, both runs) | all Young-gen, worst **12.5ms**, no Full GCs | (same session) |

Postgres CPU rose roughly proportionally to offered rate (95.5% → 116.5% mean, a ~1.22× increase
for a 1.33× increase in offered rate) — consistent with the *same* per-signal cost simply being
paid more often, not a new qualitative behavior appearing at 1200 RPS. The host has far more than 2
cores available; a Postgres container mean in the 95-117% range (i.e., roughly 1-1.2 cores'-worth of
continuous work) is not saturation.

## Step 6 — Database query behavior

`findCorrelationCandidates` filters on `(environment, primary_service, status, last_observed_at)`
and sorts on `last_observed_at`. **Direct schema inspection** (`\d incidents`) confirms the table
has **only** a primary-key index and a unique constraint on `incident_number` — **no index
supports any of the correlation query's filter/sort columns.**

`EXPLAIN (ANALYZE, BUFFERS)` against the live table (currently 7,334 rows, accumulated across this
project's entire benchmark history — a real, present-state fact, not a synthetic worst case):

```
Sort (actual time=2.732..2.732 rows=0)
  ->  Seq Scan on incidents (actual time=2.716..2.716 rows=0)
        Filter: (status <> 'COMPLETED' AND environment = 'prod' AND primary_service = '...'
                 AND last_observed_at >= now() - '5 min')
        Rows Removed by Filter: 7334
        Buffers: shared hit=1 read=214
Execution Time: 2.748 ms
```

Every correlation query performs a **full sequential scan of the entire `incidents` table**,
reading it from a mix of cache and disk (214 buffer reads not satisfied from shared_buffers in this
sample). At the table's current size this costs on the order of ~1-3ms per query — consistent with
the `correlationQuery` phase's own measured ~0.8-1.3ms mean during live load. **This cost is
structurally proportional to table size and will keep growing as more incidents accumulate over
time** — a real, present, and worsening inefficiency, independently confirmed by both the query
planner and the live per-message timing instrumentation. No index was added; this is diagnosis only.

## Step 7 — Pattern determination

**Pattern A — PARALLELISM LIMITED** is the pattern the evidence supports:

- Consumers are continuously busy once a backlog exists (lag does not oscillate near zero at 1200
  RPS — it climbs monotonically, meaning threads always have work queued, never idle waiting for
  new messages).
- Per-message processing time is stable and nearly identical at both 900 and 1200 RPS — no
  degradation under higher load.
- Postgres and the connection pool have real, measured headroom: Hikari never saturated
  (`waiting=0` at every single sample in both runs, 6-10 of 10 connections idle at any moment),
  Postgres CPU elevated but well short of saturation and scaling proportionally (not
  super-linearly) with offered rate.
- Effectively zero lock contention: **0 optimistic-lock conflicts** in either run, consistent with
  the structural argument in Step 1 (partition-key locality prevents cross-thread contention on the
  signal-consumer side in this topology).

**This pattern does not exclude Pattern D as a contributing factor** — the correlation query's
sequential scan (Step 6) is a real, measured, ~30-40%-of-per-message-time contributor that is
independent of and additive to the parallelism limit. Both are true simultaneously: three threads,
each paying a real (and partially query-inefficiency-driven) per-message cost, together cap
throughput at ~900/sec.

## Answers to the numbered questions

1. **Exact hot path**: see Step 1's diagram — one real, atomic `@Transactional` method
   (`SignalIngestionService.ingest`) spanning signal insert → correlation query → incident
   create/update → investigation-run + outbox insert → commit, no self-invocation issue (unlike the
   Outbox Publisher).
2. **900-RPS processing profile**: `listenerTotal` mean ~3.0-3.5ms; `correlationQuery` ~1.0-1.3ms;
   `signalPersist` ~0.5-0.76ms; 0 optimistic-lock conflicts; Hikari never saturated; Postgres CPU
   mean 95.5%; bounded, self-draining lag.
3. **1200-RPS processing profile**: statistically the same per-message costs as 900 RPS
   (`listenerTotal` ~3.0-3.9ms, `correlationQuery` ~0.8-1.2ms, `signalPersist` ~0.35-0.65ms); 0
   optimistic-lock conflicts; Hikari never saturated; Postgres CPU mean 116.5% (proportionally
   higher, not qualitatively different); lag grows continuously and without bound.
4. **Actual consumer processing capacity**: ~300 messages/sec/consumer-thread × 3 threads ≈ ~900
   messages/sec aggregate — directly derived from measured per-message service time, independently
   matching the observed sustainable ceiling.
5. **Dominant bottleneck**: parallelism — a fixed number (3) of sequential-processing consumer
   threads, each with a real but stable and non-degrading per-message service time. There is no
   single shared resource currently at saturation.
6. **Evidence proving it**: per-message timing statistically identical at 900 vs. 1200 RPS (no
   degradation under load); Postgres/Hikari have measured headroom, not exhaustion; zero lock
   conflicts; the ~900/sec ceiling is independently reproduced by simple arithmetic from the
   measured per-message time × 3 threads.
7. **Does Postgres have headroom?** Yes, measured directly: Hikari pool never saturated (idle
   connections available at every sample in both runs), Postgres CPU elevated but not saturated and
   scaling proportionally, not explosively, with offered rate.
8. **Does correlation/hot-row contention matter?** No measurable contention in this workload
   (0 optimistic-lock conflicts in both runs) — structurally prevented by `signals.received.v1`'s
   partition-key locality (same service+environment always routes to the same consumer thread). The
   correlation *query's own cost* (unrelated to contention — a plain, unindexed sequential scan)
   does matter, and is a real, separate finding (Step 6).
9. **Does optimistic locking materially contribute?** No, in this specific benchmark — zero
   conflicts were recorded. It exists for a different, currently-inactive contention source
   (concurrent `investigation.results.v1` consumption, not exercised here since investigation-service
   is stopped per this project's isolation methodology).
10. **Is increasing partitions/consumers likely to help?** The evidence is consistent with yes —
    Postgres, the connection pool, and lock contention all show real headroom rather than
    saturation, which is the precondition Pattern A requires for more parallel workers to plausibly
    raise the ceiling. This is not tested here (explicitly out of scope) and no number is claimed.
11. **Single highest-value next optimization, if one is justified**: **add a composite index on
    `incidents(environment, primary_service, status, last_observed_at)`** (or an equivalent
    covering index) to eliminate the sequential scan `findCorrelationCandidates` currently performs
    on every single signal. This is justified specifically because it is a proven, currently-real
    inefficiency (confirmed by both `EXPLAIN ANALYZE` and live per-message timing) that (a) is
    orthogonal to any future consumer/partition decision — it helps regardless of what happens to
    consumer count — and (b) gets *worse over time* as the `incidents` table grows, unlike every
    other measured factor in this investigation, which is stable. It requires no architecture
    change and carries no correctness risk of the kind found in this project's other components
    (e.g., the Outbox Publisher's self-invocation gap).
12. **Expected mechanism of improvement (no RPS number invented)**: an index matching the query's
    filter/sort columns would let Postgres seek directly to the small number of candidate rows for a
    given environment+service, instead of scanning the entire table — reducing `correlationQuery`'s
    per-message cost (currently ~30-40% of total per-message time) and, correspondingly, some amount
    of the Postgres CPU load observed. Because parallelism (Step 7) is the *co-dominant* factor, an
    index alone would not by itself resolve the ~900/sec ceiling — it would lower the per-message
    cost that the ceiling arithmetic in point 4 is built from, which composes with (not replaces) any
    future decision about consumer/partition count.

## Correctness (verified for both runs)

- **900 RPS**: 36,000/36,000 signals persisted (exact match to offered count), 100 incidents, 0
  duplicate `event_id`s.
- **1200 RPS**: **48,000/48,000 signals persisted** (exact match to offered count — confirms zero
  message loss even while lag grew to over 14,000; Kafka's at-least-once redelivery plus this
  project's existing idempotency guarantee correctly drained the full backlog with nothing
  permanently stranded), 100 incidents, 0 duplicate `event_id`s.
- Kafka consumer lag on `signals.received.v1` drained to **0** on all partitions after both runs.
- Instrumentation itself introduced no correctness change (all wraps were either pure timing with no
  control-flow change, or a catch-log-rethrow that preserves the exact original exception
  propagation).

## Cleanup confirmation

```
$ git status --short services/incident-service/
 M services/incident-service/src/main/resources/application.yml
 M services/incident-service/src/test/java/com/tracemind/incident/service/InvestigationLifecycleIntegrationTest.java
 M services/incident-service/src/test/java/com/tracemind/incident/service/SignalIngestionServiceIntegrationTest.java
```

Only the three files already modified for Optimization 2 (poll-interval config + the two
pre-existing test-isolation fixes) remain — identical to the state before this investigation began.
The new `com.tracemind.incident.diag` package, and all edits to `SignalConsumerListener.java`,
`SignalIngestionService.java`, and `IncidentCorrelationService.java`, have been fully removed and
verified via `mvn test` (28/28 passing) both immediately after instrumentation was added and again
after its removal. `incident-service` was restarted on the clean, uninstrumented build; concurrency
remains 3; consumer lag is 0.

No optimization was implemented. Optimization 3 was not started. Stopping here per instruction.
