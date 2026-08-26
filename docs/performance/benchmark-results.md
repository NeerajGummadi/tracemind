# TraceMind Benchmark Results (Milestone N)

**Local benchmark on developer workstation.** See `benchmark-methodology.md` for the full
environment record and methodology. Do not treat these numbers as cloud/production capacity.

All raw/summarized outputs referenced below are under `benchmarks/results/`.

---

## Test A — Connector Ingestion

**What was measured**: `POST /integrations/prometheus/alerts` in isolation — Tomcat + Spring MVC +
Kafka producer send. Investigation-service was stopped for this test (see methodology §3/§10 —
Test A's traffic still reaches `investigation.requested.v1` via the real pipeline, so it had to be
kept out of paid-call range). Connector and incident-service ran as the pre-existing baseline
processes (Spring Boot defaults, untouched per this milestone's constraints).

**Dataset**: 200 deterministic synthetic services (`service-0000`…`service-0199`) × 2 environments
(prod/staging), rotated per request — see methodology §9/§10.

**Load generator**: open-loop, `benchmarks/scripts/load_connector.py`. 100–500 RPS ran
single-process; 1000–2000 RPS used `--workers 6`/`--workers 8` (see the methodology doc's load
generator finding — a single process could not itself sustain >~600 RPS).

### Results

| Offered RPS | Achieved RPS | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Error rate | Connector CPU (max) |
|---|---|---|---|---|---|---|---|
| 100 | 99.99 | 8.32 | 9.93 | 12.52 | 36.21 | 0% | ~13% |
| 250 | 249.92 | 5.56 | 7.64 | 8.36 | 24.40 | 0% | ~13% |
| 500 | 499.72 | 4.87 | 7.74 | 8.87 | 22.16 | 0% | ~14% |
| 1000 | 991.03 | 6.72 | 7.86 | 8.57 | 19.99 | 0% | 44.6% |
| 2000 | 1976.36 | 5.20 | 8.78 | 10.93 | 52.50 | 0% | 83.6% |

Status distribution: 100% HTTP 202 at every tier (`test_a_*rps.json`). No 4xx/5xx observed at any
tier.

### Where escalation stopped

The connector's *own* metrics stayed healthy through 2000 RPS (achieved rate ≈98–99% of offered,
p99 under 11ms, CPU at 83.6% with headroom remaining, 0% errors). Escalation stopped at 2000 RPS
because a different, shared stop-criterion fired: **Kafka consumer lag on the `incident-service`
group grew continuously and linearly throughout the 20s window** (0 → 31,811, `test_a_2000rps_lag.csv`)
rather than stabilizing — one of the explicit escalation-stop conditions, and not scoped only to
Test B. At 1000 RPS the same signal appeared but smaller (lag peaked ~9,200, already draining by
the end of the load window); at 2000 RPS it was still climbing when load stopped.

This means: **the connector boundary itself was not the limiting factor at any tested tier** — the
first visible bend in the system was downstream, in incident-service's single-threaded consumption
of `signals.received.v1` (Spring Kafka listener concurrency is 1, unchanged this milestone — see
the Environment Record). Test B will characterize this properly as the backend's own throughput
ceiling; Test A's job was only to establish that the connector isn't what causes it, which the data
supports.

The backlog was confirmed to fully drain to 0 afterward (`test_a_drain_check.csv`) — this was a
throughput ceiling under sustained offered load, not data loss.

### Correctness under load

- 0 duplicate-key violations traceable to any of Test A's own official traffic (100–2000 RPS
  tiers) — each generated eventId was unique by construction (see methodology §9).
- One unrelated duplicate-key WARN storm (4999 occurrences, all for the same `event_id`) was
  investigated and traced to a manual Apache Bench cross-check (see Methodology Finding below),
  not to Test A's dataset. Confirmed via `grep`: exactly one distinct `event_id` was ever involved.
  This is presented as a **positive correctness signal**, not a defect: 5000 rapid-fire identical
  submissions in 0.266s produced exactly 1 persisted row and 4999 correctly-rejected duplicates,
  with zero errors surfaced to the client.

### Methodology finding: ad hoc `ab` cross-check duplicate storm

**Symptom**: 4999 `duplicate key value violates unique constraint "signals_event_id_key"` WARN
entries appeared in incident-service's log during the Test A window.

**Hypothesis**: Either a Kafka redelivery bug (the same message being reprocessed without ever
committing its offset) or ordinary duplicate traffic.

**Diagnostic evidence**: All 4999 entries reference the identical `event_id`
(`evt-2d8694d372d8462f`). Postgres confirms exactly one row exists for it, with
`service='ab-test-service'`.

**Root cause**: An ad hoc Apache Bench cross-check (`ab -n 5000 -c 200 -p payload.json`, used to
validate the custom load generator's overhead — see the methodology doc) reused one static JSON
payload file for all 5000 requests, so every request carried the identical
`fingerprint`+`startsAt`, and the connector's deterministic `EventIdGenerator` correctly produced
the same `event_id` for all of them.

**Classification**: Benchmark artifact (my own diagnostic tooling), not a correctness bug.

**Fix**: None needed in the system — this is the idempotency mechanism working exactly as
designed under an extreme duplicate flood.

**Re-verification**: `signals.received.v1` consumer group lag is 0 across all partitions
post-test; no other distinct `event_id` ever triggered this constraint.

### Burst vs. sustainable (Test A)

Every tier above was a **burst** measurement (20s window) per the methodology's burst/sustained
distinction — Test A does not claim any of these rates as *sustainable* throughput; that
determination is Test B's job, applied to the full backend chain including the consumer that this
test surfaced as the earlier-bending component.

### Files
`benchmarks/results/test_a_{100,250,500,1000,2000}rps.json` (summaries),
`test_a_{1000,2000}rps_{resources,lag}.csv`, `test_a_drain_check.csv`.

---

## Test B — Backend Golden Path

**What was measured**: Connector → Kafka → Incident Service consumer → Postgres → transactional
outbox → `investigation.requested.v1` publish. **Isolation strategy**: the downstream-sink
approach described in the methodology doc — `downstream_sink.py` consumes
`investigation.requested.v1` with a fresh consumer group (`auto_offset_reset=latest`) and does
nothing but count/commit; investigation-service was not started at all, so no OpenAI call was
possible regardless of how many incidents this test created.

**Dataset**: 300 deterministic services × 2 environments, **each RPS tier given its own service
prefix** (`svcb500`, `svcb600`, `svcb700`, …) — required after a real methodology gap surfaced
mid-test (see finding below).

**Duration**: 120s sustained per tier (vs. Test A's 20s bursts) — long enough to see whether Kafka
consumer lag trends flat or grows, per the sustainable-vs-burst distinction in the methodology.

### Results

| Offered RPS | Achieved RPS | p50 (ms) | p99 (ms) | Error rate | Max consumer lag | Lag at end | Sustainable? |
|---|---|---|---|---|---|---|---|
| 300 | 299.38 | 7.82 | 10.06 | 0% | 27 | 0 | **Yes** |
| 500 | 498.87 | 7.02 | 8.71 | 0% | 208 | 0 | **Yes** |
| 600 | 598.67 | 7.07 | 10.10 | 0% | 13,461 | 8,470 (still climbing) | **No** |
| 700 | 698.30 | 7.03 | 9.36 | 0% | 21,290 | 15,730 (still climbing) | **No** |

Connector's own HTTP-layer numbers stayed clean and nearly identical across all four tiers
(p50/p99 barely move, 0% errors at every tier) — confirming, consistent with Test A, that the
connector is not what bends first.

### Sustainable throughput determination

**500 RPS is the last tier where lag stayed bounded** (max 208, back to 0 by test end). **600 RPS
is the first tier where lag grew continuously for the full 120s window** without stabilizing
(0 → 2,530 → 5,290 → 7,675 → 10,115 → 11,970, ending at 8,470 only because load had already
stopped) — the explicit "grows continuously instead of stabilizing" stop-criterion. Escalation
stopped at 700 RPS (confirming the trend, not searching for a worse case).

**Sustainable throughput for the backend golden path: ~500 RPS.** (Burst capacity is higher — Test
A showed the connector alone absorbing 2000 RPS bursts — but *sustained*, keeping consumer lag
bounded indefinitely, tops out around 500 RPS with the current single-threaded consumer.)

### Webhook-acceptance-to-durable-persistence latency

Measured directly (not estimated): a supplementary run at 200 RPS/20s with per-request
`(eventId, send_epoch)` capture (`measure_persistence_latency.py`), joined against
`signals.created_at` in Postgres (4000/4000 matched, 0 missing):

| Percentile | Latency |
|---|---|
| p50 | 8.72 ms |
| p95 | 10.55 ms |
| p99 | 16.47 ms |
| max | 129.47 ms |

This is genuinely fast — confirming that once a signal is picked up, per-item processing latency
is not the problem. The 600+ RPS lag growth is a **throughput** ceiling (arrival rate exceeding
sustained processing rate), not a per-item **latency** problem.

### Signals/incidents/outbox rates

At the sustainable 500 RPS tier: **signals persisted ≈ achieved RPS (≈499/s)**, sustained for the
full 120s window (216,000 signals total across all four tiers combined, verified against Postgres
— exact match, 0 loss). **Incidents created and outbox events published were front-loaded**: with
a 300-service pool and a 5-minute correlation window, all ~300 incidents for a given tier were
created within roughly the first second of that tier's traffic (one per distinct service,
verified exactly: 300/300/300 distinct incidents for the three prefixed tiers), and zero further
incidents or outbox rows were created for the rest of the 120s — every subsequent signal to an
already-open incident correctly coalesced (`needsReinvestigation` behavior, per Milestone M) rather
than creating new investigation traffic. This is expected, correct behavior given the dataset
design, not a rate limitation — Test D specifically stresses this coalescing path at a single
incident.

### Correctness under load

- **216,000/216,000 signals persisted** (300+500+600+700 RPS tiers combined) — exact match against
  requests sent, 0 loss, 0 unexpected duplicates.
- **0 outbox rows left PENDING** after any tier — everything that was created was eventually
  published; no silent loss.
- **Exactly 300 distinct incidents per tier** (matching `service_pool_size`), confirming
  correlation and idempotency hold under sustained real load, not just isolated unit/integration
  tests.

### Bottleneck analysis

1. **Symptom**: Consumer lag on the `incident-service` group (topic `signals.received.v1`) grows
   without bound above ~500–600 RPS sustained, while the connector's own latency/CPU stay flat and
   healthy at every tier tested.
2. **Diagnostic evidence**: `incident-service` CPU never exceeded ~30–38% even at the clearly
   unsustainable 600–700 RPS tiers (`test_b_{500,600,700}rps_resources.csv`) — nowhere near
   saturated. Postgres CPU similarly stayed under ~37%. Only 1 active Postgres connection was
   observed at idle, and the architecture only ever needs one at a time under load: **Spring Kafka
   listener concurrency is 1** (unchanged this milestone, per the environment record) — one
   consumer thread processes `signals.received.v1`'s 3 partitions serially, one signal fully
   through its transaction (signal insert + correlation query + incident upsert +
   `incident_signals` insert + conditional outbox insert) before starting the next.
3. **Root cause**: throughput is bounded by *serialized per-signal transaction latency on a single
   consumer thread*, not by CPU, not by Postgres write capacity, and not by the HikariCP pool
   (default max 10, but never more than 1 connection is ever needed given concurrency=1). The
   measured ~8.7ms p50 persistence latency times a single-threaded arrival rate gives a ceiling in
   roughly the observed 500–600 RPS neighborhood.
4. **Why this component saturated first**: everything upstream (connector) and alongside
   (Postgres, Kafka broker itself) has substantial CPU headroom at the point this bends — the
   *architecture*, not any single resource, is what caps throughput here: one thread necessarily
   processes signals strictly sequentially regardless of how much spare capacity exists elsewhere.
5. **Potential future optimization (not implemented this milestone)**: raising Spring Kafka
   listener concurrency (e.g. to 3, matching the topic's partition count) would let multiple
   signals process concurrently on separate threads/connections, each within HikariCP's existing
   pool headroom (max 10, currently ~90% idle even under load) — plausible to raise the sustained
   ceiling meaningfully, since neither CPU nor Postgres appear close to their own limits yet. This
   is explicitly *not* implemented per this milestone's "keep the current single-instance/default
   configuration" and "do not optimize before measuring" constraints — it is a hypothesis for a
   future milestone, not a change made here.

### Methodology finding: sink script logic error (fixed, but not the real explanation)

**Symptom**: `downstream_sink.py` reported `received=0` for the entire 700 RPS/120s window on the
first attempt.

**Hypothesis (first pass)**: `run()` wrapped `consumer.getmany(timeout_ms=500)` in a redundant
outer `asyncio.wait_for(timeout=1.0)`; a mid-flight cancellation could plausibly drop records from
the count while aiokafka's internal position still advanced.

**Fix applied**: removed the redundant wrapper.

**Re-verification gap**: I verified the fix only at low load with a *fresh* service pool, which
"worked" trivially regardless of whether the wait_for issue was the real cause — I did not
re-verify against the actual failing condition (700 RPS, real service pool) before treating it as
resolved. Re-running 700 RPS afterward still showed `received=0`, revealing the fix, while a
reasonable defensive change, was not the actual explanation.

**Real root cause** (found by direct inspection, not assumption): Test B's 700 RPS tier reused the
*same* deterministic service pool (`service-0000`…`service-0299`) as the immediately-preceding 300
RPS tier, run only ~3–4 minutes apart — within the 5-minute correlation window. Direct query of
`incidents` for `service-0000` showed one continuously-updated row spanning both tiers
(`first_observed_at` from the 300 RPS tier, `last_observed_at` from the 700 RPS tier,
`signal_version=684`). Since nothing completes an investigation when investigation-service isn't
running (Milestone M: an incident's run stays RUNNING forever without a result), every 700 RPS
signal correctly coalesced into the 300 RPS tier's still-open incidents instead of creating new
ones — **zero new `investigation.requested.v1` messages is the correct output of the coalescing
logic here, not data loss.**

**Classification**: Benchmark methodology gap (test design reused pool across tiers), not a system
defect. The sink fix is a legitimate defensive improvement but was a red herring for this
particular symptom — worth being honest about rather than claiming the first plausible fix was the
real explanation.

**Fix**: added `--service-prefix` to `load_connector.py` so each RPS tier gets a disjoint service
pool, eliminating cross-tier correlation regardless of timing. Re-ran 700 RPS with a fresh prefix:
286/300 expected new incidents received by the sink within its window (remainder confirmed
published via `outbox_events` — 0 PENDING).

### Files
`benchmarks/results/test_b_{300,500,600,700}rps.json`,
`test_b_{300,500,600,700}rps_{resources,lag,sink}.csv`,
`test_b_persistence_{timing,signals}.csv`.

---

## Test C — Idempotency Under Load

**What was measured**: duplicate-suppression correctness under sustained real traffic, at three
distinct duplicate patterns. Full real pipeline (Connector→Kafka→Incident Service→Postgres→Outbox);
investigation-service still not run (same isolation rationale as Test B — this test creates
incidents too, and there's no need to spend on AI to validate idempotency).

**Required invariant**: for intentionally identical eventIds, duplicate suppression must be 100%,
and duplicate traffic must never create duplicate signal rows, duplicate incidents, or duplicate
investigation runs.

### Results

| Scenario | Requests sent | Distinct events | Duplicates sent | Signals persisted | Suppression rate | Incidents | Investigation runs | Error rate | Max lag |
|---|---|---|---|---|---|---|---|---|---|
| 10% duplicate rate | 24,000 | 21,620 | 2,380 | 21,620 | **100%** | 185 | 185 | 0% | not measured separately (bounded by Test B's 400 RPS finding) |
| 50% duplicate rate | 24,000 | 12,100 | 11,900 | 12,100 | **100%** | 311 | 311 | 0% | max 100, final 0 |
| Concentrated retry burst (20 distinct IDs × 300 replays) | 6,000 | 20 | 5,980 | 20 | **100%** | 20 | 20 | 0% | max 4, final 0 |

All three at 400 RPS (10%/50% scenarios, 60s) or 300 RPS (burst scenario, 20s) — well within Test
B's confirmed ~500 RPS sustainable ceiling, so this test isolates duplicate-handling correctness
from throughput concerns.

**Required invariant: fully satisfied in all three scenarios.** Signals persisted exactly equals
distinct events sent in every case (verified directly against Postgres, not inferred) — every
single duplicate attempt was correctly rejected by the `event_id UNIQUE` constraint, caught as
`DuplicateSignalException`, logged, and acknowledged without creating any additional state.
Investigation run count exactly equals incident count in every scenario (verified via JOIN) —
zero duplicate investigation runs, confirming Milestone M's per-incident RUNNING invariant holds
even under adversarial duplicate pressure, not just clean traffic.

*(10% scenario shows 185 incidents against a `service_pool_size` of 200 — traced to a generator
artifact, not a system issue: with `reuse_period=10` and `service_pool_size=200` sharing a common
factor, and worker index offsets also multiples of 200, 15 specific service-name slots were
mathematically never reached by a "new" request this run — confirmed by direct calculation, not
guessed. 50% scenario shows 311 incidents against a nominal 211-service pool for an unrelated,
equally benign reason: odd pool size + 2-environment rotation on the same index meant some service
names legitimately paired with both environments across different rotation cycles, each a genuinely
distinct, correctly-created incident — confirmed via `GROUP BY (service, environment) HAVING
COUNT(*)>1` returning zero rows, i.e. no incident was ever actually duplicated.)*

### Two genuine generator bugs found and fixed before real scenarios could run

**1. Symptom**: `--duplicate-rate 0.5` on a 30-request validation run produced 0 duplicates.
**Diagnosis**: the reuse condition required `len(seen_pool) >= seed_duplicates_from` (default 50)
— never true for a 30-request run. **Root cause**: seeding threshold not scaled to test size.
**Classification**: benchmark-tooling bug. **Fix**: exposed `--duplicate-seed-count` as a CLI
parameter. **Re-verification**: confirmed correct at low load before scaling up.

**2. Symptom**: even after fixing #1, the *pattern* was wrong — at rate 0.5 with a fresh seed
count, one run showed 5/30 distinct (83% duplicate) rather than ~50%. **Diagnosis**: the original
logic classified reuse via `(local_i % 1000) < rate*1000` — a lumpy "first `rate*1000` requests
per 1000-block are reused, the rest are new" pattern rather than a uniform interleave; for small
request counts that never reach the 1000-boundary, this collapses to "almost everything past the
seed is a duplicate." **Root cause**: block-based reuse selection, not actually representative of
a uniform duplicate rate at any scale smaller than several thousand requests. **Classification**:
benchmark-tooling design flaw, not a system issue — but one that would have made every "10%"/"50%"
result in this section meaningless if uncaught. **Fix**: rewrote as evenly-spaced reuse (`local_i %
round(1/rate) == 0`), giving both a uniform interleave and an exact, verifiable achieved rate.
**Re-verification**: 10% target → 9% achieved, 50% target → 47% achieved on a 100-request
validation run, both confirmed against the idempotency summary before the official scenarios ran.

### Files
`benchmarks/results/test_c_{10pct,50pct,burst}.json`, `test_c_{10pct,50pct,burst}_lag.csv`,
`test_c_{10pct,50pct,burst}_resources.csv`.

---

## Test D — Investigation Lifecycle / Coalescing Stress

**What was measured**: whether the investigation lifecycle invariants (Milestone M) hold under a
correlated alert storm hitting a *single* incident — the deliberate exception to every other
test's "spread across many services" dataset strategy (see methodology §10).

**Isolation strategy**: investigation-service ran for real, with a new env-gated
(`AI_TEST_DOUBLE=true`) deterministic AI double swapped in at the exact point `main.py` constructs
`AIInvestigationService` — Kafka request/result flow, evidence collection (real Prometheus/Loki/
dependency-graph), and Incident Service's real lifecycle logic are all genuinely exercised; only
the OpenAI call itself is replaced with an instant, schema-valid, evidence-grounded RCA. Default
unset everywhere else, so production behavior is unchanged unless explicitly opted into. Confirmed
in the logs: `model=ai-test-double`, `openAiLatencyMs=0.1`, zero real OpenAI calls possible.

### A genuine correctness bug was found and fixed before results could be trusted

Per the milestone's explicit instruction, this is reported in full before any storm results, since
the first 100-alert storm violated a required invariant.

**1. Symptom**: after a 100-alert storm to one incident, `signalVersion` settled at 99, not 100 —
despite all 100 signals being confirmed correctly persisted and attached (`signals` count = 100,
`incident_signals` join count = 100, both verified directly against Postgres).

**2. Hypothesis**: a lost-update race between two independent Kafka consumer threads that both
mutate the same `Incident` row — `SignalConsumerListener` (`signals.received.v1`) and
`InvestigationResultConsumerListener` (`investigation.results.v1`).

**3. Diagnostic evidence**: `investigation_runs` showed exactly 3 non-overlapping runs (STALE v1,
STALE v89, COMPLETED v99) — the non-overlap itself confirmed invariant 1 held. But run2 (the
result-consumer thread launching a follow-up) started at `37.091`, only 5ms after run1 completed at
`37.086`, *while the signal-consumer thread was still processing the storm concurrently* (signals
were still arriving through `37.158`, per the load generator's own timing). Confirmed via distinct
Kafka client IDs (`consumer-incident-service-1` for results, `consumer-incident-service-2` for
signals) that these are genuinely different threads, not sequential processing.

**4. Root cause**: `Incident` had no optimistic-locking (`@Version`) field. Both consumer threads
could independently load the same row, mutate different fields (`signalVersion` on the signal
thread; `currentInvestigationRunId`/`needsReinvestigation` on the result thread), and Hibernate's
last-write-wins flush let whichever transaction committed second silently overwrite the other's
change to `signalVersion` with its own stale in-memory value.

**5. Classification**: **Correctness bug** — real, in production code (`Incident.java`), not a
benchmark artifact. Invisible to every previous test in this milestone because none of them created
genuinely concurrent write pressure on one incident row from two different consumer threads at
once — Test D's whole design is what exposed it.

**6. Fix**: added `@Version` to `Incident` (migration `V8__add_optimistic_locking_to_incidents.sql`).
Hibernate now fails the second-committing transaction with `ObjectOptimisticLockingFailureException`
instead of silently overwriting; both listeners already propagate uncaught exceptions without
acking (existing resilience pattern from earlier milestones), so Kafka redelivers and the retry
succeeds against the current row version. No new resilience mechanism was needed — this made the
*existing* redelivery-on-failure pattern actually catch a failure mode it was blind to.

**7. Re-verification**: added a deterministic regression test
(`concurrentMutationsFromIndependentTransactionsDoNotSilentlyLoseAnUpdate`) that directly forces the
race (two independent transactions load the same row, each mutates a different field, first save
succeeds, second must throw `ObjectOptimisticLockingFailureException`, and the first transaction's
update must survive) — passes deterministically, not dependent on live timing. All 28 Java tests
pass (27 previous + this one). Re-ran the 100-alert storm against the fixed code:
**`signalVersion=100`, exactly matching all 100 signals sent**, stable across repeated re-checks.
(The live race did not happen to fire again on this particular re-run or on the 500/1000-alert
storms below — it's inherently timing-dependent — but the fix's correctness is proven
deterministically by the regression test, independent of whether any given run happens to trigger
the race live.)

### Storm results (post-fix)

| Storm size | Offered alerts/s | Achieved alerts/s | Error rate | Final `signalVersion` | Total runs | Stale runs | Reinvestigations |
|---|---|---|---|---|---|---|---|
| 100 | 200.0 | 200.7 | 0% | 100/100 ✅ | 2 | 1 | 1 |
| 500 | 400.0 | 356.5 | 0% | 500/500 ✅ | 2 | 1 | 1 |
| 1000 | 500.0 | 468.7 | 0% | 1000/1000 ✅ | 2 | 1 | 1 |

Every storm size collapsed to exactly **2 total runs** (1 initial + 1 reinvestigation) regardless of
whether 100, 500, or 1000 alerts arrived — satisfying invariant 6 directly (`run count = initial +
required reinvestigations`, not proportional to alert count). This is because the AI-double's
investigation latency (~15–28ms, see below) is short enough relative to each storm's ~0.5–2.1s
arrival window that only one reinvestigation cycle was needed to absorb all of it, in every size
tested.

### Required invariants — verified directly against Postgres for every storm size

1. **Never more than one RUNNING investigation**: proven exactly, not sampled — for every storm,
   the two runs' `[started_at, completed_at]` intervals do not overlap (e.g. 1000-alert storm:
   run1 `[29.451, 31.882]`, run2 `[31.888, 32.920]` — run2 starts 6ms *after* run1 ends).
2. **`signalVersion` increases; `needsReinvestigation` becomes true**: confirmed via the
   intermediate run's `input_signal_version` (89/–/– showing mid-storm progression was captured)
   and final values matching alert count exactly in all three post-fix storms.
3. **STALE + exactly one follow-up on completion**: the initial run was marked STALE in every
   storm (its `input_signal_version` was always stale by the time it completed), and exactly one
   follow-up `investigation_runs` row exists per storm — confirmed by count, not inferred.
4. **After the storm**: exactly one current run (`current_investigation_run_id` points to the sole
   COMPLETED run), `needsReinvestigation=false`, incident `status` never left `QUEUED` (no
   auto-resolution) — all confirmed by direct query.
5. **No duplicate investigations**: `investigation.requested.v1` and `investigation.results.v1`
   message counts for each storm's known run IDs matched 1:1 against `investigation_runs` (e.g.
   1000-alert storm: exactly 2 request messages, exactly 2 result messages, no more).
6. **Run count ≪ alert count**: 2 runs for 100, 500, *and* 1000 alerts — not 100/500/1000.

### Measurements

- **Lifecycle transition latency** (per-investigation, AI-double): total duration 14.7–28.0ms,
  dominated by real evidence collection (Loki ~14–27ms, Prometheus ~7–26ms), `openAiLatencyMs=0.1`
  in every case (confirms the double, not real OpenAI, was used).
- **Storm-to-full-settlement**: all three storms fully settled (`needsReinvestigation=false`,
  current run `COMPLETED`, stable across repeated re-checks) within ~1–3 seconds of the storm
  finishing sending.
- **Kafka lag**: 0 on both `signals.received.v1` and `investigation.requested.v1`/
  `investigation.results.v1` after each storm settled — no backlog left behind.
- **CPU/memory**: both services idle (<1% CPU) within seconds after each storm; storms were too
  short (0.5–2.1s) for the `docker stats`-based sampling method (≈2–4s effective interval, per the
  methodology doc's noted limitation) to produce meaningful in-storm CPU curves — noted as a
  measurement limitation rather than papered over. Idle RSS: incident-service ~76MB, investigation-
  service ~19MB.
- **Error rate**: 0% at every storm size, all three tiers stayed well within Test B's confirmed
  sustainable throughput ceiling (~500 RPS backend, ~356–469 achieved here), so no escalation-stop
  criterion was ever approached — all three planned storm sizes ran without needing to halt early.

### Files
`benchmarks/results/test_d_storm{100,500,1000}.json`.

---

## Test E — Transactional Outbox Throughput & Recovery

**What was measured**: the Outbox Publisher in isolation — Postgres `outbox_events` → publisher →
Kafka. **Isolation**: Investigation Service, Prometheus, and Loki were not involved at any point;
`incident-service` (which owns the publisher as an internal `@Scheduled` component, not a separate
process) and Postgres/Kafka were the only things running. Backlog was seeded by inserting rows
directly into `outbox_events` (`outbox_backlog_seed.py`), bypassing the connector/signal-ingestion
pipeline entirely, with a payload shape matching what `OutboxEvent.investigationRequested()`
actually produces. **No production code was changed** — the existing `OutboxPublisher`
(`batch-size=50`, `poll-interval-ms=1000`, unchanged per this milestone's constraints) was used as-is
throughout.

### A benchmark-tooling bug, found and fixed before any real measurement

**Symptom**: the seed script crashed with `OSError: Argument list too long` when seeding at scale
(batch size 2000 rows/call).
**Diagnosis**: the generated `INSERT` SQL (~600–800KB for 2000 rows, each with a full JSON payload)
was passed as a `docker exec ... psql -c "<sql>"` command-line argument, exceeding the OS's
`ARG_MAX` for a single exec call.
**Root cause**: SQL passed as a CLI argument instead of piped via stdin — a benchmark-tooling
limitation, not a system issue (confirmed: zero rows had been inserted when the crash occurred, no
partial/corrupt state).
**Fix**: switched to piping SQL via `stdin` (`subprocess.run(..., input=sql)`), which has no such
size limit. **Re-verified** at the exact batch size that failed (2000 rows) before re-running
Scenario 1 for real.

### Scenario 1 — Backlog Drain

**10,000 events: measured directly.** Seeded in ~1s; publisher drained the full backlog in
**~318 seconds**, a highly linear rate of **~31.4 events/sec** (10,000 ÷ 318s), matching the
`batch-size / (poll-interval + per-batch processing time)` ceiling implied by the unchanged
production configuration almost exactly (50 rows ÷ ~1.6s cycle ≈ 31/s). Confirmed via direct
Postgres query: 10,000/10,000 rows `PUBLISHED`, 0 left `PENDING`.

Resource utilization during the full drain (`test_e_10k_resources.csv`) stayed low throughout:
incident-service CPU max 32.3% (mean 7.4%), Postgres CPU max 56.8% (mean 21.8%) — **the publisher
is not resource-constrained at any point; the ~31/s ceiling is entirely a function of the
`batch-size`/`poll-interval-ms` configuration**, with large headroom left unused on every resource
that was measured.

**50,000 and 100,000 events: projected, not measured**, at the user's direction — the 10k run
already established a highly linear, configuration-bounded (not backlog-size-dependent) rate with
no resource anywhere near saturated, so a live run would extend the same straight line rather than
reveal new behavior, at a real cost of ~26–53 more minutes of wall-clock time this session.
Projected from the measured 31.4 events/sec:

| Backlog size | Measured or projected | Drain time |
|---|---|---|
| 10,000 | **Measured** | **318s (~5.3 min)** |
| 50,000 | Projected (linear extrapolation) | ~1,590s (~26.5 min) |
| 100,000 | Projected (linear extrapolation) | ~3,180s (~53.0 min) |

### Scenario 2 — Continuous Producer + Publisher

Three producer rates tested against the ~31/s publisher ceiling, each for 60s of continuous
production plus a post-production observation window:

| Producer rate | Behavior during production | Behavior after production stops |
|---|---|---|
| 10/s (well under ceiling) | Backlog oscillates 0–10, fully absorbed | N/A — already ~0 |
| 30/s (~at ceiling) | Backlog oscillates in a bounded 0–80 range, stabilizes | Drains to 0 within ~2s |
| 50/s (over ceiling) | Backlog grows linearly, ~19/s net (≈ 50 − 31) | Resumes draining at ~31/s once production stops, fully drains |

**Equilibrium point**: ~31 events/sec — matches Scenario 1's measured drain rate exactly, as
expected (same publisher, same configuration). Backlog **never grows unboundedly once production
stops**, at any tested rate — the over-ceiling case demonstrates temporary queueing under sustained
overload, not permanent damage. All three producer totals were verified exactly against Postgres
(600/600, 1800/1800, 3000/3000 published, 0 lost, 0 duplicated).

### Scenario 3 — Publisher Failure

Seeded 3,000 events, let the publisher drain briefly (2,932 pending after ~2s), then killed
`incident-service` (`SIGTERM`) mid-drain at **2,500 pending**.

- **Backlog while stopped**: frozen at exactly 2,500 for 15 seconds of repeated polling — no
  drift, confirming a stopped publisher neither loses nor silently continues processing.
- **Recovery time**: Spring Boot restart took ~15.1s to `Started IncidentServiceApplication`.
- **Drain rate after restart**: resumed at ~28.6 events/sec (1,600 pending → 0 in 55.96s) —
  consistent with the pre-failure rate, no degradation from the restart.
- **Correctness**: 3,000/3,000 `PUBLISHED` in Postgres; Kafka scan found exactly 3,000 distinct
  `incidentId`s with **zero duplicates**. No event loss, no duplicate publication.

### Scenario 4 — Crash During Publish

Seeded another 3,000 events, then **`SIGKILL`** (not graceful shutdown) `incident-service` mid-drain
at 2,300 pending / 700 published.

- **Immediate post-crash state**: exactly 3,000 rows total, split 2,300 `PENDING` / 700
  `PUBLISHED` — no third/ambiguous status exists in this schema (`OutboxEvent` only ever writes
  `PENDING` or `PUBLISHED`; there is no separate `PROCESSING` state to strand a row in). Confirmed
  `pg_locks` on `outbox_events` was empty and no idle-in-transaction connections remained — Postgres
  cleanly rolled back whatever transaction was in flight at the moment of the kill, exactly as the
  `FOR UPDATE SKIP LOCKED` + single-transaction-per-claim design is meant to guarantee.
- **After restart + full drain**: 3,000/3,000 `PUBLISHED`, 0 stranded `PENDING`, 0 stranded
  "processing" rows (by construction — see above).
- **Duplicate Kafka messages**: **zero observed** in this run (3,000 distinct `incidentId`s, no
  repeats). Worth stating precisely rather than overclaiming: a duplicate is theoretically possible
  if a crash lands in the narrow gap between a successful Kafka send and the following
  `markPublished`+commit (the Kafka send is not part of the same atomic unit as the DB write) — this
  run's `SIGKILL` did not happen to land in that specific sub-millisecond window. Per the blueprint's
  at-least-once-delivery invariant, such a duplicate would be an *expected, handled* characteristic
  of the delivery model (downstream consumers are required to be idempotent — confirmed in Milestones
  M/N via `investigationRunId`-keyed dedup on both sides), not a correctness violation if it did occur.

### Correctness invariants — verified directly against Postgres after every scenario

- **`PENDING` rows = expected**: 0 after every scenario's full drain (backlog fully accounted for
  in every case; frozen-not-lost while explicitly stopped in Scenario 3).
- **"`PROCESSING`" rows = 0**: trivially and structurally true — this status value does not exist
  in the schema. The atomicity of claim+publish+markPublished (one transaction, `FOR UPDATE SKIP
  LOCKED` for concurrency) is what achieves the *intent* behind a `PROCESSING` state (no
  observable stuck middle state) without needing one.
- **`FAILED` rows = 0**: also structurally true — no `FAILED` status exists in this schema either;
  a publish failure simply leaves a row `PENDING` for the next poll cycle to retry (see
  `OutboxPublisher`'s existing design, unchanged this milestone).
- **No row stranded forever**: confirmed in all four scenarios, including the two failure
  injections.
- **Every row reaches `PUBLISHED` exactly once**: confirmed via exact Postgres counts and Kafka
  `incidentId` uniqueness checks in Scenarios 1–4, with zero exceptions found.

### Measurements summary

- **Offered vs. achieved publish rate**: offered is unbounded (backlog seeded instantly); achieved
  is consistently **~28–31 events/sec** across every scenario (10k drain, 30/50/s producer tests,
  both recovery drains) — a single, reproducible number across five independent measurements.
- **Batch size**: 50 (config, unchanged).
- **Drain time**: 318s measured for 10k; see Scenario 1 table for 50k/100k projections.
- **Backlog growth rate** (over-ceiling case): ~19/s net at a 50/s producer rate (50 − ~31).
- **CPU**: incident-service max 32.3%/mean 7.4%; Postgres max 56.8%/mean 21.8%; Kafka max 101.7%
  (multi-core)/mean 23.0% — all well short of saturation throughout Scenario 1's full 10k drain.
- **Memory**: incident-service RSS mean ~137MB; Postgres RSS mean ~225MB — stable, no growth
  trend across any scenario (including the two multi-thousand-row seed operations).
- **Kafka lag**: not applicable as a downstream-consumer metric here (nothing consumes
  `investigation.requested.v1` in this isolated test by design) — backlog depth in `outbox_events`
  is the direct, correct analog and was tracked continuously instead.

### Files
`benchmarks/results/test_e_10k_{backlog,resources}.csv`,
`test_e2_rate{10,30,50}_backlog.csv`, `test_e3_recovery_backlog.csv`, `test_e4_recovery_backlog.csv`.

---

## Test F — Real AI Investigation Benchmark

**What was measured**: the fully real pipeline — real Prometheus, real Loki, real
`StaticDependencyCollector`, real `PromptBuilder`, real OpenAI, real Kafka, real Investigation
Service. No doubles, no mocks. 20 real investigations against `payment-service`, fired one at a
time (each waited out to full settlement before the next was fired), to stay within Milestone M's
coalescing behavior rather than trigger it.

### A real incident, caught and fixed before it became a real problem

**This is reported first and in full, per the milestone's own priority, because it involved actual
unintended spend.**

**Symptom**: the instant investigation-service was restarted with a real API key, it immediately
made 11 real OpenAI calls — before any Test F alert had been fired.

**Diagnostic evidence**: the consumed incident IDs were `INC-BENCH-validateE-8`,
`INC-BENCH-validateE-9`, `outbox-bench-service` — Test E's own synthetic benchmark data, not real
traffic. 2 of the 11 even failed schema validation (empty `supportingEvidenceIds`) since synthetic
services have no real Prometheus/Loki/topology data to ground on.

**Root cause**: Test E deliberately kept investigation-service stopped for isolation (per its own
explicit requirement), so the ~23,000 `investigation.requested.v1` messages its outbox-seeding
scenarios produced sat unconsumed on the topic. Restarting investigation-service with a real key
resumed its consumer group from the old committed offset and began draining that entire stale
backlog with real paid calls.

**Classification**: **my own process/methodology error**, not a system defect — the system behaved
exactly as designed (at-least-once delivery, resume from committed offset). I should have checked
consumer-group lag before restarting with a real key, the way earlier milestones in this session
consistently did, and didn't this time.

**Fix**: killed investigation-service immediately (11 calls made, not thousands — caught within
seconds). Checked the `investigation-service` consumer group and found **23,434 messages still
pending**. Reset the group's offset to `latest` (`kafka-consumer-groups.sh --reset-offsets
--to-latest`), skipping the stale backlog entirely without touching the underlying topic or any
other consumer group. Verified lag was 0 across all partitions before restarting.

**Re-verification**: restarted investigation-service with the real key; confirmed 0 OpenAI calls
were made before the first deliberate Test F alert was fired. Total real calls this session: 20
Test F investigations + the 11 stray ones = 31 (small, cheap gpt-4o-mini calls; the 11 stray calls
are included in nothing reported below — they're excluded from all Test F aggregates as invalid,
ungrounded samples).

A second, unrelated bug surfaced in my own benchmark orchestration script
(`run_real_investigations.py`) during the first live batch: its poll loop treated a run reaching
`STALE` as "settled," when `STALE` actually means a follow-up is still in flight — this let it fire
alert #2 before alert #1's *true* result (a REINVESTIGATION follow-up) had completed, coalescing
two of my own deliberate alerts into one lifecycle sequence instead of keeping them cleanly
separate. Fixed by requiring `needsReinvestigation=false` AND status `COMPLETED`/`FAILED`
specifically (STALE is never a stopping condition) — the exact condition already proven correct by
hand in Tests D and earlier real-validation milestones. Re-verified: the following batch of 15 ran
strictly sequentially with zero STALE runs. (The one STALE-but-real investigation from the affected
batch is still included below — its metrics are fully real and valid, it was just superseded in
the lifecycle sense before I read its result, which is itself a legitimate real data point.)

### Sample

20 real investigations, `payment-service`/`prod`, model `gpt-4o-mini-2024-07-18` throughout. All
grounding validation checks below passed for all 20 — **no STOP-and-diagnose was required.**

### Aggregate results (n=20)

| Metric | p50 | p95 | max | mean |
|---|---|---|---|---|
| Total investigation duration | 2542.9ms | 6774.1ms | 6774.1ms | 2781.5ms |
| OpenAI latency | 2522.1ms | 6748.8ms | 6748.8ms | 2767.0ms |

*(p95 with n=20 is the 19th of 20 sorted samples — effectively "second-highest," reported as
asked but not to be over-interpreted as a statistically robust tail estimate at this sample size;
it coincides with `max` here because the single slow outlier — 6774ms, a real OpenAI response-time
variance, not a system issue — falls at that position.)*

| Metric | Mean | Notes |
|---|---|---|
| Prometheus collector latency | 10.38ms | max 21.70ms |
| Loki collector latency | 13.14ms | max 36.20ms |
| Dependency graph collector latency | 0.035ms | max 0.300ms (in-memory, no I/O) |
| Evidence collection (concurrent total) | — | dominated by Loki/Prometheus real HTTP round-trips, both under 40ms even at max |
| Prompt tokens | 1158.0 | identical every time — deterministic prompt template + stable evidence shape |
| Completion tokens | 186.7 | |
| Total tokens | 1344.7 | 26,893 total across all 20 |
| Estimated API cost | **unavailable** | `estimatedApiCostUsd` is `null` for all 20 — no pricing table configured, per this project's standing rule never to fabricate cost |
| InvestigationResult publication latency | ~0.4ms mean (range −0.7 to 10.7ms) | measured via `generatedAt` (embedded in the result) vs. the Kafka broker's own message timestamp — negligible, not a bottleneck |
| Success rate | **100%** (20/20 `COMPLETED`) | 0 `FAILED`, 0 timeouts |
| Schema validation failures | **0** | (the 11 *excluded* stray calls against synthetic data had 2 — not counted here, as noted above) |
| Retries | **0** | no `AIInvestigationError` raised for any of the 20 |

### Grounding validation (required, checked for every investigation)

- **Pydantic validation**: 20/20 passed (a `FAILED` status would indicate otherwise — none occurred).
- **`supportingEvidenceIds` non-empty**: 20/20.
- **No hallucinated evidence**: 20/20 — every cited ID cross-checked against that investigation's
  own `EvidenceBundle` (metrics/logs/dependencies), zero unknown IDs found.
- **`confidence` within [0,1]**: 20/20 (Pydantic-enforced at the schema level; independently
  re-verified here against the actual returned values).
- **`incidentId` consistency**: 20/20 (RCA's `incidentId` matched the request's).

**Result: 0/20 validation failures — the required STOP-and-diagnose protocol was never triggered.**

### Bottleneck analysis

The real AI path's total latency is **almost entirely OpenAI's own response time**
(mean 2767ms of a 2781.5ms mean total — evidence collection contributes under 1% of total latency
even at its slowest observed point). This is the inverse of every other test in this milestone:
Tests A–E all found TraceMind's own components (Kafka consumer concurrency, outbox
batch-size/poll-interval) as the binding constraint with real external dependencies barely
touched; Test F is the one place where the external dependency (OpenAI) *is* the bottleneck, by a
wide margin, and nothing in this system's own design meaningfully adds to it.

### Engineering observations

- **Deterministic prompt token count** (1158 every time) confirms `PromptBuilder`'s output shape
  is stable across runs with the same evidence structure — useful for capacity/cost planning, since
  token count doesn't vary with incident content the way completion tokens (186.7 mean, some
  spread) naturally do.
- **The one 6.7s outlier** (vs. ~2-3s typical) was a genuine OpenAI response-time variance, not a
  retry or a TraceMind-side slowdown — `evidenceCollectionMs` for that same investigation was
  36.5ms, in line with the others; only `openAiLatencyMs` spiked.
- **This test is the sharpest illustration in the whole milestone of "don't benchmark this at
  high volume"** — the reasoning that justified Tests B/D's AI isolation strategies is directly
  visible here: at ~2.8s mean per investigation, sustaining even Test B's confirmed ~500 RPS
  backend throughput would require ~1,400 concurrent OpenAI calls in flight, which is neither
  affordable nor something a provider rate limit would tolerate — confirming, empirically now
  rather than just architecturally, why Tests A–E were right to keep AI out of the loop.

### Files
`benchmarks/results/test_f_batch{1,2}.json`.

---

Tests G (Failure Injection & Graceful Degradation) and H (Recovery & Backlog Drain) are reported
separately in `resilience-results.md` and `recovery-benchmark-report.md` respectively, following
the file layout set out in the methodology doc.

---

## Consolidated Summary — Tests A–H

**Local benchmark on developer workstation, single-instance/default configuration throughout — no
production code was optimized at any point in this milestone.**

### Throughput

| Layer | Sustainable | Burst | Bottleneck |
|---|---|---|---|
| Connector HTTP ingestion | — | 2000 RPS, 0% errors, p99 <11ms, 83.6% CPU with headroom left | Never reached in this milestone — shared stop-criterion (downstream lag) ended escalation, not the connector itself |
| Backend golden path (Connector→Kafka→Incident Service→Postgres→Outbox) | **~500 RPS** | up to 700 RPS briefly tolerated before lag grows unboundedly | Spring Kafka listener concurrency = 1 (config default, unchanged) — one thread serializes every signal's full transaction |
| Outbox Publisher | **~28–31 events/sec**, constant regardless of backlog size (10k measured at 318s; 50k/100k projected linearly) | N/A — config-bounded, not a burst/sustained distinction | `batch-size=50` / `poll-interval-ms=1000` (config default, unchanged) — not CPU, not Postgres, not Kafka (all had large headroom) |
| Real AI investigation | 1 at a time by design | N/A | OpenAI's own response time (mean 2767ms of 2781.5ms total, >99%) — the only test where an external dependency, not TraceMind's code, is the binding constraint |

### Latency

| Path | p50 | p95/p99 | max |
|---|---|---|---|
| Connector ingestion (any tested RPS) | 5-8ms | 8-11ms | 19-53ms |
| Webhook-to-durable-persistence (Test B, 200 RPS) | 8.72ms | p99 16.47ms | 129.47ms |
| Real AI investigation, total (Test F, n=20) | 2542.9ms | p95 6774.1ms* | 6774.1ms |
| Real AI investigation, OpenAI-only | 2522.1ms | p95 6748.8ms* | 6748.8ms |
| Investigation lifecycle transition (Test D, AI double) | 15-28ms | — | — |
| InvestigationResult publication (result-ready → Kafka broker receipt) | ~0.4ms mean | — | 10.7ms |

*p95 at n=20 is the 19th-of-20 sorted sample — reported as required, not to be read as a
statistically robust tail estimate at this sample size.

### Resilience (Test G — all 7 scenarios, real system, independently verified)

Kafka down: bounded 5.32s failure (not a hidden 60s hang), clean recovery. Postgres down: message
safely retained (never lost), clean transactional rollback, ~2s recovery. Prometheus/Loki down:
bounded ~5s timeouts, evidence bundles correctly degraded to remaining real sources, RCA still
grounded in genuine (non-hallucinated) evidence in both cases. OpenAI down: `FAILED`/`API_ERROR`,
zero billable tokens, auth failure correctly recognized as non-retryable (1 attempt, not the
configured 2 retries wasted). Investigation Service and Incident Service hard-`SIGKILL`
mid-processing: both recovered with **zero data loss and zero duplicates** (50/50 and 75/75
respectively, cross-checked against a full-history, multi-thousand-message Kafka scan).

### Recovery (Test H — all 4 scenarios)

Investigation Service: 150-message backlog drained in ≤1.47s post-restart (≥102 events/sec), 0
duplicates across the entire topic history (4,584 messages). Incident Service: 900-signal backlog
(30/s sustained arrival) drained in ~2s (≥437 events/sec catch-up), Milestone M's coalescing
correctly collapsed 900 signals into 418 real investigation attempts with exactly one current
result per incident. Kafka: 6.44s reconnect, **perfect message ordering preserved** across the
outage boundary; one genuine finding (a request that receives `503` can still be delivered
successfully in the background, since the app-level 5s wait doesn't cancel the producer's own
120s-default internal retry window) — safe in practice because deterministic event IDs make any
resulting duplicate submission a no-op, not a corruption. PostgreSQL: 500-signal backlog drained
in ~8.9s after a ~6.5s HikariCP reconnection-settling window.

**Across all 11 failure/recovery scenarios (Tests G+H combined): zero data loss, zero duplicate
processing, every correctness invariant held.** The single non-invariant-violating architectural
observation (Test H Scenario 3) is documented precisely rather than either alarmed over or omitted.

### Correctness (Tests C, D, F, G, H combined)

Idempotency under load (Test C): **100% duplicate suppression** at 10%, 50%, and concentrated-burst
duplicate patterns — verified directly against Postgres, not inferred, in every case. Investigation
lifecycle under storm conditions (Test D): every required invariant (never >1 RUNNING, correct
STALE detection, exactly-one-follow-up, run count ≪ alert count) held at 100/500/1000-alert storms
— **after** a genuine lost-update race (found and fixed with `@Version` optimistic locking,
Migration V8) that no earlier milestone's testing had surfaced. Real AI grounding (Test F): 20/20
investigations passed every grounding check (valid Pydantic schema, non-empty evidence citations,
zero hallucinated IDs, confidence in range) — zero failures, the STOP-and-diagnose protocol never
triggered.

### Identified bottlenecks, ranked by where the system actually bends

1. **Backend golden path ceiling (~500 RPS)** — Spring Kafka listener concurrency=1 serializing
   per-signal transactions on a single thread, while Postgres/Kafka/CPU all sit well under 40%
   utilization at the point this bends. **Highest-leverage candidate for a future milestone**:
   raising listener concurrency toward the topic's 3 partitions.
2. **Outbox Publisher ceiling (~30 events/sec)** — `batch-size`/`poll-interval-ms` configuration,
   not any resource. Trivial to raise once deliberately revisited, but *irrelevant in practice*
   given the backend's own ~500 RPS ceiling is reached first — the outbox is never the limiting
   factor under realistic load, only visible when isolated (Test E) or fed directly (Test H
   Scenario 1's investigation.requested.v1 backlog, which is downstream of it).
3. **Real AI latency (~2.8s mean)** — external, not TraceMind's own code; the reason Tests A–E,
   and Test H Scenario 1, all deliberately kept AI out of their loop or used the deterministic
   double.
4. **Test H Scenario 3's bounded-wait-vs-bounded-operation gap** — not a throughput bottleneck,
   but a real semantic looseness worth a future look; currently harmless due to deterministic
   event IDs.

None of these were touched during this milestone. **No optimization work has been performed** —
Milestone N is a baseline measurement and correctness benchmark only, per its own explicit
constraints throughout.
