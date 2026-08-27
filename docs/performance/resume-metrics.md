# TraceMind — Resume-Safe Performance Metrics

**Every number below is measured, on a local, containerized, single-node development
environment** (Docker Desktop on a shared macOS workstation — 4 containers: Kafka/KRaft
single-broker, Postgres, Loki, Prometheus; Java 25/Spring Boot services and a Python/FastAPI
service running natively, not containerized). None of these figures are production-scale claims
or extrapolations beyond what was directly tested. Each claim below cites the exact source file it
came from. Where a number is an inference rather than a direct measurement, that is stated
explicitly.

---

## A. Ingestion boundary (Connector, isolated)

- **Achieved ~1,976 RPS** (1976.36) at a 2,000 RPS offered tier, **p50 5.20ms / p95 8.78ms / p99
  10.93ms / max 52.50ms, 0% HTTP errors**, connector CPU 83.6% (still had headroom).
- Source: `docs/performance/benchmark-results.md`, Test A results table.
- **This was the highest tier tested, not a proven ceiling.** Escalation stopped at 2,000 RPS
  because a separate, shared stop-criterion fired downstream (`incident-service` consumer lag
  growing continuously) — the connector's own metrics never showed a bend. The connector's true
  maximum capacity was never found; only that it comfortably absorbs bursts up to 2,000 RPS.

## B. Backend baseline (pre-optimization, concurrency=1)

- **~500 RPS was the sustainable ceiling** for the full Connector→Kafka→Incident Service→Postgres→
  Outbox path: 500 RPS achieved 498.87, p50 7.02ms, p99 8.71ms, 0% errors, max consumer lag 208
  (bounded, drained to 0). 600 RPS was the first unsustainable tier (lag grew to 13,461 and was
  still climbing when load stopped).
- Source: `docs/performance/benchmark-results.md`, Test B results table.

## C. Backend optimized (Kafka consumer concurrency 1 → 3)

- **Change**: `signals.received.v1` listener concurrency raised from 1 to 3 (matching the topic's 3
  partitions). Code: `services/incident-service/.../kafka/KafkaConsumerConfig.java`.
- **Sustainable throughput: ~500 → ~900 RPS, a ~1.8× improvement.**
- **900 RPS, 120-second confirmation run**: achieved **898.05 RPS**, **p50 5.86ms / p95 8.14ms /
  p99 8.97ms / max 69.22ms**, 0% errors, max Kafka consumer lag 580 **oscillating in a bounded
  168-580 range for the full 120s window with no growth trend**, draining to 0 within ~3s of load
  stopping. **108,000/108,000 signals persisted, 100 incidents, 0 duplicate `event_id`s, 100/100
  outbox events reached `PUBLISHED`.**
- **1200 RPS tier: confirmed unsustainable.** Kafka consumer lag grew continuously and without
  bound for the entire 40-second offered window (0 → 8,256, still climbing when load stopped) —
  the same explicit "grows continuously instead of stabilizing" signature used to call every other
  unsustainable tier in this project. Notably, HTTP-layer latency stayed clean even at this tier
  (p99 9.12ms) — the failure is purely a Kafka-consumer-side limit, not a client-visible one.
- Source: `docs/performance/optimization-1-reevaluation.md` (the controlled 300 RPS
  concurrency=1-vs-3 comparison and correctness data) and
  `docs/performance/load-generator-validation-and-final-ceiling.md` (the corrected-methodology
  500/700/900/1200 RPS escalation and the 900 RPS confirmation run quoted above).

## D. Idempotency (Test C)

Three duplicate-traffic patterns, full real pipeline, required invariant (100% suppression, zero
duplicate signals/incidents/investigation runs) **fully satisfied in all three**:

| Scenario | Requests sent | Distinct events | Suppression | Incidents | Investigation runs | Errors |
|---|---|---|---|---|---|---|
| 10% duplicate rate | 24,000 | 21,620 | **100%** | 185 | 185 | 0% |
| 50% duplicate rate | 24,000 | 12,100 | **100%** | 311 | 311 | 0% |
| Concentrated retry burst (20 IDs × 300 replays) | 6,000 | 20 | **100%** | 20 | 20 | 0% |

Signals persisted exactly equaled distinct events sent in every case (verified directly against
Postgres); investigation-run count exactly equaled incident count in every case — no duplicate
investigation runs even under the most adversarial pattern tested (300 replays of the same 20
event IDs).

Source: `docs/performance/benchmark-results.md`, Test C.

## E. Investigation coalescing / concurrency correctness (Test D)

- **100/500/1000-alert storms against a single incident** each collapsed to **exactly 2** total
  investigation runs (1 initial + 1 reinvestigation) — not proportional to alert count. Final
  `signalVersion` matched the alert count exactly in all three post-fix storms (100/100, 500/500,
  1000/1000).
- **A genuine lost-update race was found and fixed before these results could be trusted**: the
  first 100-alert storm showed `signalVersion=99` instead of 100. Root cause: `Incident` had no
  optimistic-locking field, so two independent Kafka consumer threads (the signal consumer and the
  investigation-result consumer) could load the same row and silently overwrite each other's
  mutation on flush. **Fix**: added `@Version` to `Incident` (migration
  `V8__add_optimistic_locking_to_incidents.sql`) — Hibernate now throws
  `ObjectOptimisticLockingFailureException` on the losing transaction, and the existing
  redelivery-on-failure pattern retries it against the current row version. A deterministic
  regression test forcing this exact race was added and passes; the re-run 100-alert storm showed
  `signalVersion=100` exactly.
- Post-fix, all required invariants (never more than one RUNNING investigation, correct STALE
  detection, exactly one follow-up per storm, zero duplicate investigation messages) held,
  verified directly against Postgres for every storm size, not sampled.
- Source: `docs/performance/benchmark-results.md`, Test D.

## F. AI investigation (Test F, real OpenAI calls, n=20)

- **20/20 successful investigations**, `gpt-4o-mini-2024-07-18`, real Prometheus/Loki/dependency
  evidence collection, no doubles/mocks.
- **Total investigation duration**: p50 2542.9ms, mean 2781.5ms, max 6774.1ms.
- **OpenAI's own latency**: p50 2522.1ms, mean 2767.0ms, max 6748.8ms — **OpenAI accounts for
  ≈99.5% of mean total latency** (2767.0 / 2781.5ms); everything else (evidence collection,
  publication) contributes under 1%.
- **Grounding/citation correctness**: 20/20 passed Pydantic schema validation, 20/20 had non-empty
  evidence citations, **zero hallucinated evidence IDs** (every cited ID cross-checked against that
  investigation's own evidence bundle), 20/20 confidence values within `[0,1]`, 20/20 `incidentId`
  consistency. The required stop-and-diagnose protocol was never triggered.
- Source: `docs/performance/benchmark-results.md`, Test F.

## G. Failure / recovery (Tests G & H)

Across **11 tested failure/recovery scenarios combined** (7 failure-injection + 4 recovery
scenarios): **zero data loss and zero duplicate processing observed in every scenario tested.**
This is a statement about the specific tested scenarios, not a general guarantee:

- Kafka down: bounded 5.32s failure window, clean recovery (not a hidden 60s hang).
- Postgres down: message safely retained, never lost, clean transactional rollback, ~2s recovery.
- Prometheus/Loki down: bounded ~5s timeouts, evidence correctly degraded to remaining real
  sources, no hallucinated fallback evidence.
- OpenAI down: correctly classified as non-retryable auth failure, zero billable tokens.
- Investigation Service / Incident Service hard-`SIGKILL` mid-processing: both recovered with zero
  loss and zero duplicates (50/50 and 75/75 messages respectively, cross-checked against a
  full-history multi-thousand-message Kafka scan).
- Post-outage backlog drains: Investigation Service ≤1.47s for 150 messages, Incident Service ~2s
  for a 900-signal backlog (coalesced to 418 real investigation attempts, one current result per
  incident), Postgres ~8.9s for a 500-signal backlog after reconnection.
- One documented (non-invariant-violating) architectural observation: a request that receives an
  application-level `503` can still be delivered successfully in the background by the Kafka
  producer's own internal retry window — safe in this system specifically because deterministic
  event IDs make any resulting resubmission a no-op, not a duplicate.

Source: `docs/performance/resilience-results.md` (Test G) and
`docs/performance/recovery-benchmark-report.md` (Test H).

## H. Outbox Publisher

- **Baseline: ~31.4 events/sec** (10,000-event backlog drained in ~318s). `batch-size=50`,
  `poll-interval-ms=1000`.
- **Change**: `poll-interval-ms` 1000 → 50 (single config value; `batch-size` unchanged).
- **Optimized: ~40.6 events/sec**, confirmed over an 8,000-event backlog drain (188.57s, stable
  39.2-42.1/s across six 30-second windows, no decay) — **~1.29× improvement**. Escalating the same
  lever further (poll-interval 50→10ms) showed no additional gain, confirming per-row processing
  time, not scheduling cadence, is now the limit.
- **Correctness**: 8,000/8,000 events reached `PUBLISHED`, 0 stranded, **zero duplicate Kafka
  messages** (full topic scan: 8,000 distinct keys, each exactly once).
- **Crash/recovery**: `SIGKILL` mid-drain froze the backlog exactly (823 pending / 1,177 published,
  no drift over 10s of repeated checks); clean restart drained the remainder; **2,000/2,000
  eventually published, zero duplicates** (full topic scan).
- **Measured demand at the ~900 RPS workload**: the same benchmarked run cited in section C
  (108,000 signals, 100 incidents) produced **exactly 100 outbox events total** — all 100 created
  within a 0.515-second span at the very start of the 120.26s run (direct Postgres query:
  `min`/`max(created_at)` on the incidents from that run). Averaged over the full run, that is
  **≈0.83 outbox events/sec sustained** — **the publisher therefore had ≈49× sustained headroom**
  (40.6 / 0.83) for that measured workload. The brief instantaneous burst (~194/s for ~0.5s)
  briefly exceeds the publisher's steady-state capacity, producing a self-draining ~100-row
  transient absorbed in ~2.5s — not a sustained bottleneck.
- **Caveat, stated explicitly**: that 900 RPS benchmark run had `investigation-service` stopped
  (Test B's isolation methodology), so it measures only the new-incident-creation trigger for
  outbox events, not the reinvestigation-on-completion trigger. A live system would add some
  reinvestigation-driven events, but Test D already established that this path coalesces heavily
  (1000 alerts → 2 investigation runs) and is bounded by investigation-completion rate, not raw
  signal rate — not a basis for assuming the ≈49× headroom figure would disappear, but also not
  independently measured with investigation-service live.
- Source: `docs/performance/optimization-2-outbox-results.md` (baseline/optimized/correctness/
  recovery); the ≈0.83/s and ≈49× figures are a direct Postgres query against the already-recorded
  `mp900confirm`-prefixed benchmark data from `benchmarks/results/mp900_confirm.json` (section C),
  performed as a read-only verification in this session — not a separate benchmark run, and not yet
  written into a dedicated result file.

## I. Benchmark tooling correction — do not use the earlier single-process 500 RPS latency numbers

An earlier measurement reported multi-second HTTP p95/p99/max latency (p99 in the 6-10s range, max
16-30s) at sustained 500 RPS even after the consumer-concurrency optimization. This was
**identified and confirmed as an artifact of the benchmark load generator**, not the system: the
generator (`load_connector.py`) defaults to a single OS process with one GIL-bound asyncio event
loop, which could not itself keep pace with ~500 concurrent in-flight requests, inflating its own
measured latency without any corresponding slowness in the connector, Kafka producer, or broker
(confirmed by direct instrumentation of the connector's own request-handling and Kafka producer
metrics during a live reproduction — zero slow requests logged server-side throughout a run the
client reported as severely degraded).

**Validation**: re-running the identical 500 RPS workload with `--workers 4` (multiprocess
generation) restored clean results — **achieved 499.16/500 RPS, p50 6.83ms, p99 8.32ms**, over a
full 120-second run, statistically indistinguishable from the original pre-optimization baseline.

**None of the invalid single-process latency numbers (the multi-second p95/p99/max figures) should
be used as final system metrics anywhere.** The only valid final numbers at 500 RPS and above are
the multiprocess-generator figures in section C above and this section.

Source: `docs/performance/http-tail-latency-root-cause-analysis.md` (root-cause diagnosis) and
`docs/performance/load-generator-validation-and-final-ceiling.md` (the corrected re-measurement).
