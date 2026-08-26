# Benchmark Environment Verification

Purpose: determine whether the original Test B baseline is still reproducible on this machine,
before making any further optimization decisions. Kafka listener concurrency was reverted to the
original baseline value (1) — no other code or configuration was changed.

## Step 1 — Revert

`KafkaConsumerConfig.java`: removed `factory.setConcurrency(3)` from `kafkaListenerContainerFactory`.
No other change. Rebuilt, 28/28 tests pass. Verified post-restart via
`kafka-consumer-groups.sh --describe`: a single consumer (`consumer-incident-service-2`) owns all 3
`signals.received.v1` partitions — concurrency=1 confirmed.

## Step 2 — Environment verification

| Check | Result |
|---|---|
| Stale Kafka consumer groups | 9 idle `benchmark-sink-*` groups found (leftover from earlier test runs, no active members). Inert — not deleted, since cleanup wasn't authorized for this task, but flagged. |
| Kafka lag | 0 across all partitions, both `incident-service` and `investigation-service` groups, before and after restart |
| Topic backlog | 439 MB across the 3 tracked topics — accumulated history from the whole project, not a runaway backlog |
| PostgreSQL health | 6 connections (5 idle, 1 active), no bloat concerns, all containers `Up ... (healthy)` |
| Docker Desktop health | all 4 containers healthy; idle CPU 0.01-3%, memory well within limits |
| Background benchmark processes | none found (`load_connector.py`, `monitor_*.py`, `downstream_sink.py` all clear) |
| Host CPU idle before starting | 69.6% idle, load avg 3.07-3.38 (11 cores) — reasonably idle |
| **Memory stability** | **stable but chronically constrained**: swap 10.1-10.4GB used of 11.25GB (90-92%), only ~70-100MB free RAM at any point. Not actively leaking (68MB→68MB over a 5s idle check) but draining further under load (1148MB→872MB free swap across the two verification runs) |
| Clean service restarts | incident-service, connector-service, investigation-service all stopped and restarted cleanly; startup logs confirm healthy partition assignment and `AI_TEST_DOUBLE=true` on investigation-service |

**The memory finding is the one substantive anomaly**: this host has almost no free RAM and is
~90% into a fixed swap allocation. This is flagged here and revisited in the conclusion below.

## Step 3 — Test B re-run (concurrency=1, same methodology as original baseline)

Same tooling as the original baseline and as Optimization 1's benchmarking: `load_connector.py` +
`downstream_sink.py` (isolated, investigation-service not in the loop for the tiered runs) +
`monitor_resources.py` + `monitor_kafka_lag.py`, fresh service-prefix per tier, 120s per tier. Plus
the supplementary 200 RPS/20s webhook-to-persistence measurement
(`measure_persistence_latency.py`, joined against `signals.created_at`).

## Step 4 — Comparison against the original committed baseline

| Metric | Original baseline (300 RPS) | Verify re-run (300 RPS) | Original baseline (500 RPS) | Verify re-run (500 RPS) |
|---|---|---|---|---|
| Achieved RPS | 299.38 | **211.56** | 498.87 | **282.18** |
| p50 latency | 7.82ms | **1,401.08ms** | 7.02ms | **310.33ms** |
| p99 latency | 10.06ms | **11,832.20ms** | 8.71ms | **10,452.63ms** |
| max latency | — | **25,488.10ms** | — | **27,913.81ms** |
| Max consumer lag | 27 | 198 | 208 | **6,045** |
| Error rate | 0% | 0% | 0% | 0% |
| Postgres CPU max/mean | 22.9% / 13.5% | **92.4% / 48.5%** | 31.8% / 25.6% | **95.0% / 61.7%** |
| Kafka broker CPU max/mean | 136.2% / 63.5% | 112.6% / 49.4% | 145.9% / 62.8% | **182.6% / 88.1%** |
| incident-service CPU max/mean | 35.7% / 26.9% | 48.6% / 22.8% | 30.0% / 24.4% | 30.2% / 21.2% |

**Webhook-to-durable-persistence latency** (supplementary, 200 RPS/20s, joined against Postgres —
this measures request-send-time to DB-row-creation-time, independent of the HTTP response returning
to the client):

| Percentile | Original baseline | Verify re-run |
|---|---|---|
| p50 | 8.72ms | **7.00ms** |
| p95 | 10.55ms | **7.93ms** |
| p99 | 16.47ms | **10.92ms** |
| max | 129.47ms | **39.72ms** |

**Correctness held in every run**: 0% HTTP errors, all requests eventually `202`, lag returned to
near-zero after each load window, 4,000/4,000 persistence-latency records matched exactly.

### Answers

**1. Did baseline performance return?**

**No.** With Kafka listener concurrency reverted to the exact original value (1) — the same
configuration the original baseline was measured on — the client-observed p50/p99/max latency and
achieved throughput are dramatically worse than the committed baseline at both tiers tested. This is
not a marginal difference; it is a 100-1,000x latency increase at the same offered load, on
identical code and configuration to what produced the original clean numbers.

**2. If yes: conclude optimization/environment interaction.** — N/A, does not apply.

**3. If no: identify the drift.**

The regression is **not** attributable to Optimization 1 — it reproduces identically with
concurrency=1. Two independent, converging pieces of evidence point at the same drift:

- **Webhook-to-durable-persistence latency (the actual system pipeline: connector → Kafka →
  incident-service → Postgres) is healthy and matches the original baseline closely** (p50 7.0ms vs.
  8.72ms, p99 10.92ms vs. 16.47ms — as good or better). The system itself is not slow.
- **The regression is entirely in the client-observed HTTP round trip** — the gap between "signal
  durably persisted" (fast, ~7ms) and "client receives the response" (seconds). This is the same
  gap identified in the root-cause analysis for Optimization 1, and it exists independent of
  Kafka consumer concurrency.
- **Host memory is the identified drift**: ~90% of a fixed swap allocation in use, under 100MB free
  RAM at idle, continuing to drain further under any load. This was not part of the original
  baseline's recorded environment. Postgres and Kafka broker CPU (as measured by `docker stats`) are
  also elevated well above their original baseline levels at identical offered load and identical
  code — consistent with a memory-constrained host spending cycles on paging/compression rather than
  application work, exactly the kind of drift that would explain both this regression and the one
  investigated for Optimization 1.

## Recommendation for the next step

Not an optimization — a **prerequisite**: this machine's current memory state (90% swap
utilization, near-zero free RAM) makes it unsuitable for trustworthy latency/throughput
benchmarking right now. Before any further optimization decision (on Kafka consumer concurrency or
anything else):

1. Free up host memory (close unrelated applications, restart Docker Desktop, or benchmark on an
   idle/dedicated machine) and re-verify swap usage drops to a low, stable baseline.
2. Re-run this same Step 3 comparison once memory is confirmed healthy. If baseline numbers return
   under those conditions, both this regression and the original Optimization 1 regression are
   explained by environment drift, not by any code change — and Optimization 1 can be re-evaluated
   on its own merits from a clean baseline.
3. Only after a clean, reproducible baseline is re-established should Optimization 1 (or any
   variant of it) be reconsidered.

No optimization was performed. Concurrency remains reverted to 1 (original baseline value).
