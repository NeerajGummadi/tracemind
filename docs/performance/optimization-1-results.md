# Optimization 1 — Kafka Consumer Concurrency

**Status: implemented, benchmarked, and STOPPED per the mandated workflow — result is mixed, not a
clean win. Do not proceed to any further optimization until this is reviewed.**

This is the first optimization made after Milestone N's baseline (`benchmark-results.md`,
`resilience-results.md`, `recovery-benchmark-report.md`). No other change has been made to the
system since that baseline.

---

## 1. Hypothesis

Test B (baseline) established: sustained throughput plateaus at ~500 RPS, consumer lag grows
unboundedly above that, while incident-service CPU (≤38%), Postgres CPU (≤37%), and HikariCP pool
usage (1/10 connections) all stayed far from saturation. Root cause: `signals.received.v1` has 3
partitions, but Spring Kafka listener concurrency was 1 — one thread processed all 3 partitions
strictly sequentially, one full transaction at a time.

**Hypothesis**: raising listener concurrency to 3 (matching the partition count) lets Kafka assign
one partition per consumer thread, so 3 signals can be mid-transaction concurrently instead of 1,
raising the sustained throughput ceiling.

## 2. Change made

`KafkaConsumerConfig.java` — added `factory.setConcurrency(3)` to the `kafkaListenerContainerFactory`
bean only (the one `SignalConsumerListener` uses for `signals.received.v1`). The
`investigationResultKafkaListenerContainerFactory` bean (`investigation.results.v1`) was left
untouched — it was not the diagnosed bottleneck.

No business logic, correlation logic, lifecycle logic, idempotency logic, or transactional
boundaries were touched. Verified post-deploy via `kafka-consumer-groups.sh --describe`: 3 distinct
consumer instances (`consumer-incident-service-2/3/4`), each assigned exactly one partition —
confirming per-partition ordering is preserved (Kafka never splits a partition across consumers in
the same group) and the change is scoped exactly as intended.

**Regression check**: full Java test suite re-run after the change — **28/28 passing, 0 failures, 0
errors**, identical to the pre-change baseline count.

## 3. Test B re-run — results

Same methodology as baseline: `load_connector.py` (open-loop) + `downstream_sink.py` (isolated
throwaway consumer, investigation-service not in the loop) + `monitor_resources.py` +
`monitor_kafka_lag.py`, fresh service-prefix per run to avoid cross-run correlation.

### Consumer lag — the metric the hypothesis directly targets

| Tier | Baseline max lag | Optimized max lag |
|---|---|---|
| 300 RPS | 27 | 11 |
| 500 RPS | 208 | **12** |

**The hypothesis was correct on this metric.** Lag at 500 RPS — baseline's own "sustainable ceiling"
— dropped by ~17x and stayed low and flat for the entire 120s window, never trending upward. The
single-threaded serialization point is gone.

### HTTP-layer (connector) latency — the metric that actually matters end-to-end

| Tier | Run | p50 | p95 | p99 | max | Achieved RPS | Error rate |
|---|---|---|---|---|---|---|---|
| 300 (baseline) | — | 7.82ms | — | 10.06ms | — | 299.38 | 0% |
| 300 (optimized) | run 1 | 6.40ms | 11.45ms | 515.81ms | 2,511.68ms | 299.92 | 0% |
| 300 (optimized) | run 2 | 6.99ms | 1,484.51ms | 4,481.91ms | 15,720.04ms | 290.95 | 0% |
| 300 (optimized) | run 3 (60s) | 6.44ms | 9.81ms | 35.04ms | 230.78ms | 299.92 | 0% |
| 500 (baseline) | — | 7.02ms | — | 8.71ms | — | 498.87 | 0% |
| 500 (optimized) | — | **413.49ms** | **6,101.04ms** | **10,447.96ms** | **27,299.70ms** | 283.41 | 0% |

**500 RPS could not sustain the offered rate at all**: wall-clock time to send 60,000 requests
stretched to 211.7s against a 120s target, and achieved throughput fell to 283 RPS — *worse* than
what baseline sustained cleanly at this same tier.

## 4. Unexpected observation — full learning-first analysis

**Symptom**: three back-to-back 300 RPS runs on *identical* code produced wildly inconsistent tail
latency (p99 35ms → 515ms → 4,482ms), and the 500 RPS re-run showed severe, consistent latency
inflation and an inability to sustain the offered send rate — despite Kafka consumer lag staying low
and flat in every single run.

**Hypothesis (initial)**: transient host noise (this is a local developer-workstation benchmark, and
variance has been disclaimed throughout this project) — ruled out below.

**Diagnostic evidence**: comparing `monitor_resources.py` CPU data, baseline vs. optimized, same
300/500 RPS offered load:

| Component | 300 RPS baseline | 300 RPS optimized | 500 RPS baseline | 500 RPS optimized |
|---|---|---|---|---|
| Postgres CPU max/mean | 22.9% / 13.5% | 92.3% / 52.4% | 31.8% / 25.6% | 94.8% / 44.2% |
| Kafka broker CPU max/mean | 136.2% / 63.5% | 225.9% / 96.4% | 145.9% / 62.8% | 197.7% / 76.2% |
| incident-service CPU max/mean | 35.7% / 26.9% | 53.1% / 32.8% | 30.0% / 24.4% | 40.5% / 21.2% |

Docker has all 11 host cores available (no `cpus:` limit in `docker-compose.yml` for any service),
so this is not an artificial container ceiling — it is real, elevated CPU draw for the *identical*
message volume.

Also checked and ruled out:
- **Not a correctness/data-loss issue**: `status_distribution` was `{"202": N}` for every single
  request in every run — 0 errors, 0 timeouts, 0 `503`s. Post-run: consumer lag = 0 on every
  partition, 0 `PENDING` outbox rows, signal counts match requests sent exactly.
- **Not explained by any single Kafka send exceeding the connector's 5s hard timeout** (would have
  surfaced as `503`s per Test H's finding — none occurred).
- **Not a lingering host process**: checked `ps aux`/`uptime`/`docker stats` between runs — no
  runaway process, no zombie test JVMs, Docker VM has the full 11-core allocation.

**Root cause**: raising listener concurrency from 1 to 3 does not just parallelize the same broker
traffic — it triples the number of *independent consumer-group members*, each running its own
poll/fetch/heartbeat/offset-commit cycle against the broker, instead of one thread multiplexing all
3 partitions inside a single `poll()` call. That per-consumer protocol overhead scales with consumer
*count*, not just message volume. The broker (`docker-compose.yml`: single KRaft node, combined
`broker,controller` roles — not a dedicated multi-broker cluster) was already the most CPU-loaded
component in the *baseline* (63–65% mean at just 300–500 RPS, well above Postgres's 13–26% and
incident-service's 24–27%) — a fact the original bottleneck analysis had no reason to flag, since lag
growth pointed squarely at the single-threaded consumer. Optimization 1 relieves that consumer-side
serialization exactly as hypothesized, but the freed-up throughput immediately runs into the
broker's own, previously-invisible CPU ceiling: producer sends (connector) and consumer fetches
(incident-service) both compete for the same broker request-handling threads, so broker CPU pressure
shows up as producer-side (connector HTTP) latency even though the consumer's own lag looks perfect.
Postgres's CPU increase (13.5%→52.4% mean at 300 RPS) is the separate, expected, roughly-proportional
cost of 3 concurrent transactions instead of 1 — not itself a problem (well under its own ceiling),
but one more source of contention riding on the same, otherwise-idle host.

**Classification**: not a correctness defect — every invariant Test B checks held (signal counts,
outbox state, zero duplicates, lag returning to 0). It is a genuine, reproducible **architectural
side effect**: the fix for the diagnosed bottleneck (single-threaded consumer) exposed the *next*
one (single-node Kafka broker CPU capacity) at a lower offered load than expected, and on this
particular deployment (one KRaft node running combined broker+controller roles in a container) that
next bottleneck is severe enough to make 500 RPS — baseline's own clean tier — perform *worse*
end-to-end than before the change.

**Fix**: none applied. Per "do not optimize beyond Kafka consumer concurrency in this iteration,"
touching broker configuration, adding broker resources, or reverting the consumer change are all
out of scope for this step and are explicitly left for review.

**Re-verification**: re-ran 300 RPS a third time (60s, full resource capture) — clean result (p99
35ms) — confirming the regression is real but *load-dependent/variable*, consistent with contention
on a shared, now-more-heavily-loaded broker rather than a deterministic code defect.

## 5. Did throughput improve?

**Mixed — not a clean win.** Per the mandated workflow ("if throughput does NOT improve, stop
immediately and identify the next architectural bottleneck"), I am stopping here rather than
proceeding to 600/700 RPS tiers or any further change, because:

- **On the specifically hypothesized metric (consumer lag), it worked as predicted** — lag at 500
  RPS dropped ~17x and the architectural serialization point is provably gone (verified via
  partition assignment: 3 consumers, 1 partition each).
- **On end-to-end system behavior, it is a regression at the same tiers baseline handled cleanly** —
  500 RPS could not sustain the offered rate (283 actual vs. 500 offered) and p99/max latency grew
  by 1,000–3,000x. Testing 600/700 RPS would only demonstrate the same broker-CPU-driven degradation
  further; it would not change this conclusion.
- The newly-exposed bottleneck (single-node Kafka broker CPU) is a different component than
  Optimization 1 touched, so investigating or fixing it here would violate "do not optimize beyond
  Kafka consumer concurrency in this iteration."

## 6. Next architectural bottleneck (identified, not fixed)

**Kafka broker CPU capacity**, on the current single-node KRaft deployment (`docker-compose.yml`:
one broker with combined `broker,controller` roles). Evidence: broker CPU mean rose from ~63% to
~76–96% for identical message volume purely from adding 2 more consumer-group members; broker CPU
was already the most heavily loaded component in the *baseline*, before this change. Candidate
follow-up investigations for a future, separately-approved optimization: whether broker request-handler/
network-thread counts are tuned appropriately for this workload, whether consumer `fetch.min.bytes`/
`fetch.max.wait.ms` tuning could reduce per-consumer poll frequency, or whether the current
single-broker topology itself becomes the thing to revisit — none of this has been investigated
yet, only flagged as the next place the system bends.

## 7. Recommendation for review

Options for the user to decide between (not decided here, per "explain trade-offs, do not make
architectural assumptions"):
1. **Revert Optimization 1** — the consumer-lag fix isn't worth a ~1,000x latency regression at the
   same 500 RPS tier the baseline already handled cleanly.
2. **Keep Optimization 1, investigate the Kafka broker as Optimization 2** — the underlying
   serialization point is fixed and lag behavior is now excellent; the newly-exposed broker-CPU
   ceiling is a distinct, addressable problem (e.g., broker thread tuning, resource allocation, or
   topology) rather than a fundamental flaw in this change.
3. **Keep Optimization 1 only for lower-concurrency values** (e.g., 2 instead of 3) — not tested
   here; would need its own hypothesis/benchmark cycle if pursued, since this iteration was scoped
   to matching the partition count exactly (3), not to finding an optimal value.

No further code changes have been made pending this review.
