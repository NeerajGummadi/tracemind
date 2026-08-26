# Optimization 1 Re-evaluation — Kafka Listener Concurrency 1 → 3 (Post-Reboot, Clean Environment)

Re-evaluation of Optimization 1 authorized after `docs/performance/post-reboot-verification.md`
established that host memory was fully healthy (0 swap, pressure level 1) and that 300 RPS is a
clean, reproducible control point, while 500 RPS is an edge-of-envelope tier on this particular
shared workstation. This document supersedes the "mixed result" verdict in the original
`optimization-1-results.md`, which was measured before the memory/host-contention investigation
existed. **Concurrency=3 is currently applied and left in place** (see §12).

> **RESOLVED, 2026-08-26**: this document's characterization of an "unresolved, separate
> tail-latency bottleneck at 500 RPS" (§5/§6/§9/§10/§12) is **superseded** by
> `docs/performance/load-generator-validation-and-final-ceiling.md`. That tail latency was traced to
> the single-process benchmark load generator, not the system under test — corrected measurement
> shows concurrency=3 sustains ~900 RPS cleanly (p50=5.86ms, p99=8.97ms). The 300 RPS controlled
> comparison and the lag-reduction findings below remain valid and unaffected.

---

## 1. Environment state

- Reboot completed, memory healthy throughout this session (swap 0, pressure level 1 — verified in
  `post-reboot-verification.md`).
- All 4 containers (`kafka`, `postgres`, `loki`, `prometheus`) healthy for the full session.
- `investigation-service` deliberately stopped for all Test B runs in this document, per Test B's
  isolation methodology (`environment-verification.md`) — `downstream_sink.py` is the sole consumer
  of `investigation.requested.v1`.
- Control data (concurrency=1) reused directly from the corrected post-reboot 300 RPS run
  (`benchmarks/results/postreboot_test_b_300rps*`) and from the two independent post-reboot 500 RPS
  runs already recorded in `post-reboot-verification.md` — not re-run for this document, per
  instruction to avoid repeating completed benchmark tiers.
- One event mid-session, not related to the benchmark: **the host machine went to sleep** while the
  final no-monitor 500 RPS confirmation run's results were being reviewed. Confirmed harmless: all
  4 Kafka consumer client IDs logged a simultaneous `DisconnectException` /
  "session timed out without receiving a heartbeat response" at `11:54:51`, and HikariCP logged
  `Thread starvation or clock leap detected (housekeeper delta=17m37s887ms)` at the same timestamp —
  a clock-leap of that exact magnitude is the signature of a sleep/wake cycle, not an application or
  Kafka fault. All 4 consumers rediscovered the group coordinator within ~100ms and resumed
  normally; lag was confirmed at 0 across all partitions afterward. This happened **after** the last
  benchmark run had already completed and its results were captured, so no measurement in this
  document is affected.

## 2. Exact change made

`services/incident-service/src/main/java/com/tracemind/incident/kafka/KafkaConsumerConfig.java`:
added a single line, `factory.setConcurrency(3);`, to the `kafkaListenerContainerFactory` bean only
(the bean `SignalConsumerListener` uses for `signals.received.v1`). The
`investigationResultKafkaListenerContainerFactory` bean (`investigation.results.v1`) was not
touched. No other file was modified — partition count, HikariCP, producer settings, broker config,
correlation logic, outbox configuration, transaction boundaries, and idempotency behavior are all
unchanged.

**28/28 Java tests pass** (`mvn test`, `BUILD SUCCESS`) with the change applied.

## 3. Runtime partition/consumer verification

Confirmed via application startup log and `kafka-consumer-groups.sh --describe --group
incident-service`:

| Consumer client ID | Partition assigned |
|---|---|
| `consumer-incident-service-2` | `signals.received.v1-0` |
| `consumer-incident-service-3` | `signals.received.v1-1` |
| `consumer-incident-service-4` | `signals.received.v1-2` |

Exactly 3 consumers joined the group, each assigned exactly one partition — no partition is shared
across consumers (Kafka's group-membership protocol makes this structurally impossible within a
single consumer group, not merely something this run happened to avoid). Per-partition ordering is
therefore preserved by construction. Lag was 0 on all partitions before each test run began.

## 4. 300 RPS before/after comparison (primary controlled comparison)

| Metric | Concurrency=1 (control, post-reboot) | Concurrency=3 (this evaluation) |
|---|---|---|
| Achieved RPS | 299.93 | 299.92 |
| p50 | 5.58ms | 5.85ms |
| p95 | 7.94ms | 8.45ms |
| p99 | 8.67ms | 9.4ms |
| max | 46.8ms | 58.95ms |
| Wall time (target 120s) | 120.027s | 120.033s |
| Error rate | 0% | 0% |
| Max Kafka consumer lag | 22 | 26 |
| incident-service CPU (max) | 35.9% | 32.3% |
| Kafka broker CPU (max, `docker stats`) | 195.18% | 154.78% |
| Postgres CPU (max) | 66.89% | 37.83% |
| Signals persisted | 36,000/36,000 | 36,000/36,000 |
| Incidents created | 100 (= service pool size) | 100 (= service pool size) |
| Duplicate `event_id`s | 0 | 0 |
| Outbox events published | 100/100, 0 PENDING | 100/100, 0 PENDING |
| Sink-received investigation requests | 100 | 100 |

**Conclusion: no measurable difference at 300 RPS.** The small deltas (p99 8.67ms vs. 9.4ms, max
46.8ms vs. 58.95ms) are within normal run-to-run noise on a shared host — well inside the spread
already observed between repeated identical runs elsewhere in this project. Resource usage was, if
anything, slightly lower under concurrency=3 in this particular pair of runs; not interpreted as a
causal improvement, just noted as showing no regression. Correctness is bit-for-bit identical.

## 5. Higher-tier results (500 RPS)

| Metric | Concurrency=1 (control, run 1) | Concurrency=1 (control, run 2) | Concurrency=3 (this evaluation) |
|---|---|---|---|
| Achieved RPS | 346.4 | 318.3 | 339.02 |
| p50 | 32.76ms | 46.5ms | 35.89ms |
| p95 | 4,630.89ms | 5,250.43ms | 4,835.2ms |
| p99 | 8,150.22ms | 9,356.27ms | 8,611.16ms |
| max | 22,251.67ms | 29,704.49ms | 21,696.2ms |
| Wall time (target 120s) | 173.2s | 188.5s | 176.979s |
| Error rate | 0% | 0% | 0% |
| **Max Kafka consumer lag** | **4,105** (bell curve, ~80-90s to fully drain) | not separately captured, same pattern | **15** (flat, never accumulates) |
| incident-service CPU (max) | not captured this granularity | — | 44.1% |
| Kafka broker CPU (max) | up to 230% during the lag-climb window | — | up to 214.4% |
| Postgres CPU (max) | up to 77% | — | 70.92% |
| Signals persisted | — (not the point of comparison) | — | 60,000/60,000 |
| Incidents created | — | — | 100 |
| Duplicate `event_id`s | — | — | 0 |
| Outbox events published | — | — | 100/100, 0 PENDING |

**The lag/backlog metric — the one Optimization 1 specifically targets — is dramatically fixed**:
peak lag drops from 4,105 (climbing for ~60s, taking another ~20-30s to drain) under concurrency=1
to a peak of 15 (never meaningfully accumulating at all) under concurrency=3, across the full 140s
monitoring window. This is the clearest, most direct confirmation that the original diagnosis
(single-threaded consumer serialization is the architectural bottleneck) was correct, and that
concurrency=3 removes it.

**The HTTP-layer tail latency is not meaningfully different**: p99 8,611ms (c=3) vs. 8,150-9,356ms
(c=1) — within the same run-to-run spread already seen between the two independent concurrency=1
runs (8,150 vs. 9,356, a ~15% spread on identical configuration). Concurrency=3 neither fixes nor
worsens this dimension in any way distinguishable from noise.

### Confirmation run: ruling out benchmark-tooling overhead as the cause

A 90-second, 500 RPS run was made with concurrency=3 and **zero monitoring processes running**
(`downstream_sink.py`, `monitor_resources.py`, `monitor_kafka_lag.py` all stopped — only the bare
`load_connector.py`), to test whether the three background Python monitoring processes used in
every other run were themselves a source of host contention.

| Metric | Concurrency=3, with monitors | Concurrency=3, no monitors |
|---|---|---|
| Achieved RPS | 339.02 | 402.69 |
| p50 | 35.89ms | **7.06ms** |
| p95 | 4,835.2ms | 3,337.96ms |
| p99 | 8,611.16ms | 6,476.33ms |
| max | 21,696.2ms | 16,222.98ms |
| Signals persisted | 60,000/60,000 | 45,000/45,000 |
| Incidents | 100 | 100 |
| Duplicates | 0 | 0 |
| Outbox published | 100/100 | 100/100 |

**Monitoring overhead is not the cause of the tail-latency regression.** p50 improved substantially
without the monitors (7.06ms — genuinely clean), confirming they do add some real, measurable load.
But p95/p99/max remained severely degraded even with zero monitoring processes running — proving
the core phenomenon is a real, tail-specific behavior of this host under sustained ~500 RPS load,
not a benchmark-tooling artifact.

### 600/700 RPS: not run — deliberate, not an omission

Per the task's own sustainability criteria, a tier only counts as healthy if *all* of achieved-RPS,
error-rate, lag-boundedness, **and operationally-reasonable latency** hold. 500 RPS already fails
decisively on the latency criterion (p99 in the multi-second range) even though it now fully passes
the lag criterion. Escalating to 600/700 RPS would only reproduce the same client-facing tail-latency
failure at a still-higher offered rate, adding no new diagnostic information, and would violate the
"do not chase an arbitrary high RPS number" instruction. Stopped here by design.

## 6. Sustainable throughput conclusion

**A. Controlled comparison (300 RPS, highest-confidence apples-to-apples).** No measurable
improvement or regression in any client-facing metric. Both concurrency=1 and concurrency=3 are
comfortably inside the sustainable envelope at 300 RPS — this tier was never where Optimization 1's
target bottleneck was expected to bind.

**B. Capacity comparison.** On the specific mechanism Optimization 1 was designed to fix — a single
Kafka consumer thread serializing all `signals.received.v1` traffic — the fix is unambiguous and
large: peak lag at 500 RPS falls from 4,105 to 15, and the backlog no longer meaningfully
accumulates at all. Per the user's own framing, do not overstate the original ~500 RPS number as a
production capacity figure, since post-reboot testing already showed it is edge-of-envelope on this
workstation. With that caveat: **concurrency=3 does not raise the client-observed sustainable
throughput ceiling on this host**, because a second, independent bottleneck — not consumer lag,
confirmed by direct measurement — caps client-facing latency at roughly the same point regardless
of Kafka listener concurrency. The architectural fix is real and correct; it is simply not the
factor currently gating what a caller of this system experiences at ~500 RPS on this particular
machine.

## 7. Correctness results

All invariants held in every run in this evaluation (300 RPS ×1, 500 RPS ×2 including the no-monitor
run):

- Signals persisted exactly matched requests sent in every run (36,000 / 60,000 / 45,000 — zero
  loss, zero unexpected duplication).
- Incidents created exactly matched the service pool size (100) in every run.
- **Zero duplicate `event_id`s** in any run (verified by direct `GROUP BY ... HAVING COUNT(*)>1`
  query against Postgres).
- **Zero outbox events left `PENDING`** — 100/100 reached `PUBLISHED` in every run.
- **Zero `ERROR`-level exceptions and zero `ObjectOptimisticLockingFailureException`** anywhere in
  the incident-service log for the full concurrency=3 session. The only matches for
  "ERROR|OptimisticLock" were Kafka `DisconnectException`/coordinator-rediscovery entries tied to
  the host's sleep/wake cycle (see §1) — unrelated to the code change, self-healed within ~100ms,
  confirmed by the co-occurring HikariCP clock-leap log line.
- **Known gap, stated plainly**: Test D's previously-fixed optimistic-locking race (two independent
  consumer threads — the signal consumer and the investigation-result consumer — mutating the same
  `Incident` row concurrently) requires `investigation-service` to be running and producing
  `investigation.results.v1` traffic. Test B's isolation methodology correctly keeps
  investigation-service stopped, so **this evaluation did not exercise that specific concurrent-write
  scenario under concurrency=3**, even though concurrency=3 does increase real per-signal write
  concurrency on the signal-consumer side. This is a real, unclosed gap in this evaluation, not
  something to assume is fine by extension — flagged for whoever picks up further testing.

## 8. Resource utilization summary

| Component | 300 RPS, c=1 | 300 RPS, c=3 | 500 RPS, c=1 | 500 RPS, c=3 |
|---|---|---|---|---|
| incident-service CPU max | 35.9% | 32.3% | not captured at this granularity | 44.1% |
| Kafka broker CPU max (`docker stats`) | 195.18% | 154.78% | up to ~230% | 214.4% |
| Postgres CPU max | 66.89% | 37.83% | up to ~77% | 70.92% |

No component approached saturation at any tier tested (host has 11 cores; Kafka's >100% readings
reflect multi-thread/multi-core `docker stats` accounting, consistent with the earlier RCA's
documented caveat about this metric conflating JVM compute with virtualization overhead).

## 9. New bottleneck — measured, not assumed

Per instruction, this is reported as what was ruled **out** and what remains **unexplained**, not as
a guessed mechanism:

**Ruled out by direct measurement in this evaluation and the prior investigation chain:**
- Kafka consumer lag / single-threaded serialization — fixed by this optimization (§5).
- Host memory starvation — ruled out post-reboot (0 swap, pressure level 1 throughout).
- Kafka broker JVM's own CPU-bound compute — ruled out in `optimization-1-root-cause-analysis.md`
  via in-container per-thread accounting (30ms of broker-thread work across a 51s window that
  produced 29s latencies).
- GC pauses and Docker cgroup CPU throttling — ruled out in the same RCA (pauses ≤28ms; throttling
  never engaged).
- Benchmark-monitoring-process overhead — ruled out in this evaluation (§5): the no-monitor run
  still showed the same tail-latency pattern.

**What the evidence does show:** the problem is specifically a **tail-latency** phenomenon — p50 can
be clean (7.06ms, no-monitor run) while p95/p99/max are catastrophic (thousands to tens of thousands
of ms) in the same run. It appears at sustained ~500 RPS regardless of Kafka consumer concurrency.
It is not proportional to lag (lag is now near-zero under concurrency=3 while the tail latency
persists), meaning whatever is happening is **not** waiting on the Kafka backlog — the delay is
somewhere in the connector→Kafka producer-ack path or beyond, consistent with the original RCA's
still-unresolved finding of "requests queued somewhere the connector's own thread dump cannot see."

**Not conclusively identified.** This is the single largest open question and the recommended next
diagnostic step (§12), not something to optimize now.

## 10. Optimization improvement, in percentage/× terms where defensible

- **Kafka consumer lag at 500 RPS**: peak 4,105 → 15, a **~99.6% reduction (~274×)**. The backlog
  effectively stops accumulating at all, versus taking ~80-90 seconds to climb and drain.
- **HTTP p99 latency at 500 RPS**: 8,150-9,356ms (c=1, two runs) vs. 8,611ms (c=3) — **no
  defensible improvement**; the difference is smaller than the spread already observed between two
  identical-configuration runs.
- **300 RPS, all client-facing metrics**: no defensible difference in either direction (±5-15%,
  consistent with normal run-to-run noise).

## 11. Limitations of the local benchmark environment

- Single shared developer workstation, not a dedicated or isolated benchmark rig — other desktop
  applications (IDE, browser, chat apps) were running throughout, and the host went to sleep mid-
  session (recovered cleanly, but underscores this is an actively-used personal machine).
- `docker stats`-based container CPU% conflates a container's own JVM compute with Docker Desktop's
  virtualization/network-proxy overhead — a limitation already documented in
  `optimization-1-root-cause-analysis.md` and not resolved here.
- No Kafka broker JMX port is enabled, so broker-authoritative `RequestMetrics`
  (queue-time vs. processing-time vs. response-send-time) still cannot be obtained — this remains
  the most direct way to close the open question in §9 and was out of scope for this evaluation
  (would require a config change).
- All numbers here are directional evidence about this specific host's current behavior, not
  production capacity figures, consistent with the standing disclaimer in
  `benchmark-methodology.md`.

## 12. Recommendation

**Keep Optimization 1 (concurrency=3).** It is a clean, correctly-targeted, unambiguous fix for the
diagnosed architectural bottleneck (single-threaded Kafka consumer serialization): a ~274× reduction
in peak consumer lag at 500 RPS, zero measurable regression at 300 RPS, and zero correctness
violations across every run in this evaluation. There is no evidence here that would justify
reverting it.

**Do not credit it with raising the client-observed sustainable throughput ceiling.** That ceiling
is currently gated by a separate, tail-latency-specific phenomenon at ~500 RPS that is independent
of Kafka consumer concurrency, host memory, and benchmark-tooling overhead — all three of which were
directly tested and ruled out as the explanation.

**Recommended next diagnostic step (not performed here, not Optimization 2):** directly instrument
the connector→Kafka producer-ack path during a live 500 RPS tail-latency event — Tomcat thread dumps
paired with Kafka producer network-thread state at the exact moment a request is observed hanging
(the approach the original RCA started but could not fully close), ideally with the Kafka broker's
JMX `RequestMetrics` enabled this time to get an authoritative queue-time-vs-processing-time split.
That data would distinguish between the remaining candidates: Docker Desktop's network
virtualization layer, TCP-level backpressure between the Python load generator and Tomcat, or
producer-side batching/linger behavior under sustained (not bursty) load.

No Optimization 2 has been performed or proposed. Stopping here per instruction.
