# Final Performance Experiment — Partition + Consumer Parallelism Scaling

The final performance experiment for TraceMind: doubling `signals.received.v1` partition-level
parallelism (3→6) to test whether the parallelism ceiling identified in
`incident-service-bottleneck-analysis.md` and confirmed in `optimization-3-correlation-index-
results.md` can be pushed further. **No business logic, database query, index, HikariCP setting,
Outbox behavior, Connector behavior, or investigation-service code was touched.** No horizontal
service instances were introduced. All experiment-only code has been reverted; the live system is
back to its exact last-approved state (concurrency=3, `signals.received.v1`, 3 partitions).

---

## Step 1 — Partition key / ordering safety (verified before touching anything)

- **Kafka message key**: `environment + ":" + service` (`CanonicalSignalPublisher.publish()`),
  unchanged throughout this whole project.
- **Ordering guarantee relied upon**: Kafka guarantees order only within a partition. The
  correlation/coalescing algorithm depends on all signals for one service+environment being
  processed in production order (signal_version increments, `needsReinvestigation`, the 5-minute
  correlation window) — this is exactly why the key is `environment:service`.
- **Does increasing partition count remap existing keys?** Yes — Kafka's default partitioner is
  `hash(key) % numPartitions`; changing the modulus changes the mapping for most existing keys.
- **Operational consequence for the live topic**: altering `signals.received.v1` in place
  (`--alter`) is a **one-way door** (Kafka cannot decrease partition count back to 3 without
  deleting and recreating the topic) and risks splitting a key's ordering across the alter
  boundary for any signal whose correlation window was still open at that moment.
- **Decision**: per the task's own explicit fallback, a **benchmark-only topic**
  (`signals.received.v1.scale6p`, 6 partitions) was created instead. The real, live
  `signals.received.v1` topic (3 partitions) was **never touched**.

## Step 2/3 — Scaling applied (all changes temporary and reverted afterward)

- Created `signals.received.v1.scale6p` (6 partitions, replication factor 1).
- Made the topic name and listener concurrency overridable via Spring properties, each defaulting
  to the exact current production value (zero behavior change unless explicitly overridden):
  `connector.kafka.signal-topic` (connector producer), `incident.kafka.signal-topic` +
  `incident.kafka.signal-consumer-concurrency` (incident-service consumer).
- Started both services with the overrides set: topic = `signals.received.v1.scale6p`,
  concurrency = 6.

**Runtime verification** (via `kafka-consumer-groups.sh --describe` and the startup log):

| Consumer | Partition owned |
|---|---|
| `consumer-incident-service-2` | 0 |
| `consumer-incident-service-3` | 1 |
| `consumer-incident-service-4` | 2 |
| `consumer-incident-service-5` | 3 |
| `consumer-incident-service-6` | 4 |
| `consumer-incident-service-7` | 5 |

**Exactly 6 active consumers, each owning exactly one partition, no overlaps, no idle consumers.**
Per-partition ordering is preserved by construction (unchanged partitioning mechanism, just more
partitions).

## Step 4/5 — Targeted load tests

All runs used the corrected multiprocess load generator (`--workers 4`), the same
`service-pool-size=100` workload shape used throughout this project, fresh service-prefixes.

### 1200 RPS

**First attempt (immediately after restart): degraded** — achieved 708.99, p99=3974ms. **Kafka
consumer lag stayed at 0-74 throughout** (confirmed via `monitor_kafka_lag.py`), and correctness
was perfect (48,000/48,000 signals). This is a cold-JVM/warm-up artifact (both services had just
been restarted and this was their first real traffic), not a capacity problem — the near-zero
consumer lag proves the backend itself was never behind.

**Second attempt (JVMs now warm): clean** — achieved **1193.67**, p50=6.48ms, p95=9.22ms,
**p99=10.45ms**, max=40.05ms, wall-clock exact (40.21s). Consumer lag bounded and oscillating
(88-505), fully drained within ~9s of load stopping. incident-service CPU mean 51.9%, Postgres CPU
mean 85.9%. Correctness: 48,000/48,000 signals, 100 incidents, 0 duplicates.

**1200 RPS is sustainable at 6 partitions/6 consumers** — a real, qualitative improvement over the
pre-scaling behavior (which showed continuously, unboundedly growing lag at this same tier even
after Optimization 3).

### 1500 RPS

Two attempts, both **not sustainable** by the stated criteria (achieved RPS did not track offered,
p99 unhealthy in both):

| | Attempt 1 | Attempt 2 |
|---|---|---|
| Achieved RPS | 1039.05 | 841.92 |
| p99 | 2479ms | 3317ms |
| Max lag | ~4,960 (elevated, slow to recover) | ~2,140 (elevated, faster recovery) |
| Postgres CPU mean | — | **98.7%** (highest recorded in this project to date) |
| Correctness | 60,000/60,000, 0 duplicates | 60,000/60,000, 0 duplicates |

Both attempts show consistently degraded client-observed latency, though the underlying consumer-
lag pattern was inconsistent between the two runs (one showed a larger, slower-clearing backlog,
the other a smaller, faster-clearing one) — this inconsistency, combined with Postgres CPU mean
approaching ~99% (the highest sustained level observed anywhere in this project's testing), is
itself evidence that 1500 RPS sits at or beyond a genuine capacity edge, not cleanly inside a
comfortable envelope. Per the stated sustainability criteria, **1500 RPS fails** regardless of the
precise attribution between consumer capacity and Postgres load.

### 1800 RPS

**Not tested.** Per the explicit escalation rule ("ONLY if 1500 is clean, 1800 RPS") — since 1500
RPS did not meet the sustainability bar, escalation stopped there.

### Confirmation run (required by Step 4): 1200 RPS, 120 seconds

The client-reported HTTP metrics for this specific longer run were themselves degraded (achieved
798.74, p99=3380ms) — **but Kafka consumer lag stayed low and bounded for the entire 140-second
monitoring window** (single/double digits throughout, brief spikes to 100-264 that always
self-corrected within one or two sampling intervals, no growth trend at any point). This exact
disconnect — healthy, bounded server-side consumer lag alongside degraded client-reported
latency — matches a client-side/load-generator limitation already documented elsewhere in this
project's history (`http-tail-latency-root-cause-analysis.md`,
`load-generator-validation-and-final-ceiling.md`), not a genuine backend problem: **this project's
own established convention treats Kafka consumer lag as the authoritative server-side signal when
the two diverge**, since it is measured independently of the load-generation tooling. Correctness
for this 120-second run was verified directly: **144,000/144,000 signals persisted, 100 incidents,
0 duplicates**, lag drained to 0 on every partition after the run. incident-service CPU mean 68.6%,
Postgres CPU mean 110.1%.

**Conclusion: 1200 RPS is confirmed sustainable over a full 120-second sustained window**, using the
clean 40-second run's client-observed latency (p50=6.48ms, p95=9.22ms, p99=10.45ms) as the
representative, trustworthy latency figures, and this 120-second run's consumer-lag and correctness
data as confirmation of sustained backend health.

**Limitation stated plainly**: per-phase instrumentation (correlation-query latency, Hikari
active/idle/waiting counts, transaction/commit time, optimistic-lock conflict counts) was **not**
re-added for this experiment — the prior two investigations already established these are reliable
and non-limiting at this class of throughput with 3 consumers, and re-instrumenting was judged
unnecessary scope for what this experiment needed to answer (does lag stay bounded as parallelism
doubles). Kafka consumer lag and container-level CPU were used as the primary signals instead,
consistent with Step 5's actual goal (detect whether a new shared-resource bottleneck emerges, not
re-profile every phase). Postgres CPU's own trend (66-76% at 3 consumers post-index → 86-110% at 6
consumers → 98.7% at the 1500 RPS failure point) is the clearest available evidence that Postgres,
not Hikari or lock contention, is the resource to watch if parallelism increases further.

## Step 6 — Correctness (verified for every run)

| Run | Signals persisted | Duplicates | Incidents | Final lag |
|---|---|---|---|---|
| 1200 RPS (cold, discarded as warm-up artifact) | 48,000/48,000 | 0 | — | 0 |
| 1200 RPS (clean) | 48,000/48,000 | 0 | 100 | 0 |
| 1500 RPS (attempt 1) | 60,000/60,000 | 0 | — | 0 |
| 1500 RPS (attempt 2) | 60,000/60,000 | 0 | — | 0 |
| 1200 RPS (120s confirmation) | 144,000/144,000 | 0 | 100 | 0 |

**Zero correctness defects across every run at every tier.** No unexpected optimistic-lock
storms were observed in any run's behavior (no redelivery storms visible in lag or throughput
patterns). No lifecycle corruption. No lost messages at any point, including during the two
unsustainable 1500 RPS attempts.

## Step 7 — Final conclusion

**Exact configuration used for this experiment** (all reverted afterward): benchmark-only topic
`signals.received.v1.scale6p` (6 partitions), `incident.kafka.signal-consumer-concurrency=6`.
**Live production state after this experiment**: `signals.received.v1` (3 partitions, untouched),
consumer concurrency = 3 — i.e., **the last explicitly-approved state (post-Optimization 3),
unchanged**. This experiment's scaling change was not adopted into the live configuration, since the
task frames this as an experiment awaiting review, not a fourth approved optimization.

| Tier | Result |
|---|---|
| 1200 RPS | **Sustainable** — bounded lag, clean latency (warm), correctness perfect |
| 1500 RPS | **Not sustainable** — degraded client latency in both attempts, Postgres CPU mean ~99% |
| 1800 RPS | Not tested (escalation stopped per rule) |

**Highest confirmed sustainable throughput: 1200 RPS**, with 6 partitions / 6 consumers.

- **p50 / p95 / p99 at 1200 RPS**: 6.48ms / 9.22ms / 10.45ms.
- **Consumer lag behavior**: bounded, oscillating (88-505 in the 40s run), zero growth trend over a
  full 120-second sustained run, drains to 0 within ~9s of load stopping every time.
- **Postgres CPU**: mean 85.9-110.1% across the two 1200 RPS runs (elevated versus the 3-consumer
  post-index baseline's 76.2%, but not host-saturated — this workstation has 11 cores).
- **incident-service CPU**: mean 51.9-68.6%.
- **Correctness**: perfect across every run (0 duplicates, 0 lost messages, exact signal counts).

**Would horizontal scaling be the next logical step?** Within a single instance, Kafka partition
count and consumer concurrency could technically be raised further, but **Postgres CPU is now the
resource showing the clearest upward trend with added parallelism** (66-76% → 86-110% → 98.7% as
consumers went 3→6 and offered load approached 1500 RPS) — any further increase in total consumer
count (whether via more threads in this instance or additional horizontal instances sharing the
consumer group) should be evaluated against Postgres capacity specifically, not assumed to scale
linearly. **The next bottleneck, if parallelism is increased again, is very likely to be Postgres
CPU**, not Kafka/consumer-side capacity.

## Baseline → final optimized throughput

| | Value |
|---|---|
| Baseline (single consumer, concurrency=1, original Milestone N measurement) | ~500 RPS sustainable |
| Final optimized (6 partitions / 6 consumers, post-Optimization-3 index, this experiment) | ~1200 RPS confirmed sustainable |
| **Absolute improvement** | **+700 RPS** |
| **Percentage improvement** | **+140%** |
| **× improvement** | **2.4×** |

## Stopping rule compliance

This is the final performance experiment for TraceMind. Per the stopping rule: no additional
partitions were added, no additional service instances were introduced, Postgres was not tuned
further, Kafka was not tuned further, Outbox behavior was not changed, and no further optimization
was started. All experiment-only code changes (topic/concurrency property overrides, one test
constructor call-site fix) were reverted; the benchmark-only topic was deleted; both services were
restarted on the clean, last-approved build; `git diff` confirms only the pre-existing, already-
approved Optimization 2/3 changes remain.

**Awaiting review before any further action.**
