# Root Cause Analysis: Optimization 1 Latency Regression

**No code or configuration was changed to produce this analysis.** All evidence below was collected
by re-running the existing Test B load pattern under heavy read-only instrumentation (thread dumps,
in-container process inspection, GC logs, cgroup accounting, host-level CPU sampling) and inspecting
data already on disk (Kafka's own GC log, `docker-compose.yml`). This supersedes the "Kafka broker
CPU became the bottleneck" conclusion in `optimization-1-results.md` §4 — that conclusion was based
on `docker stats` container-level CPU% alone and turned out to be incomplete. The corrected
conclusion is below.

---

## 1. Symptom

At 500 RPS offered load (Optimization 1, concurrency=3) — a tier the concurrency=1 baseline handled
cleanly (p50=7.02ms, p99=8.71ms, sustained) — three independent re-runs all showed severe,
increasing degradation:

| Run | Offered | Achieved | p50 | p99 | max | Wall time (target 25-120s) |
|---|---|---|---|---|---|---|
| 500rps (full) | 500 | 283 | 413ms | 10,448ms | 27,300ms | 211.7s (target 120s) |
| rca1 (60s repro) | 500 | 226 | 1,066ms | 12,335ms | 29,064ms | 132.7s (target 60s) |
| rca2 (25s repro) | 500 | 294 | 537ms | 8,492ms | 18,966ms | 42.5s (target 25s) |

0% HTTP errors in every run — every request eventually got `202`. Kafka consumer lag stayed low
(≤12) throughout. This is a pure latency/throughput problem, not a correctness problem.

## 2. Initial hypothesis (from `optimization-1-results.md`)

"The Kafka broker's single-node CPU capacity is saturated by 3x more consumer-group protocol
overhead (heartbeats/fetches/commits), and that saturation delays producer acks, which is what the
connector's HTTP layer is measuring." This was based on `docker stats` showing the `tracemind-kafka-1`
container's CPU% rising from a baseline mean of ~63-65% to ~76-96% (peaks 197-226%) at the same
offered RPS.

**This hypothesis does not survive direct measurement (§4) and is rejected.**

## 3. Diagnostics performed

Reproduced the regression (rca1: 60s @ 500 RPS, rca2: 25s @ 500 RPS) while collecting, synchronized
to the same wall-clock window:

1. In-container `top -H -b -n1` snapshots of the Kafka JVM (5 snapshots, ~12s apart) — per-thread
   CPU time, not container-aggregate.
2. Kafka's own GC log (`-Xlog:gc*` was already enabled at broker startup) — full pause history for
   the entire test window, no instrumentation needed.
3. cgroup `cpu.max` and `cpu.stat` for the Kafka container — quota and throttling counters.
4. `jstack` thread dumps of `connector-service` (producer/HTTP side) and `incident-service`
   (consumer side) JVMs, 5 snapshots each, same cadence as (1).
5. `docker exec ... jcmd` — unavailable (jcmd not present in the `apache/kafka:3.8.0` image;
   substituted with `top -H` and the always-on GC log instead).
6. Repeated `ps -o pcpu` sampling of the Python load-generator process itself, every 2s during a
   fresh reproduction.
7. Host-wide `top -l 1 -o cpu` snapshots (macOS, aggregate CPU line + top processes), same cadence.
8. Postgres container: attempted the same in-container process inspection; the image has no
   `ps`/`top` (minimal Alpine-based image) — this diagnostic could not be completed the same way
   (noted as a limitation, not treated as evidence of anything).

## 4. Evidence collected

### (Q1) Did Kafka broker CPU actually become saturated? — **No. Measured, not inferred.**

In-container thread CPU-time deltas across the full 51-second window of the rca1 reproduction
(which itself produced p99=12.3s / max=29.1s latency):

| Thread | CPU time @ snapshot 1 (t=0s) | CPU time @ snapshot 5 (t=51s) | Delta |
|---|---|---|---|
| `kafka-1-request-handler` (main) | 5:18.00 | 5:18.03 | **30ms over 51s** |
| `kafka-1-metrics/network` thread | 0:50.56 | 0:50.57 | **10ms over 51s** |
| `kafka-1-socket` thread | 0:00.76 | 0:00.76 | **0ms** |

Aggregate `%Cpu(s)` line inside the container at the midpoint of the same window: `1.8 us, 0.0 sy,
96.5 id`. `load average: 1.84, 2.28, 2.53` (11 cores available). **The broker's own JVM threads did
essentially no additional work during a window that produced 27-29 second tail latencies.** This
directly contradicts the `docker stats`-based container CPU% readings used in the original
hypothesis, and is the single most important correction in this analysis (see §6).

### GC pauses — ruled out (Q15)

Kafka's GC log (`-Xlog:gc*`, already active, no instrumentation added) covering the entire test
window (all four RPS-300/500 sessions, 02:15-02:49 UTC): every pause is `Pause Young (Normal)`, none
longer than **28ms** during the actual test window; the single worst pause in the container's entire
multi-day history is 86ms. No `Full GC`, no concurrent-mark events — Old-gen regions stayed flat at
30/1024 the whole time (no memory pressure). **A 28ms GC pause cannot produce a 10-27 second
latency event; this is a three-order-of-magnitude mismatch. GC is conclusively not the cause.**

### Docker Desktop CPU throttling — ruled out (Q14)

`cat /sys/fs/cgroup/cpu.max` → `max 100000` (unlimited quota, no `cpus:` limit in
`docker-compose.yml`). `cat /sys/fs/cgroup/cpu.stat` → `nr_throttled 0`, `throttled_usec 0`. **The
kernel-level cgroup throttling mechanism never engaged. There is no artificial CPU ceiling on any
container.**

### (Q3, Q5) Where requests actually waited — connector thread dumps

At every snapshot, only **1-2 of 28** Tomcat worker threads were ever inside
`CanonicalSignalPublisher.publish()` → `CompletableFuture.get()` (i.e., actively waiting on a Kafka
producer ack) at once. The other ~26 were `TIMED_WAITING (parking)` in Tomcat's own executor queue
(`ThreadPoolExecutor.getTask` → `LinkedBlockingQueue.poll`), i.e., **idle, not overloaded**. The
single shared `kafka-producer-network-thread` was consistently `RUNNABLE` inside
`Sender.runOnce()` → `NetworkClient.poll()` → `Selector.select()` (a normal, healthy I/O-wait loop,
not evidence of the sender itself being stuck).

This is the key inconsistency that broke the original hypothesis open: **Little's Law says a
system sustaining ~226-294 achieved RPS with ~1-2s mean latency should have ~250-450 requests
concurrently "in flight."** But the connector's own thread pool never showed more than 1-2 threads
actually processing a request at once. **The ~250-450 "in-flight" requests were not queued inside
connector-service at all** — they were queued somewhere the connector's own thread dump cannot see:
between the Python client and the socket, or in transit.

### (Q1 corollary) The load generator itself — a real, measured cost

Direct `ps -o pcpu` sampling of the Python `load_connector.py` process during a fresh 25s
reproduction:

| t (s) | Python process %CPU | State |
|---|---|---|
| 0 | 30.0% | Sleeping |
| 6 | 66.5% | Sleeping |
| 8 | 71.7% | **Running** |
| 12 | 63.1% | Running |
| 16 | 68.1% | Sleeping |
| 20 | 70.4% | Running |

The single-threaded (GIL-bound, asyncio event-loop) Python process climbed to and sustained
**60-72% of one CPU core** — a real, measured, non-trivial cost of maintaining up to 500 concurrent
in-flight async HTTP requests (JSON payload construction, response parsing, event-loop task
scheduling for hundreds of live coroutines). Confirmed from source: `send_one()`'s
`start = time.monotonic()` is captured *after* acquiring the concurrency semaphore, so semaphore
queueing time is excluded from the reported latency — the measured latency is genuinely
"time inside `await client.post(...)`," meaning delay downstream of task scheduling, consistent with
the request having been sent but the response (or the event loop's attention to it) being delayed.

### (Q1 corollary) Host-wide CPU — the real magnitude gap

`top -l 1` on the host, same reproduction window:

| t (s) | Host CPU (user+sys) | Host idle |
|---|---|---|
| 0 | 31.8% | 68.2% |
| 3 | 46.0% | 54.1% |
| 6 | **76.4%** | **23.6%** |
| 9 | 43.3% | 56.7% |

Host-wide busy time jumped by **~44 percentage points within 6 seconds** of load starting. On an
11-core host, that is roughly **4-5 additional CPU-cores' worth of work appearing** almost
instantly. The Python client alone (≤1 core) cannot account for this. No other heavy diagnostic
commands were running concurrently during this specific sample (the sampler loop itself was
lightweight `ps`/`top` calls only). **Something broader than any single measured process is
consuming several cores' worth of CPU the moment this workload starts**, and none of the
per-component instrumentation above (Kafka JVM threads, GC log, connector/incident-service thread
dumps) accounts for it.

### (Q4) Did concurrency=3 quantifiably increase fetch/heartbeat/commit traffic?

Yes, structurally, by definition of the change: concurrency=1→3 means 3 independent consumer-group
members instead of 1, each running its own poll loop against the broker (confirmed in the original
optimization's partition-assignment check: 3 distinct `consumer-incident-service-{2,3,4}` client
IDs). That is a **3x increase in the number of independent Fetch/Heartbeat/OffsetCommit request
streams** hitting the broker, by construction — this part was correctly reasoned. What this
analysis corrects is *where that extra traffic's cost lands*: not inside the broker's own CPU-bound
request processing (§4/Q1, disproven), but plausibly in the **volume of host↔container network
crossings** through the Docker Desktop virtualization/networking layer — a layer neither `top -H`
inside the Kafka container nor `jstack` on any JVM can see, because it sits *between* the host's
network stack and the container's, outside every process I was able to instrument directly. This is
inference, not direct measurement — flagged honestly as the limitation it is (§6).

## 5. Root cause

**Primary, directly measured**: the regression's cost is not concentrated in any single JVM's
compute (broker, connector, or incident-service all show idle/healthy thread activity and trivial
GC pauses throughout the degraded windows). It correlates instead with a large, fast, host-wide CPU
utilization spike (+44 points of busy time in 6 seconds, ~4-5 core-equivalents) that occurs the
moment sustained load starts, disproportionate to what the load generator process itself (≤1 core)
can explain.

**Best-supported explanation for the missing capacity**: this is a **shared, single developer
workstation running Docker Desktop's virtualized Linux VM**, not a dedicated/isolated benchmark
environment. Every signal in this pipeline crosses the host↔VM boundary at least twice under
concurrency=1 (one producer send from connector, one consumer fetch from incident-service) and
**three times as often on the consumer side under concurrency=3** (3 independent poll loops instead
of 1 multiplexed one). `docker stats`' cgroup-based CPU% (which *did* show a real, large increase
for the Kafka container, 63%→76-96% mean) most likely reflects virtualization/network-stack
overhead attributed to the container's network namespace — work that happens on the host/VM side of
the boundary, not inside the JVM's own userspace threads, which is exactly why it doesn't appear in
`top -H` inside the container. Tripling the number of independent, periodic cross-boundary
round-trips is a plausible, structurally-justified (§4/Q4) driver of that overhead, even though the
broker's own application code is not where the time is spent.

**Ranked by measured contribution** (Q6):

1. **Host-wide, cross-cutting CPU contention** (measured directly: +44pp host busy time within 6s) —
   the dominant, directly-observed effect. Root component(s) not fully isolable with the tooling
   available inside this sandboxed environment (see limitations, §6).
2. **Load generator's own CPU cost** (measured directly: 60-72% of one core) — real, but
   insufficient alone to explain the host-wide spike or the multi-second server-observed latencies;
   more a contributing factor and a confound in interpreting "achieved RPS" than the root cause of
   `CompletableFuture.get()` taking seconds.
3. **Tripled consumer-group protocol traffic from concurrency=3** (structurally certain — 3x more
   independent poll/fetch/heartbeat/commit streams) — plausible amplifier of (1), not confirmed as
   directly CPU-costly inside the broker's own JVM (§4/Q1 disproves that specific mechanism).
4. **Kafka broker application-level CPU saturation** — **disproven**. Included only to state
   explicitly that it is ruled out, not a contributor.
5. **GC pauses, Docker CPU quota throttling** — **disproven / ruled out** with direct measurement
   (§4). Zero contribution.

## 6. Why the earlier hypothesis was incomplete

`optimization-1-results.md` reasoned from `docker stats` container-level CPU% alone and treated a
rising number as proof the broker's *application* was the constraint. That was a category error:
`docker stats` reports cgroup-accounted CPU for the whole container, which on Docker Desktop for
macOS includes virtualization/network-proxy overhead attributable to the container's network
namespace, not just the JVM's own userspace compute. The number was real and the direction was
real (both docker-stats-CPU and the regression got worse together), but the earlier report inferred
a specific mechanism ("broker request-handler threads are busy") without checking the one place
that would prove or disprove it — in-container, per-thread CPU accounting. That check (§4/Q1) shows
the JVM's own threads were almost completely idle, which falsifies the specific mechanism while
leaving the broader correlation (more consumer concurrency → worse regression) intact.

**Limitation, stated plainly**: this investigation could not fully isolate the exact host/VM-level
component consuming the missing ~4-5 cores, because:
- Docker Desktop's virtualization/network-proxy layer runs outside every container and every JVM I
  was able to attach to — no `top`/`ps`/`jstack` vantage point inside this sandbox reaches it.
- The Postgres container's minimal image has no `ps`/`top`, so the same in-container verification
  done for Kafka could not be repeated there; its `docker stats` numbers carry the same
  interpretation caveat but are unconfirmed either way.
- No remote JMX port is configured on the broker (only local `jmxremote=true` without a port), and
  `jcmd` is not present in the `apache/kafka:3.8.0` image, so broker-internal request-queue-time and
  request-latency metrics (`kafka.network:type=RequestMetrics`) — which would give a definitive,
  broker-reported number for "time spent waiting to be processed" vs. "time spent processing" —
  could not be pulled without adding instrumentation, which was explicitly out of scope for this
  analysis.

These are named as genuine gaps, not papered over — the evidence collected is sufficient to
conclusively **rule out** the original hypothesis and to **rule out** GC/throttling entirely, but it
identifies the true cost as environmental/host-level rather than pinpointing one exact subsystem
within that host/VM layer.

## 7. Recommended next optimization (not implemented — analysis only)

Not a code change recommendation, since none was authorized for this task — but the evidence points
investigation, if pursued next, toward:

1. **Get a clean measurement environment first.** The single largest confound in this whole
   investigation is that baseline and Optimization 1 were both benchmarked on a shared,
   non-isolated developer workstation with unrelated concurrent load (this very investigation
   included). Before drawing further conclusions about consumer concurrency specifically, the same
   A/B comparison (concurrency=1 vs. 3) should be re-run on an idle host, or with host resource
   isolation (e.g., `docker update --cpus` on the app containers, or a dedicated benchmark machine),
   to separate "cost of the code change" from "cost of a busy shared host."
2. **If the regression persists on a clean host**, pull the broker's own `RequestMetrics` JMX
   values (`RequestQueueTimeMs`, `LocalTimeMs`, `RemoteTimeMs`, `ResponseSendTimeMs` for Produce and
   Fetch) — this requires enabling a JMX port (a config change, out of scope here) and would give a
   direct, broker-reported answer to "is the delay in queueing, local processing, or the network
   send," closing the exact gap this analysis could not close with in-container tooling alone.
3. **If it does not persist on a clean host**, the conclusion becomes that Optimization 1 itself is
   sound and the original regression was a benchmarking-environment artifact, not a consequence of
   the code change — which would reverse the "mixed result" verdict in `optimization-1-results.md`.

No code, configuration, or further optimization has been implemented. This is analysis only, per
instruction.
