# HTTP Tail-Latency Root-Cause Analysis (Post-Optimization-1)

Root-cause investigation into the severe HTTP p95/p99/max tail latency observed at sustained
~500 RPS, which persists even though Optimization 1 (Kafka listener concurrency 1→3) essentially
eliminated Kafka consumer lag at that tier (`docs/performance/optimization-1-reevaluation.md`).
**No optimization was performed.** All instrumentation added for this investigation has been
reverted; the codebase is back to its pre-investigation state (verified: `git diff` shows only the
already-kept concurrency=3 change in `KafkaConsumerConfig.java`, plus new result/doc files).

> **CONFIRMED, 2026-08-26**: the §11 recommendation (re-run with `load_connector.py --workers N`)
> was carried out in `docs/performance/load-generator-validation-and-final-ceiling.md`. Result: the
> single-process load generator was indeed the site of the missing seconds — a 4-worker run at the
> same 500 RPS is statistically indistinguishable from the original clean baseline (p50=6.83ms,
> p99=8.32ms). The server-side findings below (§6-§8, §10) are unaffected and remain the evidence
> that proved this; §12's "moderate-to-high confidence, not proven" is now confirmed for the
> load-generator explanation specifically.

---

## 1. Symptom

At sustained ~500 RPS, a representative no-monitoring-overhead run showed:

| Metric | Value |
|---|---|
| p50 | ~7ms |
| p95 | ~3.3s |
| p99 | ~6.5s |
| max | ~16s |

This tail-latency pattern reproduces regardless of Kafka listener concurrency (1 or 3) and
regardless of benchmark-monitoring overhead (confirmed in `optimization-1-reevaluation.md`). It
does not correlate with Kafka consumer lag, which is now flat and near-zero under concurrency=3.
**0% HTTP error rate** in every run — every request eventually receives `202 Accepted`, no matter
how long it took.

## 2. Current architecture/path

```
HTTP request → Tomcat (connector-service) → AlertIngestController.ingest()
  → PrometheusToCanonicalSignalMapper.map()
  → CanonicalSignalPublisher.publish() [per signal]
      → kafkaTemplate.send(topic, key, signal)   // returns a CompletableFuture
      → future.get(sendTimeout=5000ms)           // BLOCKS the Tomcat request thread
  → ResponseEntity 202 returned
```

Key facts established by direct code inspection (Phase 0), **before any instrumentation or test
run**:

- `CanonicalSignalPublisher.publish()` **blocks the calling Tomcat thread synchronously** on
  `kafkaTemplate.send(...).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)`. The measured HTTP
  latency is, by construction, "time this Tomcat thread spent inside this call" plus normal
  request/response overhead — there is no async hand-off, no separate callback thread, no queueing
  inside the connector's own business logic.
- Producer config (`application.yml`): `acks=all`, `enable.idempotence=true`, `max.block.ms=5000`,
  `connector.kafka.send-timeout-ms=5000`. `max.block.ms` bounds how long `send()` itself can block
  (e.g. waiting for topic metadata); the separate `sendTimeout` bounds how long the caller waits for
  the returned future to complete (i.e., for the broker ack). Both are documented as kept in sync
  intentionally (code comment, added during Milestone F's Kafka-down validation).
  **No producer/broker/Tomcat/HikariCP config was changed for this investigation.**
- Any timeout in either phase throws (`KafkaException`, `TimeoutException`, `ExecutionException`,
  `InterruptedException`), all mapped by `GlobalExceptionHandler` to **HTTP 503**, never silently
  swallowed into a 202. This was verified by reading `GlobalExceptionHandler.java` directly, not
  assumed.
- **This is the single most important fact established before any test was run**: given the ~5-10s
  combined theoretical bound on the connector's own code, and 0% error rate observed at every tier
  including runs with p99 in the 6-10 second range and max up to 20-30 seconds, **the connector's
  own synchronous publish path cannot be the full explanation for the observed client-side
  latencies** — either the actual server-side work is completing in a bounded, healthy time and
  something else is inflating what the client measures, or there is a mechanism this reasoning
  missed. This motivated instrumenting the actual code path directly rather than assuming either
  direction.

## 3. Hypotheses considered

1. **User's primary hypothesis**: single-node Kafka broker intermittently unable to service
   Produce requests fast enough, causing broker-side queueing / delayed acks, surfacing as
   connector HTTP tail latency.
2. Kafka producer-side queueing/buffer exhaustion inside connector-service (accumulator, batching,
   metadata refresh, retries).
3. Connector-service (Tomcat) thread-pool exhaustion or internal blocking.
4. The client-observed latency does not correspond to genuine server-side processing time at all —
   i.e., the "missing seconds" are being lost outside the connector→Kafka→broker path entirely
   (client-side scheduling, OS/network layer, or benchmark-tooling artifact).

Per instruction, hypothesis 1 was actively targeted for falsification, not assumed correct, and the
prior mistake of inferring broker load purely from `docker stats` container CPU% was explicitly not
repeated.

## 4. Instrumentation added

All instrumentation was additive, threshold-gated (only logs when a request exceeds 500ms, to avoid
flooding logs at 500 RPS while catching every genuine tail event), and has since been **fully
reverted** (confirmed via `mvn test` — 8/8 tests pass both before and after reversion, and `git
diff` shows no trace of it remaining).

1. **`CanonicalSignalPublisher.publish()`** — split the existing `send()` / `.get()` call into two
   timed phases using `System.nanoTime()`: `sendCallMs` (time for `kafkaTemplate.send()` itself to
   return a future — covers metadata-block/accumulator-enqueue time) and `ackWaitMs` (time from
   having the future to the broker ack being observed). Logged together with `eventId` when the
   total exceeded 500ms. **No control flow, exception handling, or timeout behavior was changed.**
2. **`RequestTimingDiagnosticFilter`** (new, servlet `Filter`) — measures T0 (request enters the
   filter chain, before the controller runs) to T4 (response about to return), i.e. total
   server-side wall time for the whole request including mapping and every signal's publish. Scoped
   to `/integrations/prometheus/alerts` only.
3. **`ProducerMetricsDiagnosticLogger`** (new, self-contained `@PostConstruct`/`@PreDestroy`
   scheduler, no changes to application wiring) — logs the producer's own exposed JMX-style client
   metrics (`request-latency-avg/max`, `record-queue-time-avg/max`, `buffer-available-bytes`,
   `record-retry-rate`, `record-error-rate`, `requests-in-flight`, `produce-throttle-time`, etc.)
   every 2 seconds via `producer.metrics()` — data the producer already collects internally, not new
   instrumentation of Kafka itself.
4. **`load_connector.py --per-request-csv`** (new, optional, off by default) — dumps every request's
   `(send_epoch, latency_ms, status)`, not just the pre-existing capped 2000-row raw sample, so a
   specific slow client-observed request's wall-clock time could be correlated against the
   server-side logs above. This is the only change to a benchmark script; it is purely additive
   (existing callers/behavior unaffected) and has been reverted along with everything else.
5. **Targeted `jstack` thread dumps** of the connector-service JVM (read-only, no code change) taken
   at three points during the reproduction run.

**Broker JMX (Phase 3) was not enabled.** It requires restarting the shared Kafka container (which
would force a consumer-group rebalance affecting the already-kept Optimization 1 setup) and,
critically, **was not needed**: the evidence from instrumentation 1-3 already fully accounts for
the connector-side and producer-side path with a healthy result, which is sufficient to falsify the
primary hypothesis without needing broker-side confirmation (see §7, §10, §12 for the honest
limitation this leaves).

## 5. Reproduction methodology

A single, minimal-as-practical reproduction: 500 RPS offered, 90-second target duration, using only
`load_connector.py` (no `downstream_sink.py`/`monitor_resources.py`/`monitor_kafka_lag.py` running,
per the prior finding that monitoring overhead is not the cause) plus the instrumentation above.
Environment was verified healthy immediately before the run: swap 0/0, memory pressure level 1,
Kafka consumer lag 0 on all partitions, all 4 containers healthy, no stale benchmark processes.
Concurrency remained at 3 (the currently-kept Optimization 1 state) — this investigation is about
the HTTP path, not consumer lag, so no change was needed there.

## 6. Time-correlated evidence

This is the core of the investigation. All of the following come from the **same run, same time
window**.

### Client-observed result (this exact run)

| Metric | Value |
|---|---|
| Offered / achieved RPS | 500 / 241.74 |
| Wall time (target 90s) | **186.1s** |
| p50 | 1,224.25ms |
| p95 | 6,767.83ms |
| p99 | 10,556.61ms |
| max | 23,563.13ms |
| Error rate | 0% |

Latency distribution across all 45,000 requests: only 2,575 (5.7%) completed under 50ms; 19,997
(44%) fell in the 1-5 second range; 4,890 (10.9%) exceeded 5 seconds. **This is not a small tail —
close to half of all requests were multi-second from the client's perspective**, even though p50 is
reported as a healthy-looking 7ms in other runs (see §9 for why these are not contradictory).

### Server-side, same run, same time window

- **Kafka producer metrics, sampled every 2s for the entire 186-second run** (excerpted; the full
  pattern is constant throughout):

  | Metric | Value (constant throughout) |
  |---|---|
  | `request-latency-avg` | ~0.36-0.38ms |
  | `request-latency-max` | 2-7ms |
  | `record-queue-time-avg` | ~4.15-4.18ms |
  | `record-queue-time-max` | 6-13ms |
  | `requests-in-flight` | 0.0 |
  | `buffer-available-bytes` | 33,554,432 (i.e. **100% of the buffer free**, essentially unused) |
  | `record-retry-rate` | 0.0 |
  | `record-error-rate` | 0.0 |
  | `produce-throttle-time-avg/max` | 0.0 / 0.0 |

  **Not one sample, at any point in the 186-second run, showed any sign of producer-side queueing,
  buffer pressure, retries, or throttling.**

- **`CanonicalSignalPublisher` and `RequestTimingDiagnosticFilter` diagnostic logs (500ms
  threshold)**: **zero log lines for the entire run.** Not one of the 45,000 requests ever took
  longer than 500ms inside the connector's own code — from Tomcat receiving the request, through
  mapping, through `kafkaTemplate.send()`, through waiting for the broker ack, to the response being
  ready to return.

- **`jstack` thread dumps of connector-service, taken at 3 points during the run** (t≈9s, t≈20s,
  t≈33s into the run): consistently **99-101 of ~102-104 Tomcat worker threads in
  `TIMED_WAITING (parking)`** (idle in the executor queue — not blocked on anything, waiting for
  work) at every snapshot, only **1-3 threads** ever `TIMED_WAITING (on object monitor)` (consistent
  with briefly waiting on the Kafka producer future, matching the sub-10ms ack times measured
  above), and the **`kafka-producer-network-thread` consistently `RUNNABLE`** (healthy, actively
  polling — never stuck).

### Time-window breakdown (per-request CSV, bucketed by 10-second windows)

| Window | n | p50 | p95 | max |
|---|---|---|---|---|
| t=0-10s | 2,557 | 1,362ms | 7,068ms | 15,906ms |
| t=30-40s | 3,378 | 959ms | 5,657ms | 19,726ms |
| t=60-70s | 2,100 | 1,234ms | 7,509ms | 23,563ms |
| t=100-110s | 2,163 | 1,323ms | 7,320ms | 16,314ms |
| t=140-150s | 2,481 | 1,203ms | 7,254ms | 18,340ms |
| t=180-190s | 709 | 1,490ms | 4,293ms | 5,815ms |

**The degradation is present from the very first 10-second window and stays roughly flat for the
entire 186-second run** — there is no ramp-up, no warm-up period, and critically, **no correlation
with the Kafka consumer-lag climb-and-drain pattern** observed in earlier runs (which built up over
~60-90s then drained). This is a different phenomenon from the lag dynamics Optimization 1 targeted
and fixed.

## 7. Hypotheses falsified

1. **Primary hypothesis (Kafka broker overload causing delayed producer acks): falsified by direct,
   time-correlated evidence.** If the broker were intermittently failing to service Produce
   requests, that would show up as elevated `request-latency-avg/max` or `record-queue-time-max` on
   the producer, since those metrics measure exactly "how long did this producer wait for the
   broker to ack." They did not move from their healthy baseline (max single-digit milliseconds) at
   any point during a run whose client-observed p99 was over 10 seconds. **If the connector saw an
   ~8-second Kafka send, the producer's own metrics would show it — they do not, at any sampled
   point in this run.**
2. **Producer-side queueing/buffer exhaustion: falsified.** `buffer-available-bytes` stayed at
   100% free the entire run; `requests-in-flight` was 0 at every sample; zero retries, zero errors,
   zero throttling.
3. **Connector-service (Tomcat) thread-pool exhaustion or internal blocking: falsified.** 99-101 of
   ~102-104 worker threads were idle at every snapshot; only 1-3 were ever actively waiting on
   anything, consistent with genuinely fast Kafka acks, not a backlog.
4. **This investigation's own `CanonicalSignalPublisher`/filter instrumentation ruled out the
   entire connector-side code path as the site of the delay**: zero requests, out of 45,000, ever
   exceeded 500ms inside the code that the client's HTTP latency is supposedly measuring.

## 8. Proven / best-supported root cause

**The "missing seconds" are not occurring anywhere inside the connector-service JVM, the Kafka
producer, or (by strong inference, see §12 limitation) the Kafka broker's request-handling itself.**
Every instrumented point along the HTTP request → Kafka producer → broker-ack → HTTP response path
shows healthy, sub-10-millisecond behavior for the entire duration of a run whose client-observed
p99 exceeded 10 seconds.

The best-supported explanation, given what was directly measured, is that **the delay is being
introduced outside the system under test — most likely in the benchmark load generator itself
(`load_connector.py`)**, which runs as a single OS process with one GIL-bound asyncio event loop
managing up to 500 concurrent in-flight coroutines by default (`--workers` was not used in this or
prior 500 RPS runs). Supporting evidence, all from this exact run:

- The client's own **achieved RPS fell to 241.74 against 500 offered, and wall-clock stretched to
  186.1s against a 90s target** — the client itself could not sustain issuing/completing requests
  at the offered rate, independent of anything the server did.
- This matches the earlier root-cause analysis's finding (`optimization-1-root-cause-analysis.md`)
  that the single-threaded Python load generator process climbed to and sustained 60-72% of one CPU
  core under comparable load, and that "the ~250-450 in-flight requests were not queued inside
  connector-service at all... queued somewhere the connector's own thread dump cannot see."
- A single Python process's asyncio event loop is cooperatively scheduled on one OS thread: if
  maintaining hundreds of concurrent coroutines' bookkeeping (JSON construction, HTTP call
  lifecycle, response parsing) exceeds what that one core can do in real time, individual
  coroutines' `await client.post(...)` calls can sit fully resumable (their socket response already
  received) without the event loop getting around to resuming them — inflating the
  `time.monotonic()`-measured latency without any corresponding slowness on the wire or on the
  server.

## 9. Why p50 remains healthy while p95/p99 explode

Two independent effects both point the same direction and are consistent with a client-scheduling
explanation rather than a server-side one:

- **Not all coroutines are starved equally.** A single-threaded event loop still services requests
  in roughly the order it gets to them; some fraction of coroutines get their turn quickly (fast
  p50), while others wait behind however much other event-loop work is queued ahead of them at that
  moment (the long tail). If the server itself were slow, *every* concurrently in-flight request
  would share in that slowness roughly proportionally — but the connector's own logs show **zero**
  slow requests, at any point, which a genuinely slow server could not produce while still returning
  a low p50 to the same client.
- **In this specific 90s-duration run, however, p50 itself was also degraded (1,224ms)** — worse
  than the ~7ms seen in a separate, shorter no-monitoring confirmation run referenced in
  `optimization-1-reevaluation.md`. This is itself informative: it shows the degree of client-side
  contention is not perfectly reproducible run-to-run (consistent with a resource-scheduling
  phenomenon on a shared, non-isolated host, rather than a deterministic server-side limit), and
  underscores that **"p50 stays healthy" is not universally true across every run** — what is
  consistent across every run examined (this one and the prior no-monitoring run) is that the
  server-side instrumentation shows no corresponding slowness whenever the client reports one.

## 10. Kafka broker overload hypothesis: supported or falsified?

**Falsified**, to the extent it is possible to falsify with the tooling available in this
environment. The producer's own client-side metrics (`request-latency-max`, `record-queue-time-max`)
are the direct, standard signal for "is the broker taking a long time to ack my sends," and they
never moved from a healthy baseline (single-digit milliseconds) throughout a run with 10+ second
client-observed p99. This directly contradicts the mechanism the hypothesis proposes. The one honest
gap (§12) is that broker-side JMX `RequestMetrics` — the most authoritative possible source — was
not collected, so a broker-internal issue that somehow doesn't surface in producer-observed latency
cannot be mathematically ruled out to 100% certainty. But that would require a mechanism where the
broker is slow to process a request yet the producer client observes a fast ack for it, which is not
how the Produce request/response protocol works — this is a very weak residual possibility, not a
live competing explanation.

## 11. Recommended Optimization 2 (not implemented)

Recommendation is **diagnostic, not a code change**, since the leading hypothesis points at the
benchmark tooling rather than the system under test:

1. **Re-run the same 500 RPS reproduction using `load_connector.py --workers N`** (N ≥ 4, matching
   the approach Test A's methodology already established for RPS tiers where a single process hits
   its own ceiling) to test directly whether spreading request generation across multiple OS
   processes eliminates the tail latency. If it does, this conclusively confirms the load generator
   itself as the site of the "missing seconds," fully exonerating the connector/Kafka/broker path.
   If severe tail latency persists even with multiple worker processes, that would be new evidence
   against this document's conclusion and would need to reopen the broker/network investigation
   (potentially now justifying enabling broker JMX).
2. **If confirmed as a load-generator artifact**, no application-side Optimization 2 is warranted at
   all for this specific symptom — the ~500 RPS "ceiling" observed throughout this project's
   benchmark history may itself need re-interpretation as partially a benchmark-tooling limit on
   this workstation rather than purely a TraceMind capacity limit. This would be a significant,
   retroactive correction to how prior Test A/B/Optimization-1 tail-latency numbers should be read,
   and should be validated (via step 1) before being treated as settled.

## 12. Confidence level and remaining uncertainty

**High confidence** that the Kafka broker-overload hypothesis, as specifically stated, is false —
this is supported by direct, time-correlated, multi-point evidence (producer metrics + connector
code-path timing + Tomcat thread state), not inference from container-level CPU%.

**Moderate-to-high confidence, not proven**, that the benchmark load generator itself is the actual
site of the missing seconds. This is the best-supported explanation given everything measured, and
is consistent with a prior, independent investigation's findings, but it was not directly confirmed
by instrumenting the Python client's own event-loop scheduling delay in this session — the
recommended `--workers N` re-run (§11) is what would move this from "best-supported explanation" to
"proven."

**Explicitly not resolved**: broker-internal JMX `RequestMetrics` were not collected (would require
a Kafka container restart, out of scope for this diagnostic pass). This leaves a small, structurally
implausible residual possibility that some broker-internal effect is invisible to producer-side
metrics — stated honestly rather than papered over, per instruction not to invent certainty beyond
what the evidence supports.

No Optimization 2 has been implemented. Stopping here per instruction.
