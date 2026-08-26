# Load-Generator Validation and Final Optimized Throughput Ceiling

Direct validation of the recommendation in
`docs/performance/http-tail-latency-root-cause-analysis.md` §11: re-run the same workload with
`load_connector.py --workers 4` to test whether the single-process load generator was the actual
site of the multi-second HTTP tail latency observed at ~500 RPS. **Result: confirmed.** This
document supersedes the "500 RPS still has an unresolved tail-latency bottleneck" framing in
`optimization-1-reevaluation.md` and closes the open question in the root-cause analysis. **No
production code was changed** — concurrency=3 (Optimization 1) remains the only applied change to
the system; everything here is benchmark-methodology correction plus new capacity data.

---

## Pre-run verification

Confirmed before any run in this session: Kafka consumer lag 0 on all partitions, swap 0/0, memory
pressure level 1 (normal), all 4 containers healthy, investigation-service correctly excluded
(consistent with Test B's isolation methodology), no stale benchmark processes, no new
instrumentation added (this validation used only the already-existing `--workers` flag on
`load_connector.py`, present since Test A).

## Does the multiprocess generator fix the tail latency? Yes — decisively.

### 500 RPS

| Run | Achieved RPS | Wall time (target) | p50 | p95 | p99 | max | Max lag |
|---|---|---|---|---|---|---|---|
| Single-process (prior finding) | 241.74 | 186.1s (90s) | 1,224ms | 6,768ms | 10,557ms | 23,563ms | n/a (lag was fine even then) |
| **4-worker, 40s discovery** | 497.98 | 40.16s (40s) | 6.93ms | 799.71ms | 2,500.93ms | 4,771.7ms | — |
| **4-worker, 120s confirmation** | **499.16** | **120.20s (120s)** | **6.83ms** | **7.85ms** | **8.32ms** | **38.76ms** | **15** |

The 40-second discovery run still showed a smaller residual tail (p99=2.5s). Direct inspection of
the completion-order raw sample showed this was **entirely a one-time startup transient**: 52-95%
of the first ~800 completions were slow, then **zero** slow requests (max latency 8-12ms) for the
remaining ~1,200 samples. This is diluted away almost completely in a realistic-duration run — the
120-second confirmation run is **statistically indistinguishable from the original Milestone N
clean baseline** (p50=7.02ms, p99=8.71ms).

**Conclusion: the previously-observed multi-second HTTP tail latency at 500 RPS is classified as a
benchmark-generator artifact** — a single GIL-bound asyncio process could not keep pace with
generating and completing ~500 concurrent requests/sec, inflating its own measured latency without
any corresponding slowness in TraceMind's connector, Kafka producer, or broker. This is exactly what
`http-tail-latency-root-cause-analysis.md`'s instrumentation had already shown from the server side
(zero slow requests logged server-side throughout a run the client reported as severely degraded);
this validation confirms the client-side half of that picture directly.

**The server-side evidence from that investigation is preserved and remains valid** — it correctly
ruled out the Kafka broker, the Kafka producer, and connector-service's own request handling as the
site of the delay. Nothing in this document contradicts that; it confirms the "best-supported,
not-yet-proven" conclusion that document reached.

## Escalation: finding the ceiling with the corrected generator

Fast escalation (500 → 700 → 900 → 1200 RPS), short (40s) discovery runs, concurrency=3 unchanged
throughout, investigation-service stopped throughout (Test B isolation methodology):

| Tier | Achieved RPS | Wall time (40s target) | p50 | p95 | p99 | max | Max lag | Lag behavior |
|---|---|---|---|---|---|---|---|---|
| 700 | 697.22 | 40.16s | 6.8ms | 8.01ms | 8.63ms | 22.66ms | 92 | drains to 0 immediately |
| 900 | 896.43 | 40.16s | 5.79ms | 8.07ms | 9.07ms | 27.6ms | 361 | oscillates, drains to 0 |
| **1200** | 1,194.23 | 40.19s | 5.87ms | 8.44ms | 9.12ms | 29.28ms | **8,256, still climbing** | **grows continuously for the entire 40s window** — the explicit unsustainable signature |

**1200 RPS fails the sustainability criterion decisively** — lag grew continuously from 0 to 8,256
across the full offered-load window without stabilizing (only beginning to drain once load
stopped), exactly matching the "grows continuously instead of stabilizing" stop-criterion used
throughout this project's benchmark history (e.g. the original Milestone N baseline's 600/700 RPS
tiers). Notably, **HTTP-layer latency stayed clean even at 1200 RPS** (p99=9.12ms) — the failure is
purely a Kafka consumer-side throughput ceiling, not a client-visible latency problem, consistent
with everything already known about where this architecture's bottleneck lives.

900 RPS is the last tier where lag stayed bounded (oscillating, not growing) and fully drained.
Per instruction, escalation stopped here rather than bisecting between 900 and 1200 — the ladder
already localizes the ceiling to "somewhere in (900, 1200]" without more runs.

## Confirmation run at the candidate ceiling (900 RPS, 120s)

| Metric | Value |
|---|---|
| Achieved RPS | 898.05 / 900 offered |
| Wall time (target 120s) | 120.26s |
| p50 / p95 / p99 / max | 5.86ms / 8.14ms / 8.97ms / 69.22ms |
| Error rate | 0% |
| Max Kafka consumer lag | 580, oscillating in a bounded range (168-580) for the full 120s window, no growth trend |
| Lag after load stops | drains to 0 within ~3s |
| Signals persisted | 108,000/108,000 |
| Incidents created | 100 (= service pool size) |
| Duplicate `event_id`s | 0 |
| Outbox events published | 100/100, 0 `PENDING` |

**900 RPS is confirmed as a genuinely sustainable tier** — bounded, equilibrium-oscillating lag
(not growing), clean HTTP latency matching the 500 RPS baseline almost exactly, and every
correctness invariant holds over the full 120-second run.

## Final defensible numbers

| | Concurrency=1 (original Milestone N baseline) | Concurrency=3, single-process generator (misleading) | **Concurrency=3, corrected multiprocess generator** |
|---|---|---|---|
| Sustainable throughput | ~500 RPS | *(appeared unresolved/regressed — benchmark artifact)* | **~900 RPS** |
| p50 / p99 at sustainable ceiling | 7.02ms / 8.71ms | 32-46ms / 8.1-9.4s (artifact) | **5.86ms / 8.97ms** |
| First tier showing genuine unsustainability | 600 RPS (lag climbing) | n/a | **1200 RPS (lag climbing continuously)** |

**Optimization 1 (Kafka listener concurrency 1→3), correctly measured, raises the backend's
sustainable throughput ceiling from ~500 RPS to ~900 RPS** — roughly a **1.8× improvement** — with
HTTP-layer latency at the new ceiling matching the original baseline's latency at its (lower)
ceiling almost exactly. The apparent "separate, unresolved tail-latency bottleneck" reported in
`optimization-1-reevaluation.md` is retracted: it was never a property of the system, only of the
single-process benchmark tool used to measure it.

## Documentation status

- `docs/performance/optimization-1-reevaluation.md` — §5/§6/§9/§10/§12's characterization of an
  "unresolved, separate tail-latency bottleneck at 500 RPS" is **superseded by this document**. The
  300 RPS controlled comparison and the lag-reduction findings in that document remain valid and
  unaffected.
- `docs/performance/http-tail-latency-root-cause-analysis.md` — its server-side findings (§6-§8,
  §10) are **confirmed and preserved**; its §11 recommendation has been **carried out and validated**
  by this document; its §12 "moderate-to-high confidence, not proven" framing is now upgraded to
  **confirmed** for the load-generator explanation specifically (the broker-JMX gap it flagged
  remains genuinely open, but is now moot for capacity-planning purposes given this result).
- Prior committed baselines (`benchmark-results.md`, etc.) are historical record of the concurrency=1
  system and are unaffected by this document.

No Optimization 2 has been performed. No production code was changed in this validation.
