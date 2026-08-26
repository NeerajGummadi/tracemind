# Post-Reboot Baseline Verification

Continuation of `optimization-session-handoff.md`, performed immediately after the planned reboot.
No code was changed. Kafka listener concurrency remains reverted to 1 (verified: a single consumer
owns all 3 `signals.received.v1` partitions).

---

## 1. Host memory health after reboot

| Metric | Pre-reboot (from handoff §7) | Post-reboot |
|---|---|---|
| Free RAM | ~63 MB | ~280 MB free, 447 MB "unused" at idle |
| RAM held by memory compressor | 8.21 GB | 1.5 GB |
| Swap used | 10.67 GB / 12 GB (86.8%) | **0 MB / 0 MB (no swap file exists)** |
| Kernel memory-pressure level | 2 = WARNING | **1 = normal** |

**The reboot fully resolved the memory-starvation condition** — this is a real, measured
improvement, not a marginal one. Swap was completely reset rather than merely reduced.

## 2. Stack startup

All 4 containers (`kafka`, `postgres`, `loki`, `prometheus`) came up healthy via `docker compose up
-d`. All 3 application services started cleanly. `incident-service` confirmed via
`kafka-consumer-groups.sh --describe`: a single consumer owns all 3 `signals.received.v1`
partitions (concurrency=1, unchanged). `investigation-service` confirmed `AI_TEST_DOUBLE=true`, 0
real OpenAI calls possible.

One benchmark-process note, not a system issue: `investigation-service` was first started with a
`cd` into its own directory, causing `infrastructure/topology/service-dependencies.yml` (a
repo-root-relative path) to resolve incorrectly and log a "dependency graph file not found"
warning. Restarted from the repo root with `--app-dir`; the warning did not recur. Irrelevant to
Test B either way, since Test B's isolation strategy keeps investigation-service out of the loop
entirely (see §3).

## 3. Test B re-run — methodology correction

The handoff's own step 3 says to start all three services; `environment-verification.md`'s actual
Test B methodology says investigation-service must **not** be in the loop for the tiered runs
(isolation strategy, so `downstream_sink.py` is the only consumer of
`investigation.requested.v1`). These two documents are in tension. Discovered this only after a
first 300 RPS run: with investigation-service live, `downstream_sink.py` received 3,300
`investigation.requested.v1` messages against a 100-service pool — consistent with the AI-double's
near-instant completion time triggering repeated Milestone M reinvestigation cycles under
sustained arrival, not a clean one-per-incident front-load. Stopped investigation-service and
re-ran; the sink then received exactly 100 messages, matching a clean front-loaded run. All results
below use the corrected (investigation-service stopped) methodology, per
`environment-verification.md`.

## 4. Results

| Tier | Run | Achieved RPS | p50 | p99 | max | Wall time (target 120s) | Max lag |
|---|---|---|---|---|---|---|---|
| 300 RPS | committed baseline | 299.38 | 7.82ms | 10.06ms | — | — | 27 |
| 300 RPS | post-reboot (run 1, uncorrected methodology) | 299.94 | 5.63ms | 8.81ms | 53.2ms | 120.03s | 6 |
| 300 RPS | post-reboot (run 2, corrected methodology) | 299.93 | 5.58ms | 8.67ms | 46.8ms | 120.03s | 22 |
| 500 RPS | committed baseline | 498.87 | 7.02ms | 8.71ms | — | — | 208 |
| 500 RPS | post-reboot (run 1) | 346.4 | 32.76ms | 8,150ms | 22,252ms | 173.2s | 4,105 |
| 500 RPS | post-reboot (run 2) | 318.3 | 46.5ms | 9,356ms | 29,704ms | 188.5s | not separately captured, same pattern |
| 500 RPS | 40s burst (after 2 prior 120s runs) | 499.79 | 4.74ms | 8.24ms | 33.13ms | 40.02s | clean |

**300 RPS: baseline fully returned**, and slightly beats the original numbers (p50 5.6ms vs.
7.82ms) — consistent with a genuinely healthier, less contended host.

**500 RPS: baseline did NOT return**, and this reproduced identically across two independent
120s runs (not a one-off fluke) — but the pattern differs qualitatively from the pre-reboot
regression documented in `environment-verification.md`:

- Pre-reboot: p50 1,401ms — degraded from request 1, no recovery, memory pinned throughout.
- Post-reboot: **clean start, degrades over roughly the first 60-90s, then fully recovers to near-zero
  lag while load is still ongoing** (see §5) — a transient buildup-and-drain shape, not sustained
  saturation.
- A 40-second burst at the identical 500 RPS rate, run immediately after the two 120s runs (so the
  JVMs/connection pools were already warm from ~4 minutes of continuous 300-500 RPS traffic), was
  completely clean — matching the original baseline almost exactly.

## 5. Time-series evidence (500 RPS, run 1)

Kafka consumer lag over the 120s window (`postreboot_test_b_500rps_lag.csv`):

| t (s) | Lag | | t (s) | Lag |
|---|---|---|---|---|
| 0-6 | 0 | | 62 | **4,105 (peak)** |
| 9 | 268 | | 71-79 | 3,316 → 1,255 (draining) |
| 20-50 | climbing, 1,130 → 3,457 | | 83-88 | 106 → 0 (recovered) |
| 56-65 | 3,491 → 4,105 | | 91-139 | stays ≤ 104 for the rest of the run |

`tracemind-kafka-1` container CPU (`docker stats`-based, same caveat as the earlier RCA about this
metric conflating JVM compute with virtualization overhead) tracks the same shape: ~30% at t=5s,
rising to **101-104% through t=20-50s** (the exact window lag is climbing fastest), then dropping to
**2-6% by t=80s onward** once lag has drained. `incident-service`'s own CPU stayed modest throughout
(13-21%, never close to saturated) and Postgres stayed at 10-34% — neither is the constraint.

## 6. Interpretation

**The memory-starvation hypothesis is confirmed for the 300 RPS regression** (fully resolved by the
reboot, clean reproduction twice) **but does not fully explain the 500 RPS regression** (host memory
is now healthy — 0 swap, pressure level 1 — yet 500 RPS still degrades severely and reproducibly).

The new time-series evidence points at a different, more specific explanation than either prior
hypothesis (broker CPU saturation — disproven in the RCA; raw memory starvation — ruled out here):
**500 RPS is currently sitting very close to the single-threaded consumer's effective throughput
ceiling on this particular shared workstation**, close enough that a transient perturbation early in
a sustained run (JIT warm-up, connection/pool ramp-up, or the same unexplained host-wide contention
bursts the RCA measured but couldn't attribute to one component) is enough to push arrival rate
above service rate for the first ~60-90 seconds. Because 500 RPS offers almost no headroom over the
single-thread ceiling (unlike 300 RPS, which has substantial margin), the resulting backlog takes a
long time to drain rather than being absorbed immediately — but it does fully drain and the system
does recover within the same load window, which is qualitatively different from the pre-reboot
state (sustained, non-recovering degradation) or from a permanently exceeded ceiling (which would
show unbounded, still-climbing lag at the end of a 120s window, as the original baseline's own
600-700 RPS tiers did).

This is consistent with, and refines rather than overturns, the original Milestone N baseline's own
characterization of 500 RPS as the edge of the sustainable envelope (max lag 208 there, small but
non-zero) — on a shared developer workstation with more background contention than a truly idle
box, that edge has less margin right now than it did when the original baseline was recorded.

## 7. Conclusion and recommendation

**Per the standing decision rule: baseline has not cleanly, robustly returned at 500 RPS, so
Optimization 1 (concurrency=3) should not be re-attempted yet.** 300 RPS is solid and matches
baseline. 500 RPS is reproducibly fragile in a new, better-understood way (transient warm-up/margin
sensitivity, not raw memory starvation or broker saturation), but it is a real gap from the
committed baseline that a future optimization decision should be made with eyes open to, not
worked around by picking a different host state.

Options for review (not decided here):
1. **Treat 500 RPS's current fragility as expected given "sustainable ceiling" was always closer to
   500 than to 300**, and proceed to re-evaluate Optimization 1 using 300 RPS as the primary
   clean-baseline comparison point, with 500 RPS explicitly flagged as sensitive to host
   contention on this particular machine rather than a hard pass/fail gate.
2. **Do not proceed to Optimization 1 yet.** Re-run 500 RPS a few more times at different times of
   day / after closing other desktop applications (IDE, browser, chat apps were all present in the
   post-reboot process list) to see whether the transient-warmup pattern is itself host-contention
   dependent, narrowing further whether it's a code-adjacent effect (JVM/connection-pool warmup) or
   purely environmental.
3. **Accept this as the realistic ceiling of this shared workstation** and treat any future
   optimization's 500+ RPS numbers as directional rather than strictly comparable to the original
   Milestone N baseline, since that baseline's own environment record does not document how idle
   the host was at the time.

No code or configuration was changed in this investigation.
