You're resuming a TraceMind performance-optimization session after a machine reboot. Read this
whole file before doing anything else — do not start benchmarking or editing code until you've
read the four documents listed in "Files to read first" below.

## Current branch / working tree

Branch: `main`. Working tree has **no modified tracked files** — `KafkaConsumerConfig.java` is
byte-identical to HEAD (Optimization 1's concurrency change was fully reverted, verified via
`git diff`). The only pending changes are new untracked files: four `docs/performance/*.md` reports
from this investigation, plus raw benchmark result files under `benchmarks/results/` (JSON/CSV, not
yet committed). Nothing needs to be reverted — just read the docs and continue.

## Optimization status

**Optimization 1 (Kafka listener concurrency 1→3) is NOT currently applied.** The code is back to
the original baseline (concurrency=1). Do not re-apply `.setConcurrency(3)` to
`kafkaListenerContainerFactory` in `KafkaConsumerConfig.java` until instructed — the investigation
below is still open.

## Benchmark status

The Milestone N baseline (`docs/performance/benchmark-results.md`) is trusted and committed. Test B
(backend golden path) baseline: 300/500 RPS clean (p50≈7-8ms, p99≈9-10ms), 600/700 RPS unsustainable
(unbounded lag growth). That baseline is the ground truth every comparison below is measured
against.

## Known findings (read in this order — each one supersedes/refines the last)

1. **Optimization 1 was implemented and benchmarked** (concurrency 1→3). Consumer lag at 500 RPS
   improved dramatically (208→12 max), exactly as hypothesized — but HTTP-layer latency regressed
   severely (p99 rose from ~9ms to 10+ seconds). Per the "stop and diagnose, don't chain changes"
   rule, this triggered a full root-cause investigation instead of a second optimization.
2. **Root-cause analysis disproved the first hypothesis.** "Kafka broker CPU saturation" was ruled
   out with direct evidence: in-container per-thread CPU accounting showed the broker's own threads
   did ~30ms of work across a 51-second window that produced 29-second latencies; GC pauses never
   exceeded 28ms; cgroup CPU throttling never engaged. The real correlate found was a large,
   unexplained host-wide CPU spike (+44 points of busy time within 6 seconds of load starting) not
   attributable to any single measured component.
3. **Concurrency was reverted to exactly 1 (the baseline value) and Test B was re-run with the
   original methodology.** The baseline did **not** reproduce — 300 RPS showed p50=1,401ms/p99=11.8s
   vs. the committed baseline's 7.82ms/10.06ms, on identical code and config. **This proves the
   regression was never about Kafka consumer concurrency — it's environmental**, not a consequence
   of the optimization.
4. **A direct host memory diagnostic found the likely environmental cause**: ~63MB free RAM (of
   18GB), 8.21GB held by the memory compressor, swap at 86.8% (10.67GB/12GB), kernel pressure level
   2 (WARNING). None of the TraceMind processes were responsible (combined Java+Python footprint
   ~106MB). The largest structural cause identified: Docker Desktop's VM has a fixed 7.75GB memory
   allocation against only ~1.56GB of actual container usage — ~6.2GB reserved but idle.
5. **A reboot was performed specifically to reset this memory/swap state.** You are resuming
   immediately after that reboot.

## Files to read first, in this order

1. `docs/performance/benchmark-results.md` — the original, trusted Test B baseline (search for
   "## Test B — Backend Golden Path").
2. `docs/performance/optimization-1-results.md` — what Optimization 1 changed and the regression it
   produced.
3. `docs/performance/optimization-1-root-cause-analysis.md` — the diagnostic evidence that
   disproved the broker-CPU-saturation hypothesis.
4. `docs/performance/environment-verification.md` — the experiment proving the regression reproduces
   even at concurrency=1 (i.e., it isn't Optimization 1's fault).
5. `docs/performance/optimization-session-handoff.md` — the detailed pre-reboot handoff (this file
   is a condensed version of it; read the handoff doc for full command reference and rationale if
   anything here is unclear).

## Immediate next task

**Do not touch code yet.** In order:

1. Verify host memory health first: `vm_stat`, `sysctl vm.swapusage`,
   `sysctl kern.memorystatus_vm_pressure_level`. Confirm free RAM is no longer near-zero and swap
   usage has dropped substantially from 86.8%. If it hasn't improved, stop and flag this before
   doing anything else — the reboot didn't fix the underlying problem.
2. Bring the stack up: check `docker ps` for the 4 containers
   (`tracemind-kafka-1`, `tracemind-postgres-1`, `tracemind-loki-1`, `tracemind-prometheus-1`); run
   `docker compose up -d` from the repo root if they aren't already up. Confirm all report
   `healthy`.
3. Start the three application services (exact commands are in
   `optimization-session-handoff.md` §9 — incident-service and connector-service via
   `mvn spring-boot:run`, investigation-service via uvicorn with `AI_TEST_DOUBLE=true` — **never**
   restart investigation-service with a real `OPENAI_API_KEY` without checking consumer lag first,
   per an earlier incident in this project). Confirm incident-service's startup log shows all 3
   `signals.received.v1` partitions assigned to a single consumer (concurrency is still 1).
4. Re-run Test B (300 and 500 RPS tiers at minimum) using the same methodology as
   `environment-verification.md` (`load_connector.py` + `downstream_sink.py` +
   `monitor_resources.py` + `monitor_kafka_lag.py`, fresh service-prefix per run) and compare
   against the committed baseline in `benchmark-results.md`.
5. Branch on the result:
   - **Baseline returns** (p50 back to ~7-8ms, p99 back to ~9-10ms) → confirms the earlier
     regression was environment drift, not code. Optimization 1 (concurrency=3) can then be
     re-benchmarked from this clean baseline, on its own merits, following the same
     hypothesize→implement→benchmark→compare→analyze discipline as before.
   - **Baseline still doesn't return** → do not re-attempt Optimization 1. Repeat the host memory
     diagnostic from this session (top memory/CPU consumers, Docker Desktop VM allocation vs.
     actual container usage, orphaned processes) and report findings before any further
     optimization work.

Follow the project's standing rules throughout: one optimization at a time, explain the hypothesis
before implementing, never chain multiple changes, and use the learning-first
Symptom/Hypothesis/Diagnostic/Root-cause/Fix/Re-verification format for any unexpected observation.
