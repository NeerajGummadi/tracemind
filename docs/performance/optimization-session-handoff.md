# Optimization Session Handoff (pre-reboot)

Written immediately before a planned machine reboot, so work can resume without re-deriving
context. No code was changed to produce this document.

---

## 1. Current project state

TraceMind is past the pre-code scaffolding stage described in the top-level `CLAUDE.md` — three
services are implemented and runnable (`connector-service`, `incident-service` in Java/Spring Boot;
`investigation-service` in Python/FastAPI), backed by Kafka, PostgreSQL, Loki, and Prometheus via
`docker-compose.yml`. The project is currently in a **Performance Optimization phase** (post
Milestone N), working through Kafka consumer concurrency as the first candidate optimization —
paused mid-investigation for the reason in §5.

## 2. Completed milestones

- **K** — Real Loki log integration.
- **L** — Static dependency graph integration (trimmed to only genuinely deployed entities per
  explicit instruction — `infrastructure/topology/service-dependencies.yml` should only ever
  contain `payment-service → payment-service-db`).
- **M** — Investigation lifecycle, versioning, and reinvestigation (coalescing, `@Version`
  optimistic locking on `Incident`, migration V8).
- **N** — End-to-end performance/stress/resilience benchmarking, Tests A–H, fully complete with a
  committed baseline. One genuine production bug found and fixed during N (the optimistic-locking
  race in Test D). Zero data loss/duplication across all Test G/H failure-injection scenarios.

## 3. Benchmark status

**Baseline (Milestone N, concurrency=1) is committed and trusted**:
- `docs/performance/benchmark-methodology.md` — environment record + methodology.
- `docs/performance/benchmark-results.md` — Tests A–F results + consolidated A–H summary. Test B
  (backend golden path) baseline numbers, the ones every later comparison is measured against:

  | RPS | Achieved | p50 | p99 | Max lag | Sustainable? |
  |---|---|---|---|---|---|
  | 300 | 299.38 | 7.82ms | 10.06ms | 27 | Yes |
  | 500 | 498.87 | 7.02ms | 8.71ms | 208 | Yes |
  | 600 | 598.67 | 7.07ms | 10.10ms | 13,461 (still climbing) | No |
  | 700 | 698.30 | 7.03ms | 9.36ms | 21,290 (still climbing) | No |

  Webhook-to-persistence latency (200 RPS/20s, joined against Postgres): p50=8.72ms, p95=10.55ms,
  p99=16.47ms, max=129.47ms.
- `docs/performance/resilience-results.md` (Test G, 7 scenarios) and
  `docs/performance/recovery-benchmark-report.md` (Test H, 4 scenarios) — both clean, zero data
  loss/duplication.

## 4. Current optimization status

**Optimization 1 (Kafka listener concurrency 1→3) is implemented in history but currently
REVERTED in the working tree.** `KafkaConsumerConfig.java`'s `kafkaListenerContainerFactory` bean
has no `.setConcurrency(...)` call right now — this is back to the original baseline behavior
(concurrency=1, verified via `kafka-consumer-groups.sh --describe`: a single consumer owns all 3
`signals.received.v1` partitions). 28/28 Java tests pass in this state.

**No optimization is currently applied to the codebase.** Do not re-apply concurrency=3 without
re-reading §8 first.

## 5. Why Optimization 1 was paused

Raising concurrency to 3 (matching the partition count) dramatically fixed the *diagnosed* metric —
consumer lag at 500 RPS dropped from a max of 208 to 12 — exactly as hypothesized. But it also
produced a severe, reproducible regression in end-to-end HTTP latency at the same tier: p50 rose
from 7ms to 400ms+, p99 from 9ms to 10+ seconds, max to 27+ seconds, with achieved throughput
falling below the offered rate. Per the mandated one-change-at-a-time workflow, this triggered a
full stop-and-diagnose instead of proceeding to Optimization 2.

Full writeup: `docs/performance/optimization-1-results.md`.

## 6. Current hypothesis

A deep root-cause investigation (`docs/performance/optimization-1-root-cause-analysis.md`)
**disproved** the first-pass explanation (Kafka broker JVM CPU saturation) with direct evidence:
in-container per-thread CPU accounting showed the broker's own threads did ~30ms of work across a
51-second window that produced 29-second latencies; GC pauses never exceeded 28ms; cgroup CPU
throttling never engaged (unlimited quota). Instead, the evidence pointed at **host-wide CPU
contention** (+44 percentage points of busy time within 6 seconds of load starting — several
CPU-cores' worth of work appearing from nowhere measurable in any single component) as the real
correlate.

**That hypothesis was then tested directly**: concurrency was reverted to exactly 1 (the original
baseline value, no other change) and Test B was re-run with the original methodology
(`docs/performance/environment-verification.md`). **The baseline did NOT reproduce** — 300 RPS
showed p50=1,401ms/p99=11.8s (vs. baseline's 7.82ms/10.06ms) on *identical* code and config. This
proves the regression was never about Kafka consumer concurrency at all — it is environmental.

**Current, most specific hypothesis**: the host is memory-starved (see §7), and that starvation —
not the concurrency change — is what produced both the Optimization 1 regression and the failed
baseline-reproduction attempt. Webhook-to-durable-persistence latency (send-time to Postgres
row-creation, independent of the HTTP response returning to the client) measured clean and
baseline-matching (p50=7.0ms) even during the "regressed" re-run — the actual signal-processing
pipeline is fine; the delay is specifically in the client↔connector round trip, consistent with a
starved host stalling process scheduling somewhere in that path.

## 7. Current machine/environment issue (why the reboot is happening)

A direct host memory diagnostic (chat-only, not yet a committed file) found:

| Metric | Value |
|---|---|
| Free RAM | ~63 MB (of 18 GB total) |
| RAM held by the memory compressor | 8.21 GB (45% of total RAM) |
| Swap used | 10.67 GB / 12 GB (86.8%) |
| Kernel memory-pressure level | 2 = WARNING |

None of the TraceMind processes (Java/Python, combined ~106 MB) are responsible. The largest
identified structural cause: **Docker Desktop's Linux VM has a fixed 7.75 GB memory allocation**,
of which the 4 running containers actually use only ~1.56 GB combined — roughly 6.2 GB reserved but
idle. The rest is ordinary long-uptime desktop-app accumulation (IDE, browser, chat apps). One
orphaned, harmless leftover process was found: `demo_metrics_exporter.py` (PID 31557, port 9105,
running ~4.5 days, 11 MB) — will die naturally on reboot, no action needed.

**The reboot is expected to reset swap/compressor state and free the memory currently pinned by
long-running processes**, giving a clean slate to re-test whether the original baseline is
reproducible in a healthy environment.

## 8. Exactly what the next step should be after reboot

1. **Verify memory health first, before touching the app stack.** Run `vm_stat`,
   `sysctl vm.swapusage`, `sysctl kern.memorystatus_vm_pressure_level`. Confirm free RAM is no
   longer near-zero and swap usage has dropped substantially from 86.8%. If it hasn't, the drift is
   not (only) accumulated process state — stop and investigate further before benchmarking again.
2. **Bring the stack up**: `docker compose up -d` from the repo root if containers aren't already
   running (`docker ps` to check first — they may auto-start with Docker Desktop). Confirm all 4
   containers report `healthy`.
3. **Start the three application services** (see §9 for exact commands). Confirm clean startup:
   incident-service log should show `partitions assigned: [signals.received.v1-0,
   signals.received.v1-1, signals.received.v1-2]` on a **single** consumer (concurrency is still
   reverted to 1 — correct, do not change it yet).
4. **Re-run Test B exactly as in `environment-verification.md`** (300 and 500 RPS tiers at minimum,
   same load_connector.py + downstream_sink.py + monitor_resources.py + monitor_kafka_lag.py
   methodology, fresh service-prefix per run) and compare against the original committed baseline
   in §3.
5. **Decision point**:
   - If baseline numbers return (p50 back to ~7ms, p99 back to ~10ms at 300/500 RPS) →
     confirms the regression was environment drift, not Optimization 1. Optimization 1
     (concurrency=3) can then be re-benchmarked on this clean baseline on its own merits.
   - If baseline still does not return → the drift is not resolved by a reboot alone; do not
     re-attempt Optimization 1 yet. Escalate to a fresh diagnostic pass (repeat the host memory
     inspection from this session) before any further optimization work.
6. Only after a clean, reproducible baseline is confirmed should Optimization 1 or any other
   optimization be reconsidered — per the standing rule, one change at a time, hypothesis → change
   → benchmark → compare → analyze before the next step.

## 9. Important commands and files to revisit

**Service startup** (each in its own terminal/background process):
```
# incident-service (port 8082, Kafka-consumer-only, no embedded web server)
cd services/incident-service && mvn spring-boot:run

# connector-service (port 8081, HTTP webhook entrypoint)
cd services/connector-service && mvn spring-boot:run

# investigation-service (port 8083, MUST use AI_TEST_DOUBLE=true for benchmarking —
# never restart it with a real OPENAI_API_KEY without first checking consumer lag,
# per the Test F incident earlier in this project)
cd services/investigation-service && AI_TEST_DOUBLE=true .venv/bin/python3 -m uvicorn investigation_service.main:app --port 8083 --host 0.0.0.0
```

**Verify concurrency is still reverted**:
```
sed -n '44,59p' services/incident-service/src/main/java/com/tracemind/incident/kafka/KafkaConsumerConfig.java
# kafkaListenerContainerFactory bean should have NO .setConcurrency(...) call
```

**Consumer group / lag check**:
```
docker exec tracemind-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group incident-service
```

**Benchmark tooling** (all in `benchmarks/scripts/`, run with the venv interpreter at
`services/investigation-service/.venv/bin/python3`):
`load_connector.py`, `downstream_sink.py`, `monitor_resources.py`, `monitor_kafka_lag.py`,
`measure_persistence_latency.py`.

**Key documents, in reading order for a fast re-orientation**:
1. `docs/performance/benchmark-results.md` — the original, trusted baseline.
2. `docs/performance/optimization-1-results.md` — what was tried and the regression it produced.
3. `docs/performance/optimization-1-root-cause-analysis.md` — deep diagnostic evidence, disproves
   the broker-CPU hypothesis.
4. `docs/performance/environment-verification.md` — proves the regression reproduces even at
   concurrency=1, i.e., it isn't Optimization 1's fault.
5. This file — what to do next.

**Standing project rules still in force**: one optimization at a time, hypothesis → implement →
benchmark → compare → analyze before proposing the next change; never restart investigation-service
with a real API key without checking consumer lag first; keep
`infrastructure/topology/service-dependencies.yml` limited to genuinely deployed entities only.
