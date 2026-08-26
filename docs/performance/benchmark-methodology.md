# TraceMind Benchmark Methodology (Milestone N)

This document formalizes the benchmark plan approved before any load was run. It adds no product
features — its purpose is a reproducible performance baseline, bottleneck identification, and
defensible metrics. **Do not optimize based on this document without first re-reading the
Bottleneck Analysis in `benchmark-results.md`** — per the milestone's own rule, nothing here is
license to tune the architecture before a baseline exists.

## 1. Layers benchmarked independently

| Layer | What it isolates | Test |
|---|---|---|
| Connector HTTP boundary | Tomcat + Spring MVC + Kafka producer send, no downstream dependency | A |
| Backend golden path | Kafka → Incident Service consumer → JPA/Postgres → transactional outbox → `investigation.requested.v1` publish | B |
| Duplicate/idempotency handling | The `event_id` UNIQUE constraint + correlation query under adversarial repeat traffic | C |
| Investigation lifecycle state machine | RUNNING-invariant, `signalVersion`, `needsReinvestigation`, staleness — under signal-storm volume, AI mocked | D |
| Outbox publisher | The `@Scheduled` poll-claim-publish loop alone | E |
| Real AI investigation | Genuine Prometheus/Loki/dependency-graph/OpenAI latency, small sample | F |
| Failure modes | Each external dependency failing individually | G |
| Restart/backlog recovery | Consumer-lag drain after an outage | H |

Layers are benchmarked separately because three real external dependencies are chained (Postgres,
Kafka, OpenAI) — an end-to-end-only number can't say *which one* is the bottleneck.

## 2. Metrics measured
Throughput (req/s, signals/s, incidents/s, outbox publish rate), latency (p50/p95/p99/max, both
load-generator-observed and internally-derived via timestamp deltas), correctness (duplicate
suppression rate, row counts, error rate), resources (CPU%/RSS per process, `docker stats` for
Postgres/Kafka), Kafka consumer lag per topic/partition/group, Postgres connection utilization.

## 3. Why the backend benchmark must not call real OpenAI per request
OpenAI costs money per call, has provider rate limits far below what Kafka/Postgres can sustain,
and has highly variable multi-second external latency. At 1000 RPS × 30s that's 30,000 paid calls —
mixing it in would benchmark OpenAI's rate limiter, not TraceMind's engineering.

## 4. AI-path benchmark, measured separately
Test F only: real orchestrator/collectors end-to-end, small controlled sample, explicit permission
required, run in isolation from backend load tests.

## 5. Sustainable vs. burst throughput
**Burst** = highest RPS accepted for a short window (~15–30s) before backlog builds — Kafka
absorbs it temporarily. **Sustainable** = the RPS at which, over a longer window (~2 minutes),
consumer lag stays flat/bounded rather than growing without limit.

## 6. Warm-up strategy
A short warm-up burst (~30–60s, discarded) precedes measured tests: JVM JIT, Hibernate
query-plan cache, Kafka connection/partition-assignment establishment, httpx connection pools.

## 7. Test duration
Burst tests ~15–30s per tier; sustained tests ~2 minutes at a candidate rate. Shorter than a real
production benchmark's 10+ minute windows — a stated limitation of running on a shared dev laptop,
not hidden.

## 8. Concurrency levels
Test A follows 100/250/500/1000/2000 RPS, continuing only while healthy. RPS is enforced via an
**open-loop, constant-arrival-rate** generator (scheduled send times), not closed-loop
worker-looping, so reported RPS is the actual offered load.

## 9. Dataset/cardinality strategy
A deterministic pool of generated service names (`service-0000`…`service-NNNN`) across 2
environments, rotated per request index — deterministic/reproducible, not random.

## 10. Avoiding one hot incident
Direct consequence of #9. **Test D is the deliberate exception** — it intentionally hammers one
incident to stress the coalescing invariant.

## 11. Load-generator latency vs. internal processing latency
The generator measures wall-clock time to HTTP 202 (Tomcat + Kafka producer send/ack) — not the
full async pipeline. Downstream latency (e.g. signal persistence) is derived by joining the
generator's locally-recorded `(eventId → send time)` against Postgres timestamps after the test,
never by having the generator block on downstream completion.

## 12. Kafka consumer lag measurement
Periodic `kafka-consumer-groups.sh --describe` snapshots per group, sampled into CSV during and
after each test.

## 13. CPU/memory measurement
`docker stats --no-stream` for containers; `ps -o %cpu,rss -p <pid>` for natively-run JVM/Python
processes. Note: `docker stats` invocation overhead (~2s per call) coarsens the effective sampling
interval below what's requested when containers are included — acceptable for trend-level CPU/mem
data, not a sub-second profiler.

## 14. Correctness validation under load
Post-test SQL assertions (row counts, zero duplicate rows given the UNIQUE constraint) plus a scan
of service logs for unexpected errors.

## 15. Files added
`benchmarks/scripts/load_connector.py` (open-loop HTTP load generator, multi-process — see the Test
A finding below for why), `monitor_resources.py`, `monitor_kafka_lag.py`. Machine-readable output
under `benchmarks/results/*.json`/`*.csv` (summarized, not raw per-request logs).

**Isolation strategy for AI**: Test B uses a downstream sink (stops consumption at
`investigation.requested.v1`, investigation-service not run at all). Test D needs
`InvestigationResultService`'s real result-consumption logic, so it uses an env-gated
(`AI_TEST_DOUBLE=true`) deterministic AI double swapped in at the exact point `main.py` constructs
`AIInvestigationService` — default unset everywhere else, so production behavior is unchanged
unless explicitly opted into. Test F uses the fully real path, small sample, explicit approval.

## Benchmark Environment Record

**Local benchmark on developer workstation.** These results do not represent cloud/production
hardware.

| | |
|---|---|
| OS | macOS 26.3.1, Darwin kernel 25.3.0, arm64 |
| CPU | Apple M3 Pro, 11 cores |
| RAM | 18 GB (host) |
| Docker Desktop VM allocation | 11 CPUs / 7.75 GiB RAM — containers run inside this budget |
| Java | OpenJDK 25.0.4 (Homebrew) |
| Python | 3.14.1 |
| Docker | 29.4.2 |
| Kafka | apache/kafka:3.8.0 (KRaft mode) |
| PostgreSQL | 16.15 (postgres:16-alpine, aarch64) |
| Kafka partitions | `signals.received.v1`: 3, `investigation.requested.v1`: 3, `investigation.results.v1`: 3, replication factor 1, single broker |
| Spring Kafka listener concurrency | default (1) — unchanged this milestone |
| HikariCP pool | default (max 10) — unchanged this milestone |
| Tomcat (connector-service) | default (max threads 200) — unchanged this milestone |
| Service instances | 1× each of connector/incident/investigation-service, no horizontal scaling |
| JVM flags | none beyond `mvn spring-boot:run` defaults |

## Methodology finding: load generator required a fix before Test A could proceed

**Symptom**: At offered 1000 RPS, the first-cut single-process generator achieved only ~237–366
RPS with p50 latency inflating to 1–2s (vs. single-digit ms at 500 RPS).

**Diagnosis**: Concurrent CPU sampling showed connector-service at only 3–12% CPU during this
"failure" — nowhere near saturated. Cross-checking with Apache Bench (a compiled tool) at 200
concurrency against the same endpoint achieved 18,786 req/s with 0 failures and p99=39ms.

**Root cause**: `load_connector.py`'s single Python asyncio process (GIL-bound JSON
serialization + per-request coroutine overhead) could not itself schedule/dispatch 1000+ req/s —
a benchmark-tooling limitation, not a TraceMind backend limitation.

**Classification**: Benchmark artifact.

**Fix**: Added multi-process support (`--workers N`) — partitions the deterministic request plan
across OS processes, merges raw per-request results before computing percentiles (never averages
pre-computed percentiles across workers).

**Re-verification**: With `--workers 6`, the same 1000 RPS offered rate achieved 985–991 RPS with
p50≈5–7ms, p99≈8.5ms, matching the healthy profile seen at lower tiers.
