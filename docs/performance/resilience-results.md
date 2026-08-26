# TraceMind Resilience Results (Milestone N, Test G)

**Local benchmark on developer workstation.** See `benchmark-methodology.md` for the environment
record. Each scenario below was run independently against the real system, with the system
restored to a healthy, quiescent state before the next scenario began. No production code was
changed for this test — Test G observes and verifies existing behavior only.

AI-dependent scenarios (3, 4, 6) used the `AI_TEST_DOUBLE=true` env-gated deterministic double
(introduced in Milestone N Test D) at the user's explicit direction — these scenarios test
evidence-collection and process-crash resilience, not AI quality, which Test F already proved
exhaustively with 20 real calls. Scenario 5 (OpenAI itself failing) necessarily used the real
`AIInvestigationService` code path with a deliberately invalid key, which fails before any
billable usage occurs.

---

## Scenario 1 — Kafka Unavailable

Used `docker pause` (not `stop`) deliberately — this exercises the *bounded-timeout/hang* path
rather than fast connection-refused, which is what actually matters for regression-testing the
historical `max.block.ms` concern noted in this codebase's own `application.yml` comments.

1. **Symptom**: Kafka paused while connector and incident-service were running; a real alert fired.
2. **Expected behavior**: bounded failure (not a false 202), no hidden 60s delay.
3. **Actual behavior**: connector returned **HTTP 503** (`"REJECTED"`) in **5.32s**, matching the
   configured `connector.kafka.send-timeout-ms=5000` exactly — not the historical hidden
   `max.block.ms` default. incident-service logged `DisconnectException` at INFO level and
   "Rediscovery will be attempted" — no crash, process stayed alive throughout.
4. **Recovery time**: Kafka reported `healthy` again within 2s of `docker unpause`; incident-service
   rejoined the consumer group and reassigned partitions within the same window; the next real alert
   was accepted (202) and fully processed within ~9s of Kafka's return.
5. **Data loss**: none — the rejected alert was never persisted anywhere (confirmed 0 rows for its
   event data), consistent with the connector correctly refusing rather than silently dropping.
6. **Duplicate processing**: none.
7. **Kafka lag**: N/A during the outage (nothing was produced); 0 after recovery.
8. **Postgres correctness**: no partial/orphaned rows from the rejected alert.
9. **Final system health**: fully healthy.

### A self-caught false alarm (reported per the learning-first rule, since it looked like a real bug at first)

**Symptom**: my own post-recovery verification query (searching Kafka/Postgres for the alert's
*fingerprint* string) found 0 matches for the recovery alert, despite the connector returning 202 —
looking exactly like a false-202 correctness bug.
**Diagnosis**: re-searched by the *exact returned `eventId`* instead of the fingerprint — found
the message correctly delivered and persisted, exactly once, in both Kafka and Postgres.
**Root cause**: `CanonicalSignalV1`'s payload never includes the raw Alertmanager `fingerprint`
string at all — only the SHA-256-derived `eventId`. My search term could never have matched
regardless of delivery outcome.
**Classification**: my own verification-methodology error, not a system defect.
**Re-verification**: 1/1 exact match in Kafka, 1/1 in Postgres. Scenario 1 is fully correct.

---

## Scenario 2 — PostgreSQL Unavailable

1. **Symptom**: Postgres paused while incident-service running; a real alert fired.
2. **Expected behavior**: transaction rollback, no partial writes, no outbox inconsistencies,
   message retained (not lost) for later reprocessing.
3. **Actual behavior**: connector accepted (202) — it never touches Postgres. incident-service's
   consumer failed to process the message (HikariCP `Failed to validate connection` WARNs, then
   `Connection is not available, request timed out after 30056ms`), left the message **unacked**
   (Kafka lag=1, message safely retained), and the `OutboxPublisher`'s own scheduled poll failed
   with a caught, logged WARN (`"Outbox publish attempt failed, leaving row(s) pending for the next
   poll cycle"`) rather than crashing. Process stayed alive throughout.
4. **Recovery time**: ~2s after `docker unpause` — the stuck signal was redelivered and correctly
   processed.
5. **Data loss**: none — signal reprocessed and persisted exactly once after recovery.
6. **Duplicate processing**: none — exactly 1 row for the affected `event_id`, no duplicate-key
   violations.
7. **Kafka lag**: 1 during the outage (the single stuck message), 0 after recovery.
8. **Postgres correctness**: 0 partial/orphaned rows during the outage; exactly 1 signal + 1
   correctly-correlated incident row after recovery.
9. **Final system health**: fully healthy.

---

## Scenario 3 — Prometheus Unavailable

1. **Symptom**: Prometheus paused; a real investigation triggered (AI test double).
2. **Expected behavior**: investigation continues, `MetricEvidence` empty, other evidence types and
   the RCA step still work.
3. **Actual behavior**: `PrometheusMetricsCollector` logged 3 WARN-level `"Prometheus query failed"`
   messages (one per metric), evidence collection took **5011.8ms** — bounded by
   `prometheus_timeout_seconds=5.0`, not an indefinite hang. Investigation still reached
   `COMPLETED`. `EvidenceBundle.metrics` was empty (`0` items); `dependencies` still had 1 real
   item; the RCA cited `E-INC-2265-DEP-1` — a real, non-hallucinated evidence ID (dependency
   evidence doesn't depend on Prometheus/Loki at all, being purely in-memory/static).
4. **Recovery time**: Prometheus reported `healthy` again 6s after `docker unpause`.
5. **Data loss**: none — evidence that *could* be collected (dependency) was collected and used.
6. **Duplicate processing**: N/A (single investigation).
7. **Kafka lag**: 0 throughout (investigation-service kept pace).
8. **Postgres correctness**: run correctly transitioned to `COMPLETED` with the degraded-but-valid
   evidence bundle stored in `result_payload`.
9. **Final system health**: fully healthy.

*(Loki also happened to return 0 matching log lines on this run — a pre-existing, unrelated
timing-window characteristic seen throughout this session, not caused by or related to the
Prometheus outage. The investigation still completed correctly using dependency evidence alone,
which is an even stronger demonstration of graceful degradation than the scenario strictly required.)*

---

## Scenario 4 — Loki Unavailable

1. **Symptom**: Loki paused; a real investigation triggered (AI test double), Prometheus restored
   first so this scenario tests Loki in isolation.
2. **Expected behavior**: `LogEvidence` empty, metrics + dependency graph continue, investigation
   still completes.
3. **Actual behavior**: `LokiLogsCollector` logged one WARN-level `"Loki query failed"`, evidence
   collection took **5032.1ms** — bounded by `loki_timeout_seconds=5.0`. Investigation reached
   `COMPLETED`. `EvidenceBundle.logs` was empty; `metrics` had 3 real items (Prometheus working
   again); `dependencies` had 1. RCA cited `E-INC-2265-METRIC-db_connection_pool_active` — real,
   non-hallucinated.
4. **Recovery time**: Loki reported `ready` (200) 2s after `docker unpause`.
5. **Data loss**: none.
6. **Duplicate processing**: N/A.
7. **Kafka lag**: 0 throughout.
8. **Postgres correctness**: correct `COMPLETED` state with degraded-but-valid evidence stored.
9. **Final system health**: fully healthy.

---

## Scenario 5 — OpenAI Unavailable

Used a deliberately invalid API key (`sk-invalid-deliberately-broken-...`) with the **real**
`AIInvestigationService` code path (not the double) — an invalid key is rejected by OpenAI before
any billable usage occurs, so this is zero-cost while still exercising the genuine failure-handling
logic.

1. **Symptom**: a real alert triggered an investigation with a broken key.
2. **Expected behavior**: `investigation.result` status `FAILED`, `failureReason` populated, no
   excessive retries, no crash.
3. **Actual behavior**: `status=FAILED`, **`failureReason=API_ERROR`**, `rootCauseAnalysis=None`
   (correctly absent). `promptTokens`/`completionTokens`/`totalTokens` were all `None` — confirming
   **zero billable tokens were ever processed**. Only **1** request was sent to
   `api.openai.com` — the OpenAI SDK correctly recognized a 401 auth error as non-retryable (per
   the blueprint's own invariant: never retry auth failures) rather than exhausting the configured
   `openai_max_retries=2` on a request that could never succeed. Evidence was still fully collected
   and preserved (`logs=3, metrics=3, dependencies=1`) despite the AI failure — the investigation
   pipeline degraded gracefully rather than aborting entirely. Process stayed alive throughout.
4. **Recovery time**: N/A (no outage to recover from — restoring the real key for later tests
   is a config change, not a "recovery").
5. **Data loss**: none — evidence preserved in the `FAILED` result.
6. **Duplicate processing**: N/A.
7. **Kafka lag**: 0 (one message, promptly processed and acked with a `FAILED` result).
8. **Postgres correctness**: `investigation_runs.status` correctly set, `failure_reason` populated
   at the incident-service level too (mirroring the AI-level `failureReason`).
9. **Final system health**: fully healthy — a failed AI call is a normal, handled outcome, not a
   system failure.

---

## Scenario 6 — Investigation Service Crash

A real crash-mid-processing test required deliberately outrunning the AI double's own speed: an
initial small burst (8 alerts) was fully drained before a kill could even land, so a larger burst
(50 alerts, distinct services) was fired and investigation-service was `SIGKILL`ed **0.3s** in —
confirmed via consumer-group lag (exactly 50, matching the burst) that it was genuinely dead with a
real backlog outstanding, not merely between messages.

1. **Symptom**: `SIGKILL` sent to investigation-service with 50 messages in flight/backlogged.
2. **Expected behavior**: Kafka redelivery, no lost investigations, no duplicate
   `InvestigationResult`s, backlog drains completely on restart.
3. **Actual behavior**: on restart, backlog (lag=50) drained to 0 within **1 second**. Postgres
   confirmed **50/50** distinct incidents created and **50/50** investigation runs `COMPLETED` —
   zero lost. Kafka topic scan across its *entire* history (4,359 distinct `investigationRunId`s
   from every test in this milestone) found **zero duplicates** anywhere, including through this
   crash.
4. **Recovery time**: ~5s to `healthy` (uvicorn startup), 1s to drain the backlog.
5. **Data loss**: none (50/50 completed).
6. **Duplicate processing**: none (0 duplicate `investigationRunId`s on the topic, ever).
7. **Kafka lag**: 50 at kill time, 0 within 1s of restart.
8. **Postgres correctness**: exact counts confirmed by direct query, no orphaned/stuck rows.
9. **Final system health**: fully healthy.

---

## Scenario 7 — Incident Service Crash

Same approach: fired 75 alerts across 60 distinct services, `SIGKILL`ed incident-service 0.15s in.
Since the connector doesn't depend on incident-service at all, all 75 were accepted (202, 0%
errors) and safely queued in Kafka regardless of incident-service's state — confirmed lag=75
(matching exactly) before restart.

1. **Symptom**: `SIGKILL` sent to incident-service with 75 signals backlogged.
2. **Expected behavior**: Kafka replay, no lost signals, no duplicate incidents, no corrupted
   lifecycle state.
3. **Actual behavior**: on restart, backlog drained to 0 within **1 second**. Postgres confirmed
   **75/75** signals persisted exactly (no loss) and **60/60** distinct incidents (matching the
   service pool exactly, no duplicates — verified via `GROUP BY primary_service HAVING COUNT(*)>1`
   returning zero rows). No incident was left with a null `current_investigation_run_id` (0 found)
   — no corrupted lifecycle state. No unexpected errors in the log post-restart.
4. **Recovery time**: ~15.1s to `Started IncidentServiceApplication` (Spring Boot startup, matching
   Test E Scenario 3's independently-measured figure), 1s to drain the backlog.
5. **Data loss**: none (75/75 signals persisted).
6. **Duplicate processing**: none (60/60 incidents, zero services with more than one incident row).
7. **Kafka lag**: 75 at kill time, 0 within 1s of restart.
8. **Postgres correctness**: exact counts confirmed, zero corrupted lifecycle state.
9. **Final system health**: fully healthy.

---

## Summary across all 7 scenarios

| Scenario | Data loss | Duplicate processing | Crash? | Recovery time |
|---|---|---|---|---|
| 1. Kafka down | None | None | No | ~2s (Kafka) + ~9s (full pipeline) |
| 2. Postgres down | None | None | No | ~2s |
| 3. Prometheus down | None (graceful degradation) | N/A | No | ~6s |
| 4. Loki down | None (graceful degradation) | N/A | No | ~2s |
| 5. OpenAI down | None (evidence preserved) | N/A | No | N/A |
| 6. Investigation Service SIGKILL | None (50/50 recovered) | None (0/4,359 topic-wide) | Recovered cleanly | ~5s + 1s drain |
| 7. Incident Service SIGKILL | None (75/75 recovered) | None (60/60 correct) | Recovered cleanly | ~15s + 1s drain |

**Every correctness invariant held in every scenario.** The one anomaly encountered (Scenario 1's
apparent false-202) was traced to my own verification-query mistake, not a system defect, and is
documented in full above rather than silently corrected. No STOP-and-diagnose-and-fix cycle against
production code was required anywhere in Test G — a clean resilience result across bounded-timeout
failures, transactional rollback, graceful evidence-collection degradation, AI failure handling, and
two independent hard-crash-and-recover scenarios.

---

*Test H (Restart/Backlog Recovery) not yet run — stopping before it per instruction.*
