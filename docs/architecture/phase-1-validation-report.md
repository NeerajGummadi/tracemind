# Phase 1 Validation Report

> **Scope:** System validation and failure injection for the reactive golden path
> (Alertmanager → Connector Service → Kafka → Incident Service → PostgreSQL →
> Outbox Publisher → Kafka). No new features. Executed against real
> docker-compose infrastructure (PostgreSQL, Kafka, both services running as
> actual processes) — not just the existing unit/integration test suites,
> which validate components in isolation and already pass (8 tests in
> connector-service, 12 in incident-service).

---

## 1. Happy Path

**Action:** one `POST /integrations/prometheus/alerts` for `payment-service`/`prod`/`CRITICAL`.

**Result:** created `INC-1`.

| Check | Result |
|---|---|
| Signals for this incident | 1 |
| Incidents created | 1 |
| Outbox rows for this incident | 1, status `PUBLISHED` |
| Messages on `investigation.requested.v1` keyed `INC-1` | 1 |

```
INC-1 | {"eventId": "evt-26a08438-...", "incidentId": "INC-1", "primaryService": "payment-service",
         "triggerSignalIds": ["evt-b7cf7d28d1308fe6"], ...}
```

## 2. Duplicate Delivery

**Action:** the identical Alertmanager payload from Scenario 1 (same `fingerprint` + `startsAt`) sent a second time.

**Result:** Connector generated the same `eventId` (`evt-b7cf7d28d1308fe6`) both times, confirming deterministic `eventId` derivation. Incident Service logged:

```
Skipping already-processed signal evt-b7cf7d28d1308fe6
```

| Check | Before | After duplicate |
|---|---|---|
| Signals with this event_id | 1 | 1 (unchanged) |
| Incidents (`INC-1`) | 1 | 1 (unchanged) |
| Outbox rows for `INC-1` | 1 | 1 (unchanged) |

## 3. Correlation

**Action:** two distinct alerts (different `fingerprint`s), same service (`fraud-service`) and environment (`prod`), 2 minutes apart (within the 5-minute window).

**Result:** both correlated into a single incident, `INC-2`.

| Check | Result |
|---|---|
| Signals | 2 |
| Incidents | 1 |
| `incident_signals` rows for `INC-2` | 2 |
| Outbox rows for `INC-2` | **1**, not 2 — confirms the outbox row is written only on new-incident creation, never on correlation into an existing one |

## 4. New Incident (different service)

**Action:** one more alert, same environment (`prod`), different service (`checkout-service`).

**Result:** a third, distinct incident, `INC-3` — not merged into `INC-1` or `INC-2`.

```
 incident_number | primary_service
------------------+------------------
 INC-1            | payment-service
 INC-2            | fraud-service
 INC-3            | checkout-service
```

## 5. Severity Escalation

**Action:** four alerts for the same service (`billing-service`), one minute apart, in order `WARNING → HIGH → CRITICAL → WARNING`.

**Result:** all four correlated into one incident (`INC-4`, 4 `incident_signals` rows). Severity after each step:

| Alert severity | Incident severity after |
|---|---|
| WARNING | WARNING |
| HIGH | HIGH |
| CRITICAL | CRITICAL |
| WARNING | **CRITICAL** (unchanged — proves severity never decreases) |

## 6. Outbox Retry (genuine Kafka outage, not a mocked failure)

**Action:** `docker compose stop kafka` (the real container, not a test double), then a `PENDING` outbox row inserted directly into Postgres while Kafka was down.

**First run surfaced a real bug — see the "Bug found and fixed" section below.** After the fix:

| Time | Event |
|---|---|
| T+0s | Row inserted, Kafka down |
| T+5s | First publish attempt fails and is logged (bounded by `send-timeout-ms=5000`, as configured) |
| T+11s | Second attempt fails |
| — | Row confirmed still `PENDING`, `published_at` still null |
| — | `docker compose start kafka` |
| T+~5s after restart | Row confirmed `PUBLISHED`, `published_at` set |
| — | Exactly **1** message on `investigation.requested.v1` for this row — no duplicate spam from the failed attempts |

## Bug found and fixed during Scenario 6

**Symptom:** on the first run of Scenario 6 (before the fix below), the row's first failure wasn't logged until ~40 seconds after Kafka went down, despite `send-timeout-ms` being configured to 5000ms and the poll interval being 1000ms — roughly 40 poll cycles' worth of missing retry attempts.

**Root cause:** `send-timeout-ms` only bounds `.get()` on the `CompletableFuture` that `KafkaTemplate.send()` returns. It does not bound `send()` itself, which can block synchronously while the producer fetches cluster metadata from an unreachable broker — governed by Kafka's own `max.block.ms`, which defaults to 60000ms. Neither `OutboxKafkaProducerConfig` (incident-service) nor `KafkaProducerConfig` (connector-service, same underlying issue) ever set it. Milestone C's and E's unit tests never caught this because those tests explicitly configured a short `max.block.ms` for test speed — masking the gap in the production configs they were modeled on.

**Fix:** `max.block.ms` set explicitly in both services' `application.yml`, matching each service's own configured send timeout (5000ms), so the *entire* publish attempt — metadata fetch included — is bounded consistently.

**Verification:** re-ran Scenario 6 from a clean state after the fix. First failure now logged at ~5s (not ~40s), row correctly stays `PENDING` throughout the outage, and correctly becomes `PUBLISHED` with exactly one Kafka message once Kafka is restored (table above). Full test suites re-run clean on both services after the fix (8/8 connector-service, 12/12 incident-service).

**Incidental observation, not a bug:** immediately after restarting Kafka, incident-service's consumer took ~44 seconds to complete its group rebalance (visible as `UNKNOWN_MEMBER_ID` → `NotCoordinatorException` → eventually `Successfully joined group`) before it resumed consuming `signals.received.v1`. An alert sent during that window sat safely on the Kafka topic and was processed correctly (as `INC-5`) once the rebalance completed — no message was lost, confirming at-least-once delivery holds across a full consumer-group disruption, not just a producer-side outage.

---

## 7. Crash Window Documentation

**Not simulated, per instruction — documented.**

```
Kafka publish succeeds
        ↓
process crashes
        ↓
status never updated (still PENDING)
        ↓
row is retried by the next poll cycle (this instance restarting, or another instance)
        ↓
duplicate publish to investigation.requested.v1
```

**Why this is acceptable:** `OutboxPublisher.claimAndPublishOne()` (blueprint §10, and Milestone E's implementation) commits the Kafka send and the `PUBLISHED` status update as two separate physical actions — a Kafka broker ack and a Postgres transaction commit — and no distributed transaction spans both. There is no way to make "Kafka has durably stored the message" and "Postgres has durably recorded that fact" atomic without two-phase commit, which the blueprint explicitly rules out (Invariant: "at-least-once delivery... idempotent consumers," not exactly-once). If the process dies in the gap between those two actions, the row is still `PENDING` from Postgres's point of view, so the *next* successful poll cycle — on this instance after restart, or on another instance entirely — will legitimately re-claim it (`FOR UPDATE SKIP LOCKED` only excludes rows currently locked by a live transaction; a crashed process holds no lock) and publish it again. The topic then has two messages for the same incident.

This is safe because everything downstream of this topic is required to be idempotent, by the same principle already applied to `signals.received.v1` (blueprint invariant 4, Milestone D's `event_id UNIQUE` mechanism). Nothing has been built yet that consumes `investigation.requested.v1` (Investigation Service is out of scope for Phase 1), but the contract is already explicit: `InvestigationRequestedV1.eventId` exists precisely so a future consumer can deduplicate the same way `signals.received.v1`'s consumer already does. Building the eliminate-this-window architecture now (outbox-and-Kafka in one transaction, which isn't possible; or a distributed transaction coordinator) would be exactly the kind of complexity blueprint's engineering philosophy rejects for a duplicate window that's already fully absorbed downstream.

## 8. Offset Recovery Documentation

**Not simulated, per instruction — documented.**

Three crash points for `SignalConsumerListener.onMessage()`, all already exercised for real in this session's Scenario 6 aftermath (the consumer's own group rebalance is a real instance of "the consumer stops holding its position and must resume correctly"):

**Before DB commit.** The consumer process (or its container) dies mid-transaction, inside `SignalIngestionService.ingest()`. Postgres itself rolls back the incomplete transaction on connection loss — nothing partial is ever visible. The Kafka offset was never acknowledged (manual ack mode, `ack.acknowledge()` is the last line of `onMessage()`, called only after `ingest()` returns). On restart, the consumer resumes from the last *committed* offset, so this message is redelivered from scratch. Redelivery is a normal, first-time-shaped `ingest()` call: the signal doesn't exist yet, so it inserts cleanly.

**After DB commit, before offset commit.** The transaction committed — `signals`, `incidents`, `incident_signals`, and (if applicable) `outbox_events` are durably written — but the process dies before `ack.acknowledge()` runs, or dies while Kafka is processing the ack. On restart, Kafka redelivers the same message (the offset was never advanced). This time `ingest()` hits the `event_id UNIQUE` constraint on the very first statement (`signalRepository.saveAndFlush(signal)`), throws `DataIntegrityViolationException`, which the listener maps to `DuplicateSignalException` and acks anyway. Zero duplicate business state — this is the exact mechanism Scenario 2 above just proved live.

**Before offset commit, in general** (the union of the above two, from Kafka's perspective — it cannot distinguish them, and doesn't need to): both are handled by the same idempotency check. There's no third distinct case; "offset never committed" always means "will be redelivered," and redelivery is always safe because `ingest()`'s behavior depends only on whether `event_id` already exists in Postgres, not on any assumption about why it's being redelivered.

**Why idempotency protects us in all cases:** the `event_id UNIQUE` constraint is the single source of truth for "has this signal been fully processed," and it's checked as literally the first write in the transaction that would do anything else. This means there is no window where a signal is "partially" processed and redelivery could corrupt state — either the constraint has committed (fully processed, redelivery is a safe no-op) or it hasn't (not processed, redelivery is a normal first attempt). At-least-once delivery plus this single idempotency check is sufficient; nothing about offset timing needs to be exact.

---

*See `docs/diagrams/phase-1-reactive-flow.md` for the sequence diagram and
`docs/architecture/phase-1-lessons-learned.md` for the engineering retrospective.*
