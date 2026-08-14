# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

TraceMind is in the pre-code scaffolding stage. The repository currently contains only documentation
(`README.md`, `docs/architecture/engineering-blueprint.md`) and empty placeholder directories
(`services/connector-service`, `services/incident-service`, `shared/contracts`,
`infrastructure/kafka`, `infrastructure/postgres`, `docs/adrs`, `docs/api`, `docs/diagrams`, `scripts`).
No build tooling, dependency manifests, or source files exist yet. There are no commands to build, lint,
or test — that will change as services are scaffolded, at which point this file should be updated with
the actual commands.

Repository structure and implementation must remain consistent with
`docs/architecture/engineering-blueprint.md`.

## What TraceMind is

An AI-native incident investigation platform: given a production alert, it collects telemetry evidence,
correlates it across services, and produces an evidence-backed root cause analysis (RCA). It does not
detect incidents (upstream monitoring tools like Prometheus Alertmanager/Datadog/CloudWatch do that) — it
investigates *why* an already-detected incident happened. See `README.md` for the full problem statement
and `docs/architecture/engineering-blueprint.md` for the complete system design (this is the primary
architectural reference — read it before making non-trivial changes).

`docs/architecture/engineering-blueprint.md` is the authoritative architectural specification. If
repository code, README.md, or this file ever conflicts with the blueprint, follow the blueprint and ask
for clarification rather than making assumptions.

## Engineering philosophy

- Solve one problem exceptionally well; introduce a new technology only when it solves a real production
  problem, not speculatively.
- Prefer simple, explainable designs over clever abstractions — every architectural decision must be
  explainable, not just functional.
- Build the smallest system that demonstrates production-quality engineering, not the largest distributed
  system possible.
- Every important design decision should be documented (see `docs/adrs/`).

## Workflow

- Before writing code for any non-trivial feature, explain the implementation plan first (what changes,
  which files/components, how it fits the existing architecture) and wait for approval if it implies any
  architecture change.
- Do not make architectural assumptions. If the blueprint doesn't specify something (a new service, a new
  datastore, a new framework, a schema change), ask rather than inventing it — see the guardrails below.
- Implement in small, reviewable increments rather than large sweeping changes.
- After implementing, explain the trade-offs made.
- - Never modify more than one architectural milestone in a single implementation step unless explicitly requested.

## Architectural invariants (do not violate)

These are declared as frozen in the engineering blueprint (Section 43) and repeated as explicit
coding-agent guardrails (Section 44). Do not introduce additional services, databases, brokers, or
frameworks without approval, and specifically:

- **PostgreSQL is the sole authoritative source of business state.** Kafka is transport, not truth;
  Redis is never required for durable correctness (cache/locks only — losing Redis must degrade
  efficiency, not correctness).
- **Transactional outbox, not dual writes.** Never do `save to Postgres` then `Kafka.send(...)` as two
  separate steps. Always write the domain change and the outbox event in the same DB transaction; a
  separate outbox publisher delivers to Kafka.
- **At-least-once delivery everywhere.** All Kafka consumers must be idempotent; durable deduplication
  is enforced via Postgres constraints (e.g. `event_id UNIQUE`), never via Kafka semantics alone.
- **Vendor-specific parsing terminates at the connector boundary.** Every alert source (Prometheus,
  Datadog, CloudWatch, ...) is normalized into `CanonicalSignalV1` by the Connector Service. No
  downstream component may contain vendor-specific parsing logic.
- **Raw telemetry never goes directly into the LLM.** Deterministic evidence construction (the Evidence
  Correlation Engine building an Evidence Graph) always happens before any AI/probabilistic reasoning.
- **AI explains evidence, it does not own truth.** Every substantive hypothesis/claim must cite
  `evidenceIds[]`; no evidence means no high-confidence claim. Keep `OCCURRED_BEFORE` (observed) and
  `CAUSED_BY` (hypothesized) distinct — deterministic code must never claim causality, only correlation.
- **Logical subsystem boundaries do not automatically justify new microservices.** E.g. the Evidence
  Correlation Engine stays inside the Investigation Service until there's a concrete reason (independent
  scaling, heavy CPU, separate ownership) to extract it.
- **Every network call has a bounded timeout; retries are limited to transient failures** (timeouts, 429,
  transient 5xx/Kafka unavailability) with exponential backoff + jitter — never retry malformed
  payloads, schema violations, or auth failures.
- **Partial failure degrades gracefully rather than failing the whole investigation.** E.g. missing logs
  or an unreachable runbook store lowers `evidenceCompleteness`/confidence instead of aborting; a Slack
  outage must never invalidate a completed investigation.

## System architecture

TraceMind follows a single reactive incident investigation architecture (no separate prediction path):

- **Reactive Investigation Path** (the whole product): webhook → Connector Service normalizes to
  `CanonicalSignalV1` → Kafka (`signals.received.v1`) → Incident Service (idempotency check, incident
  correlation, persist, transactional outbox) → Kafka (`investigation.requested.v1`) → Investigation
  Service (collects evidence from Prometheus/Elasticsearch/dependency graph/pgvector concurrently →
  deterministic Evidence Correlation → Evidence Graph → AI hypothesis generation → AI critic/verifier →
  RCA composer + schema validation) → Incident Service persists result → Notification Worker (Slack,
  fire-and-forget) + Dashboard.

Planned services (per blueprint Section 41 — only two directories exist so far as empty placeholders):

| Service | Tech | Owns |
|---|---|---|
| `connector-service` | Spring Boot | Webhook ingestion, payload validation, normalization to `CanonicalSignalV1`, publish to Kafka. Never touches Postgres/Elasticsearch, never runs AI or correlation logic. Returns `202 Accepted`. |
| `incident-service` | Spring Boot | Consumes canonical signals, idempotency, incident correlation (deterministic, see below), lifecycle management, transactional outbox, incident APIs, persists RCA results. |
| `investigation-service` (not yet scaffolded) | FastAPI / Python | Evidence collection (Prometheus, Elasticsearch, dependency graph, pgvector runbooks), Evidence Graph construction, AI hypothesis → critic → RCA composition pipeline. |

Incident correlation (initial version, deterministic — no AI grouping): a signal attaches to an existing
incident if same org + same environment + same primary service + incident status != `COMPLETED` +
`lastObservedAt` within a 5-minute window; otherwise a new incident is created.

Incident lifecycle: `DETECTED → QUEUED → INVESTIGATING → COMPLETED`, with `INVESTIGATING → FAILED` as the
alternate exit. Transitions must go through explicit domain operations (`queueInvestigation()`,
`startInvestigation()`, `completeInvestigation()`, `failInvestigation()`), never ad hoc status writes.

Kafka partition keys matter for ordering: `signals.received.v1` is keyed by `environment:service` (signals
for the same service stay ordered before an incident ID exists); `investigation.requested.v1` is keyed by
`incidentId` (all workflow events for one incident land on the same partition).

AI Investigation Engine is intentionally three stages only — hypothesis generation → critic/falsification
→ RCA composition — resist adding more agent stages. RCA verification states are
`VERIFIED | PARTIALLY_VERIFIED | UNVERIFIED | INSUFFICIENT_EVIDENCE`; confidence is presented externally
as `HIGH | MEDIUM | LOW`, never as a raw LLM-derived probability.

Data model (Postgres, source of truth) starts with four tables: `signals`, `incidents`,
`incident_signals` (join table), `outbox_events`. See blueprint Section 9 for exact columns before adding
migrations.

## Tech stack

Current: Java 25, Spring Boot, Maven, Docker, PostgreSQL.
Introduced as required by the architecture: (introduce only when a concrete problem demands it, per the "engineering over complexity"
principle): Kafka, Elasticsearch, Redis, pgvector, OpenTelemetry, Prometheus, Grafana.
