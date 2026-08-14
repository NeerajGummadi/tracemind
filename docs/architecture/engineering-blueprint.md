# Engineering Blueprint

> **Version:** 1.0
>
> **Status:** Draft
>
> **Project:** TraceMind

---

# 1. Purpose

## Vision

TraceMind is an AI-native incident investigation platform designed to help engineers understand production incidents faster by automatically correlating telemetry across distributed systems and generating evidence-backed root cause analyses.

Rather than replacing existing monitoring platforms, TraceMind complements them by automating the investigation process after an alert has already been triggered.

---

## Problem Statement

Modern production systems generate massive amounts of telemetry, including logs, metrics, traces, and infrastructure events.

Monitoring tools can detect anomalies and notify engineers when systems become unhealthy.

However, identifying **why** an incident occurred still requires engineers to manually collect evidence from multiple systems before they can begin reasoning about the root cause.

This investigation process is repetitive, time-consuming, and difficult to scale.

TraceMind reduces this manual effort by automating evidence collection and assisting engineers with structured, evidence-backed investigations.

---

## Engineering Philosophy

TraceMind follows a small set of engineering principles.

- Solve one problem exceptionally well.
- Prefer simple architectures over unnecessary complexity.
- Introduce new technologies only when they solve a real production problem.
- Every architectural decision must be explainable.
- Every important design decision should be documented.

The goal is not to build the largest distributed system.

The goal is to build the smallest system that demonstrates excellent engineering.

---

# 2. System Boundary

## Where TraceMind Begins

TraceMind begins when an external observability platform notifies it of a production incident.

Rather than continuously monitoring infrastructure or detecting anomalies itself, TraceMind accepts incident notifications from existing monitoring systems and focuses entirely on the investigation workflow.

Incoming notifications first pass through an ingestion layer, where vendor-specific payloads are authenticated, validated, deduplicated, and normalized into TraceMind's internal event contract before entering the remainder of the platform.

This allows the core investigation engine to remain completely independent of any specific monitoring vendor.

Typical upstream systems include:

- Prometheus Alertmanager
- Datadog
- Amazon CloudWatch
- Grafana Alerting
- PagerDuty
- Other monitoring or incident management platforms

These systems remain responsible for detecting that a production problem exists.

TraceMind is responsible for explaining why the problem occurred.

---

## Alert Ingestion Layer

The Alert Ingestion Layer is the entry point into TraceMind.

Its purpose is to isolate the remainder of the platform from vendor-specific integrations.

Responsibilities include:

- Receiving webhook requests from external monitoring systems
- Authenticating incoming requests
- Validating payloads
- Deduplicating repeated alerts
- Normalizing vendor-specific schemas
- Converting alerts into TraceMind's canonical incident event
- Publishing normalized events to the Incident Management workflow

The ingestion layer belongs to the Incident Service during the initial MVP.

If future scale or organizational ownership requires it, the ingestion layer can be extracted into an independent service without impacting downstream components because every internal component depends only on the canonical event contract.

---

## Canonical Incident Event

All downstream components communicate using a single internal event format.

Regardless of whether an alert originated from Prometheus, Datadog, CloudWatch, or another monitoring platform, every alert is transformed into the same internal representation before entering the investigation pipeline.

This provides several advantages:

- Vendor independence
- Simplified business logic
- Consistent event processing
- Easier testing
- Easier onboarding of new monitoring providers
- Long-term maintainability

No downstream component should ever contain vendor-specific parsing logic.

---

## What TraceMind Owns

Once a normalized incident event enters the platform, TraceMind owns the complete investigation lifecycle.

Its responsibilities include:

- Creating and managing incidents
- Tracking investigation state
- Triggering asynchronous investigations
- Collecting relevant evidence
- Correlating telemetry across services
- Understanding service dependencies
- Running AI-assisted reasoning
- Producing evidence-backed root cause analyses
- Persisting investigation results
- Exposing investigation progress and results to engineers

---

## What TraceMind Does Not Own

TraceMind intentionally avoids responsibilities already handled well by existing observability platforms.

It does not own:

- Telemetry generation
- Metrics collection
- Log ingestion
- Distributed trace collection
- Alert rule configuration
- Anomaly detection (initial MVP)
- Infrastructure orchestration
- Automatic production remediation

These responsibilities remain with the production system and existing observability tooling.

---

## Boundary Principle

TraceMind follows one architectural principle:

> **Monitoring systems detect production incidents. TraceMind investigates production incidents.**

This clear separation keeps the platform focused on incident investigation rather than becoming another general-purpose observability platform.

---

## External Dependencies

TraceMind interacts with external systems through well-defined provider interfaces.

Examples include:

- Alert Providers
- Metrics Providers
- Log Providers
- Service Topology Providers
- Notification Providers

Provider-specific integrations remain isolated behind adapters so that the investigation engine never depends directly on Prometheus, Datadog, CloudWatch, or any other vendor implementation.

---

## Initial MVP Boundary

The first implementation targets a controlled production-like environment.

The MVP assumes:

- Prometheus Alertmanager generates alerts.
- Prometheus provides metrics.
- Application logs are available through a searchable log source.
- A predefined service dependency graph is available.
- Engineers review AI-generated investigations before taking action.

Automatic anomaly detection, automatic remediation, and autonomous incident resolution are intentionally outside the scope of the initial release.

# 3. Product Architecture

TraceMind follows a reactive incident investigation architecture.

## Reactive Investigation Path

The reactive path begins after an alert has already fired.

```text

Production System

      │

      ├── Metrics

      ├── Logs

      └── Traces

      │

      ▼

Observability Platform

      │

      ▼

Alertmanager / Provider

      │ webhook

      ▼

Connector

      │

      ▼

Canonical Signal

      │

      ▼

Kafka

      │

      ▼

Incident Service

      │

      ▼

Incident Correlation

      │

      ▼

Investigation Requested

      │

      ▼

Investigation Service

      │

      ├── Metrics

      ├── Logs

      ├── Dependency Graph

      └── Runbooks / Historical Incidents

      │

      ▼

Evidence Correlation

      │

      ▼

Evidence Graph

      │

      ▼

AI Investigation

      │

      ▼

RCA

      │

      ├── Slack

      └── Dashboard

```

---


---

# 4. Components and Responsibilities

## Connector Service

**Technology:** Spring Boot

Responsibilities:

- receive provider webhooks

- validate payloads

- normalize them into `CanonicalSignalV1`

- publish canonical signals to Kafka

- return HTTP `202 Accepted`

Does not:

- create incidents

- query PostgreSQL

- correlate alerts

- query Elasticsearch

- run AI

- perform investigation logic

Initial endpoint:

```http

POST /integrations/prometheus/alerts

```

Successful response:

```json

{

  "status": "ACCEPTED",

  "eventId": "evt-8b0ef143"

}

```

`202 Accepted` is used because TraceMind has accepted the signal for asynchronous processing; investigation has not completed.

---

## Incident Service

**Technology:** Spring Boot

Responsibilities:

- consume canonical signals

- enforce event idempotency

- persist signals

- correlate related signals into incidents

- manage incident lifecycle

- persist incident state

- atomically create outbox events

- expose incident APIs

- persist completed investigation results

The Incident Service owns the incident domain.

---

## Incident Correlation Domain

Incident correlation remains deterministic in the first version.

Candidate incident requirements:

```text

same organization

same environment

same primary service

status != COMPLETED

lastObservedAt within correlation window

```

Initial correlation window:

```text

5 minutes

```

Later correlation can incorporate:

- service dependency proximity

- compatible signal categories

- trace identifiers

- topology relationships

- richer scoring

The first implementation deliberately avoids AI-based incident grouping.

---

## Investigation Service

**Technology:** FastAPI / Python

Responsibilities:

- consume `investigation.requested.v1`

- collect evidence

- query Prometheus

- query Elasticsearch

- resolve service dependencies

- retrieve operational knowledge

- build normalized evidence

- construct an Evidence Graph

- generate root-cause hypotheses

- challenge hypotheses

- compose the final RCA

- validate structured AI output

- publish investigation completion/failure

---

## Evidence Correlation Engine

The Evidence Correlation Engine is a logically distinct subsystem.

For the MVP, it remains physically inside the Investigation Service.

It performs deterministic operations such as:

```text

normalize

   ↓

time-window alignment

   ↓

entity/service matching

   ↓

dependency expansion

   ↓

log-pattern aggregation

   ↓

metric anomaly extraction

   ↓

trace/correlation-ID linking

   ↓

temporal ordering

   ↓

Evidence Graph

```

It should not be extracted into a separate service until there is a real reason such as:

- independent scaling

- heavy CPU requirements

- separate ownership

- independent reuse

---

## Notification Worker

Responsibilities:

- deliver investigation output to Slack

- retry notification failures independently

- never influence investigation correctness

Notification is a side effect, not part of the core transaction.

---

## Dashboard

Responsibilities:

- list incidents

- display incident severity and status

- show investigation progress

- display evidence timeline

- display Evidence Graph

- show probable RCA

- show verification state

- show recommendations

- show unknowns


---

# 5. Canonical System Flow

Example: a database-related alert fires for `payment-service`.

```text

1. Prometheus detects configured condition

      ↓

2. Alertmanager fires webhook

      ↓

3. Connector validates payload

      ↓

4. Connector converts provider payload → CanonicalSignalV1

      ↓

5. Connector publishes signals.received.v1

      ↓

6. Incident Service consumes signal

      ↓

7. Event idempotency check

      ↓

8. Incident correlation

      ├── attach to existing incident

      └── create new incident

      ↓

9. Persist signal / incident state in PostgreSQL

      ↓

10. Write investigation.requested event to outbox atomically

      ↓

11. Outbox publisher sends event to Kafka

      ↓

12. Investigation Service consumes request

      ↓

13. Acquire optional short-lived coordination lock

      ↓

14. Collect evidence concurrently

      ├── Elasticsearch logs

      ├── Prometheus metrics

      ├── dependency graph

      └── pgvector runbooks/history

      ↓

15. Deterministic evidence correlation

      ↓

16. Build Evidence Graph

      ↓

17. AI hypothesis generation

      ↓

18. AI critic / verifier

      ↓

19. RCA composer + schema validator

      ↓

20. Publish investigation.completed

      ↓

21. Incident Service persists result

      ↓

22. Notification Worker sends Slack update

      ↓

23. Dashboard displays investigation

```

The production application never waits for this workflow.

---

# 6. Canonical Contracts

## CanonicalSignalV1

Everything entering TraceMind is normalized into the same canonical event.

```json

{

  "eventId": "evt-8b0ef143",

  "schemaVersion": "1.0",

  "source": "PROMETHEUS",

  "signalType": "DB_CONNECTION_PRESSURE",

  "service": "payment-service",

  "environment": "prod",

  "severity": "CRITICAL",

  "startedAt": "2026-08-12T14:03:00Z",

  "observedAt": "2026-08-12T14:03:05Z",

  "labels": {

    "instance": "payment-service-2"

  },

  "attributes": {

    "metric": "db_pool_utilization",

    "value": 98.7,

    "threshold": 95

  }

}

```

Prometheus, Datadog, CloudWatch, and future connectors all produce this structure.

This contract is the primary vendor-neutral boundary of TraceMind.

---

## InvestigationRequestedV1

```json

{

  "eventId": "evt-investigation-1001",

  "schemaVersion": "1.0",

  "incidentId": "INC-1001",

  "primaryService": "payment-service",

  "environment": "prod",

  "severity": "CRITICAL",

  "firstObservedAt": "...",

  "lastObservedAt": "...",

  "triggerSignalIds": [

    "evt-1",

    "evt-2",

    "evt-3"

  ]

}

```

---

# 7. Kafka Architecture

## signals.received.v1

Producer:

```text

Connector Service

```

Consumer:

```text

Incident Service

```

Partition key:

```text

environment + ":" + service

```

Example:

```text

prod:payment-service

```

Reason:

Before an incident ID exists, signals concerning the same service and environment should remain ordered together as much as possible.

---

## investigation.requested.v1

Producer:

```text

Incident Service Outbox Publisher

```

Consumer:

```text

Investigation Service

```

Partition key:

```text

incidentId

```

Reason:

All workflow events related to the same incident should land on the same partition and preserve relative ordering.

---

## Delivery Semantics

TraceMind assumes:

> **At-least-once delivery.**

The system does not attempt global exactly-once processing.

Therefore:

- consumers must be idempotent

- duplicate delivery must produce the same final business state

- durable deduplication is enforced through PostgreSQL constraints/state

---

# 8. Incident Model

An alert is not an incident.

Multiple signals may describe the same underlying incident.

Example:

```text

14:01 DB_LATENCY_HIGH

payment-service

14:02 DB_CONNECTION_PRESSURE

payment-service

14:04 HTTP_5XX_HIGH

payment-service

```

These may all attach to:

```text

INC-1001

```

rather than creating three incidents.

---

## Incident Lifecycle

```text

DETECTED

   ↓

QUEUED

   ↓

INVESTIGATING

   ↓

COMPLETED

```

Alternative exit:

```text

INVESTIGATING

   ↓

FAILED

```

Future transitions may include:

```text

FAILED → QUEUED

COMPLETED → REOPENED

```

Statuses should not be modified arbitrarily.

The domain should expose explicit transition operations such as:

```text

queueInvestigation()

startInvestigation()

completeInvestigation()

failInvestigation()

```

---

# 9. PostgreSQL Data Model

PostgreSQL is the authoritative source of truth for TraceMind business state.

The first implementation starts with four tables.

## signals

```sql

signals

-------

id UUID PRIMARY KEY

event_id VARCHAR UNIQUE NOT NULL

source VARCHAR NOT NULL

signal_type VARCHAR NOT NULL

service VARCHAR NOT NULL

environment VARCHAR NOT NULL

severity VARCHAR NOT NULL

started_at TIMESTAMP

observed_at TIMESTAMP NOT NULL

payload JSONB NOT NULL

created_at TIMESTAMP NOT NULL

```

---

## incidents

```sql

incidents

---------

id UUID PRIMARY KEY

incident_number VARCHAR UNIQUE NOT NULL

title VARCHAR NOT NULL

primary_service VARCHAR NOT NULL

environment VARCHAR NOT NULL

severity VARCHAR NOT NULL

status VARCHAR NOT NULL

first_observed_at TIMESTAMP NOT NULL

last_observed_at TIMESTAMP NOT NULL

created_at TIMESTAMP NOT NULL

updated_at TIMESTAMP NOT NULL

```

---

## incident_signals

```sql

incident_signals

----------------

incident_id UUID REFERENCES incidents(id)

signal_id UUID REFERENCES signals(id)

PRIMARY KEY (incident_id, signal_id)

```

---

## outbox_events

```sql

outbox_events

-------------

id UUID PRIMARY KEY

aggregate_type VARCHAR NOT NULL

aggregate_id UUID NOT NULL

event_type VARCHAR NOT NULL

payload JSONB NOT NULL

status VARCHAR NOT NULL

created_at TIMESTAMP NOT NULL

published_at TIMESTAMP

```

---

# 10. Transactional Outbox

The Incident Service must not perform:

```text

save incident

   ↓

Kafka.send(...)

```

because this creates a dual-write failure window.

Example:

```text

database commit succeeds

   ↓

application crashes

   ↓

Kafka publish never happens

```

Result:

```text

incident = QUEUED

```

but no investigation will ever begin.

Instead:

```text

BEGIN

INSERT incident

INSERT outbox_event(

  investigation.requested

)

COMMIT

```

Then a separate outbox publisher reads unpublished events and delivers them to Kafka.

This guarantees that:

> Database state and the intention to publish are committed atomically.

---

# 11. Idempotency and Deduplication

TraceMind contains multiple distinct deduplication problems.

## Event Deduplication

The exact same webhook/Kafka event may be delivered multiple times.

Durable solution:

```text

event_id UNIQUE

```

in PostgreSQL.

Redis may optionally optimize this check, but correctness must remain in PostgreSQL.

---

## Investigation Deduplication

Two workers may attempt to process the same incident.

Redis may provide a short-lived coordination lock such as:

```text

SET investigation:INC-1001 worker-7 NX EX 120

```

However, Redis must never be required for durable correctness.

If the lock disappears, duplicate expensive work may occur, but PostgreSQL/state/idempotency must keep business state correct.

---

## Incident Correlation

This is not event deduplication.

Different legitimate signals may belong to the same incident.

That is a domain correlation problem.

---

# 12. Evidence Model

Raw telemetry does not go directly into the LLM.

TraceMind first converts telemetry into normalized evidence.

Example:

```json

{

  "evidenceId": "E-701",

  "incidentId": "INC-1001",

  "type": "METRIC",

  "source": "PROMETHEUS",

  "entity": "payment-db",

  "fact": "Connection pool utilization reached 100%",

  "firstObservedAt": "14:01:43Z",

  "lastObservedAt": "14:04:12Z",

  "value": 100,

  "unit": "percent",

  "reference": "prometheus://...",

  "quality": 0.98

}

```

Another example:

```json

{

  "evidenceId": "E-702",

  "type": "LOG_PATTERN",

  "entity": "payment-service",

  "fact": "Hikari connection acquisition timeout repeated",

  "occurrences": 73,

  "firstObservedAt": "14:02:04Z"

}

```

---

# 13. Dependency Graph

The Dependency Graph represents system topology.

Example:

```text

Gateway

   ↓

Payment

   ↓

Fraud

   ↓

PostgreSQL

```

For the MVP, topology may come from static configuration.

Future sources may include:

- OpenTelemetry

- Kubernetes

- service mesh

- runtime discovery

---

# 14. Evidence Graph

The Evidence Graph represents what happened during one specific incident.

Nodes represent:

- facts

- entities

- events

Edges represent relationships.

Example:

```text

[DB pool 100%]

      │ occurred_before

      ▼

[Hikari timeouts]

      │ observed_in

      ▼

[Fraud Service]

      │ upstream_of_failure

      ▼

[Payment Service 5xx]

```

Possible edge types include:

```text

DEPENDS_ON

OCCURRED_BEFORE

OBSERVED_IN

SAME_TRACE

SAME_SERVICE

CORRELATED_WITH

PROPAGATED_TO

SUPPORTED_BY

CONTRADICTED_BY

```

Important distinction:

> `OCCURRED_BEFORE` is an observable relationship.

> `CAUSED_BY` is often a hypothesis.

Deterministic code must not claim causality when only correlation exists.

---

# 15. AI Investigation Engine

The LLM is invoked only after evidence construction.

Architecture:

```text

Evidence Graph

      ↓

Hypothesis Generator

      ↓

Candidate Hypotheses

      ↓

Critic / Verifier

      ↓

Revised Hypothesis Set

      ↓

RCA Composer

      ↓

Schema Validator

```

The system intentionally avoids a large collection of unnecessary agents.

Three meaningful reasoning stages are sufficient:

- hypothesis generation

- criticism / falsification

- RCA composition

---

# 16. Hypothesis Contract

Example:

```json

{

  "hypotheses": [

    {

      "id": "H1",

      "description": "Slow SQL caused DB connection pool exhaustion",

      "supportingEvidence": [

        "E-701",

        "E-702",

        "E-708"

      ],

      "contradictingEvidence": [

        "E-710"

      ],

      "confidence": 0.82,

      "missingEvidence": [

        "database slow-query sample"

      ]

    }

  ]

}

```

The LLM cannot simply say:

```text

database issue

```

Every substantive hypothesis must cite evidence identifiers.

---

# 17. Critic / Verification Stage

The critic does not invent a new RCA.

Its job is to attempt to falsify each hypothesis.

It should identify:

- unsupported causal claims

- contradictions

- missing evidence

- weak evidence chains

- unjustified confidence

Example:

```json

{

  "hypothesisId": "H1",

  "verdict": "SUPPORTED_WITH_GAPS",

  "problems": [

    "No direct slow-query evidence exists."

  ],

  "adjustedConfidence": 0.71

}

```

---

# 18. Final RCA Contract

Example:

```json

{

  "incidentId": "INC-1001",

  "status": "COMPLETED",

  "probableRootCause": {

    "summary": "Database connection pool exhaustion",

    "confidence": 0.89

  },

  "causalChain": [

    "DB connection usage saturated",

    "connection acquisition timeouts began",

    "fraud-service latency increased",

    "payment-service 5xx rate increased"

  ],

  "evidence": [

    "E-701",

    "E-702",

    "E-704"

  ],

  "contributingFactors": [],

  "unknowns": [

    "Underlying cause of elevated connection duration not yet confirmed"

  ],

  "recommendedActions": {

    "immediate": [],

    "shortTerm": [],

    "longTerm": []

  }

}

```

A trustworthy incident system must be allowed to say:

> "I don't know this part."

---

# 19. AI Verification States

Possible RCA verification states:

```text

VERIFIED

PARTIALLY_VERIFIED

UNVERIFIED

INSUFFICIENT_EVIDENCE

```

`VERIFIED` means:

> verified against the evidence available to TraceMind.

It does not mean mathematically proven causality.

---

# 20. Confidence Model

Raw LLM confidence must not be presented as calibrated probability.

TraceMind confidence should be influenced by:

- number of independent evidence sources

- evidence agreement

- temporal consistency

- dependency consistency

- critic verdict

- missing evidence

- evidence quality

External presentation should prefer:

```text

HIGH

MEDIUM

LOW

```

rather than implying statistical calibration that does not exist.

---

# 21. RAG and Operational Knowledge

Runbooks and historical incident summaries may be stored in PostgreSQL with pgvector.

Pipeline:

```text

incident / evidence summary

      ↓

embedding

      ↓

pgvector similarity search

      ↓

top-K runbook/history chunks

      ↓

operational context

```

RAG does not determine the RCA.

It supplies operational knowledge.

---

# 22. Source of Truth

## PostgreSQL

Authoritative for:

- incidents

- lifecycle

- investigations

- evidence metadata

- integration configuration

- dependency topology

- durable business state

---

## Kafka

Owns:

- asynchronous transport

- durable event delivery

Kafka is not the application source of truth.

---

## Elasticsearch

Owns:

- operational log search

It is not authoritative for TraceMind business state.

---

## Redis

Owns:

- ephemeral coordination

- cache

- short-lived locks

- fast optimization

Losing Redis must not destroy an incident or corrupt durable state.

> Redis failure hurts efficiency, not correctness.

---

## pgvector

Owns:

- semantic retrieval of runbooks

- historical incident knowledge

---

# 23. Async Programming and Concurrency

There are two forms of asynchrony.

## Event-Level Asynchrony

Kafka decouples services.

Alert ingestion never waits for investigation completion.

---

## Intra-Investigation Concurrency

Independent evidence queries may run concurrently.

```text

Incident

   ├── Elasticsearch logs

   ├── Prometheus metrics

   ├── dependency lookup

   └── pgvector retrieval

```

Python may use:

```python

asyncio.gather(...)

```

or equivalent asynchronous clients.

Sequential versus concurrent evidence collection should be benchmarked.

---

# 24. Concurrency Limits

TraceMind must use bounded concurrency.

Never allow a burst of incidents to create an unbounded number of simultaneous LLM calls.

Conceptually:

```text

investigation workers = N

LLM calls per worker = M

global effective concurrency = N × M

```

Concurrency limits are configurable.

For the MVP, a small demo limit is sufficient.

Kafka absorbs temporary bursts.

---

# 25. Backpressure

If:

```text

incident arrival rate > investigation throughput

```

then:

```text

Kafka consumer lag increases

```

This is acceptable temporarily.

Monitor:

- consumer lag

- oldest queued investigation

- investigation duration

- failure rate

If sustained lag remains high, increase worker capacity rather than dropping incidents or overwhelming downstream providers.

---

# 26. Timeouts

Every network call must have a bounded timeout.

There must be:

> **No infinite waits anywhere.**

Timeouts apply to:

- Prometheus

- Elasticsearch

- Redis

- PostgreSQL

- LLM provider

- Slack

- vector retrieval

Exact values remain configurable and should be determined through measurement rather than guessed as permanent architecture constants.

---

# 27. Retry Policy

Retry only transient failures.

Good candidates:

- network timeout

- HTTP 429

- temporary 5xx

- temporary Kafka broker unavailability

- transient Slack failure

Do not retry:

- malformed payload

- invalid schema

- authorization failure

- deterministic application error

Retry pattern:

```text

attempt

   ↓ failure

exponential backoff + jitter

   ↓

attempt

   ↓

max attempts reached

   ↓

DLQ / FAILED

```

---

# 28. Circuit Breakers

Circuit breakers should not be placed everywhere.

Strong candidate:

```text

LLM Provider

```

If the provider repeatedly fails:

```text

CLOSED

   ↓ failures

OPEN

   ↓ cooldown

HALF_OPEN

```

During `OPEN`:

- stop hammering the provider

- mark investigations delayed/degraded

- preserve evidence

- allow later recovery

---

# 29. Partial Failures and Graceful Degradation

Example:

```text

Logs available ✅

Metrics available ✅

Runbook unavailable ❌

```

Investigation should not automatically fail.

Proceed with reduced evidence completeness.

Example:

```json

{

  "evidenceCompleteness": 0.72,

  "missingSources": [

    "RUNBOOK"

  ]

}

```

If logs are unavailable but metrics and topology exist, the investigation may proceed with lower confidence.

This is graceful degradation.

---

# 30. AI Partial Failure

Suppose:

```text

Hypothesis generation succeeds

Critic fails

```

TraceMind should not pretend the RCA is fully verified.

Possible output:

```text

RCA status:

UNVERIFIED

```

Useful partial results should be preserved.

---

# 31. LLM Failure

If the model times out:

1. retry using a bounded policy

2. optionally support model fallback later

3. preserve collected evidence

4. mark the AI stage failed/degraded

5. continue exposing evidence through the dashboard

> TraceMind should remain useful when AI is unavailable.

---

# 32. Redis Failure

If Redis fails:

- cache is lost

- short-lived coordination locks may disappear

- duplicate expensive work may occur

However:

- PostgreSQL remains authoritative

- idempotency protects durable state

- business correctness is preserved

---

# 33. Elasticsearch Failure

If Elasticsearch fails:

TraceMind loses log evidence.

It does not lose:

- incident state

- metrics

- dependency topology

- runbooks

- workflow state

Investigation may proceed in degraded mode.

---

# 34. Kafka Failure

Producers should fail safely.

A component must not acknowledge durable success if the event cannot be persisted or safely scheduled for delivery.

Transactional outbox is the preferred protection for database-backed producers.

---

# 35. PostgreSQL Failure

PostgreSQL is the most serious dependency because it is the durable source of truth.

No new durable incident state should be acknowledged as successful while PostgreSQL is unavailable.

The MVP may simply degrade rather than attempting sophisticated stale-read behavior.

---

# 36. Slack Failure

Slack is not part of the core workflow.

```text

Investigation completed ✅

Notification failed ⚠️

```

Notification retries independently.

A Slack outage must never invalidate a completed investigation.

---

# 37. AI Safety and Hallucination Control

System rule:

> Use only supplied evidence. Never fabricate telemetry, relationships, or actions. Explicitly identify insufficient evidence and unknowns.

Every substantive claim should reference:

```text

evidenceIds[]

```

Principle:

> No evidence → no high-confidence claim.

---

# 38. Dashboard

## Overview

Display:

- active incidents

- severity

- status

- recent investigations

## Incident Details

Display:

- title

- affected services

- status

- evidence timeline

- Evidence Graph

- probable RCA

- verification state

- recommendations

- unknowns

## System Topology

Display the dependency graph.

---

# 39. Slack Output

Example:

```text

🚨 INC-1001 — Payment Service Degradation

Severity: Critical

Probable root cause

Database connection pool exhaustion

Verification

SUPPORTED WITH GAPS

Evidence

• Pool utilization reached 100% at 14:01

• Hikari acquisition failures began at 14:02

• Payment 5xx increased at 14:04

Propagation

payment-db → fraud-service → payment-service

Immediate actions

• Inspect long-running DB queries

• Check leaked/held connections

• Verify pool configuration

Unknown

Underlying cause of elevated connection duration not yet confirmed.

Open TraceMind →

```

---

# 40. MVP Scope

The first MVP prioritizes the reactive golden path:

```text

Webhook / Alertmanager

      ↓

Connector

      ↓

Kafka

      ↓

Incident Service

      ↓

PostgreSQL

      ↓

Outbox

      ↓

Investigation Requested

      ↓

Evidence Collection

      ↓

Evidence Graph

      ↓

Hypothesis

      ↓

Critic

      ↓

RCA

      ↓

Slack + Basic Dashboard

```

RAG is outside the initial MVP.

Dashboard polish is outside the initial MVP.

The reactive investigation path must work first.

---

# 41. Repository Structure

```text

tracemind/

│

├── services/

│   ├── connector-service/        # Spring Boot

│   ├── incident-service/         # Spring Boot

│   ├── investigation-service/    # FastAPI

│

├── frontend/

│   └── dashboard/

│

├── infrastructure/

│   ├── nginx/

│   ├── prometheus/

│   ├── alertmanager/

│   ├── elasticsearch/

│   ├── kafka/

│   ├── postgres/

│   └── redis/

│

├── shared/

│   ├── contracts/

│   ├── topology/

│   └── runbooks/

│

├── simulator/

│   └── telemetry-generator/

│

├── docs/

│   ├── architecture/

│   ├── adrs/

│   ├── api/

│   └── diagrams/

│

├── docker-compose.yml

└── README.md

```

---

# 42. MVP Infrastructure

Initial Docker Compose may include:

```text

PostgreSQL

Kafka

Redis

Elasticsearch

```

However, the first proven vertical slice requires only:

```text

PostgreSQL

Kafka

Connector Service

Incident Service

```

Prometheus and Alertmanager may be added after a simulated webhook works.

The first system proof should be:

```text

curl alert payload

      ↓

Connector

      ↓

Kafka

      ↓

Incident Service

      ↓

PostgreSQL

```

---

# 43. Architectural Invariants

These rules are frozen unless a deliberate architecture review changes them.

## Invariant 1

> PostgreSQL is the authoritative source of TraceMind business state.

## Invariant 2

> Kafka is event transport, not application truth.

## Invariant 3

> Redis is never required for durable correctness.

## Invariant 4

> The system assumes at-least-once delivery and uses idempotent consumers.

## Invariant 5

> Database state and event-publish intent are made atomic through the transactional outbox where required.

## Invariant 6

> Vendor-specific alert formats terminate at the connector boundary.

## Invariant 7

> Downstream systems operate only on canonical contracts.

## Invariant 8

> Raw telemetry does not go directly into the LLM.

## Invariant 9

> Deterministic evidence construction occurs before probabilistic reasoning.

## Invariant 10

> AI never owns the truth; AI explains the evidence.

## Invariant 11

> The Dependency Graph describes system topology; the Evidence Graph describes a specific incident.

## Invariant 12

> Observable relationships and causal hypotheses must remain distinct.

## Invariant 13

> Failure of one evidence source should degrade confidence, not automatically destroy the investigation.

## Invariant 14

> TraceMind should remain useful when the AI provider is unavailable.

## Invariant 15

> Logical subsystem boundaries do not automatically justify additional microservices.

---
- The engineering blueprint (`docs/architecture/engineering-blueprint.md`) is the authoritative architectural specification. If repository code, README, or CLAUDE.md conflicts with the blueprint, follow the blueprint and ask for clarification rather than making assumptions.


# 44. Coding-Agent Guardrails

Coding agents must not independently change architecture.

Explicit instructions:

> Do not introduce additional services, databases, brokers, frameworks, or architectural components without approval.

> Do not replace Kafka with direct REST communication.

> Do not skip the transactional outbox by directly publishing Kafka events after database commit.

> PostgreSQL is the source of truth. Redis must not be used for durable correctness.

> Use at-least-once delivery assumptions and make consumers idempotent.

> Do not move raw telemetry directly into the LLM.

> Do not create additional microservices for logical subsystems unless independent deployment or scaling requirements justify them.

These guardrails prevent agent-generated architecture drift.

---

# 45. Architecture Freeze Statement

TraceMind is a vendor-neutral incident intelligence platform built above existing observability systems.

Alerts enter through provider-specific connectors and are normalized into stable canonical contracts.

Kafka provides durable asynchronous transport.

PostgreSQL owns durable incident and investigation state.

Incident Service performs deterministic signal deduplication, incident correlation, lifecycle management, and transactional event publication.

Investigation Service collects heterogeneous evidence and deterministically transforms it into an Evidence Graph.

Only after evidence construction does AI generate, challenge, and explain root-cause hypotheses.

Elasticsearch provides operational log search.

Redis provides ephemeral coordination only.

pgvector supplies operational knowledge retrieval.

The platform degrades gracefully when evidence providers, notification systems, caches, or AI providers fail.

The reactive investigation golden path is the primary product.

> **This is the system we are building.**

## Engineering Philosophy

When implementing TraceMind:

- Prefer simple, explainable designs over clever abstractions.
- Every technology must justify its existence.
- Build the smallest system that demonstrates production-quality engineering.
- Do not introduce infrastructure before it solves a real problem.
- Ask before changing architecture.

Before implementing any feature:

1. Explain the implementation plan.
2. Wait for approval if architecture changes are required.
3. Implement in small reviewable commits.
4. Explain trade-offs after implementation.