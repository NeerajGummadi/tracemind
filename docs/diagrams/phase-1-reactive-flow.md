# Phase 1 Reactive Flow — Sequence Diagram

Complete request lifecycle for the reactive golden path, as implemented and
validated in Milestones A–F. Covers the happy path, the idempotent-duplicate
branch, the correlation-vs-new-incident branch, and the outbox retry loop.

```mermaid
sequenceDiagram
    participant AM as Alertmanager
    participant Conn as Connector Service
    participant K1 as Kafka: signals.received.v1
    participant Inc as Incident Service (listener)
    participant PG as PostgreSQL
    participant Pub as Outbox Publisher (scheduled)
    participant K2 as Kafka: investigation.requested.v1

    AM->>Conn: POST /integrations/prometheus/alerts
    Conn->>Conn: validate payload
    Conn->>Conn: derive deterministic eventId<br/>(SHA-256 of fingerprint + startsAt)
    Conn->>Conn: map to CanonicalSignalV1
    Conn->>K1: publish, key = environment:service
    K1-->>Conn: broker ack (bounded wait)
    Conn-->>AM: 202 Accepted { eventId }

    Note over Conn,AM: publish failure -> 503, not 202<br/>(never ack a signal Kafka didn't durably accept)

    K1->>Inc: deliver CanonicalSignalV1
    Inc->>Inc: deserialize (ignoring producer's<br/>foreign __TypeId__ header)

    activate Inc
    Inc->>PG: BEGIN
    Inc->>PG: INSERT signal (event_id UNIQUE), flush immediately

    alt event_id already exists (duplicate delivery)
        PG-->>Inc: DataIntegrityViolationException
        Inc->>PG: ROLLBACK (nothing else written)
        Inc->>Inc: throw DuplicateSignalException
        Note over Inc: caught by listener, logged, still acknowledged -<br/>already fully processed earlier, safe to skip
    else new signal
        PG-->>Inc: insert OK
        Inc->>PG: SELECT candidate incidents<br/>(env + service + status != COMPLETED + within 5 min)
        alt matching open incident found
            Inc->>PG: UPDATE incident: lastObservedAt,<br/>severity (only if escalating)
            Inc->>PG: INSERT incident_signals link
            Note over Inc,PG: no outbox row - investigation<br/>already requested for this incident
        else no match
            Inc->>PG: INSERT incident (status=QUEUED)
            Inc->>PG: INSERT incident_signals link
            Inc->>PG: INSERT outbox_events (status=PENDING)
        end
        Inc->>PG: COMMIT
    end
    deactivate Inc

    Inc->>K1: acknowledge offset (only after commit,<br/>or after a confirmed duplicate)

    loop every poll-interval-ms
        Pub->>PG: SELECT ... WHERE status='PENDING'<br/>ORDER BY created_at LIMIT 1<br/>FOR UPDATE SKIP LOCKED
        alt row claimed
            PG-->>Pub: locked PENDING row
            Pub->>Pub: extract incidentId from stored payload
            Pub->>K2: publish stored payload verbatim,<br/>key = incidentId
            alt publish succeeds within bounded timeout
                K2-->>Pub: broker ack
                Pub->>PG: UPDATE status=PUBLISHED, published_at=now()
                Pub->>PG: COMMIT
            else publish fails or times out
                Pub->>PG: ROLLBACK (row stays exactly as claimed: PENDING)
                Note over Pub: batch loop stops for this cycle -<br/>next scheduled poll is the retry, no tight loop
            end
        else nothing pending
            Note over Pub: poll ends, nothing to do
        end
    end
```

## Notes on what this diagram intentionally omits

- **Investigation Service** does not exist yet (out of scope through Milestone F) — `investigation.requested.v1` has no consumer in this diagram.
- **The crash-between-Kafka-ack-and-status-update window** on the Outbox Publisher side is real and accepted, not shown as a failure branch here — see "Crash Window Documentation" in `docs/architecture/phase-1-validation-report.md` for why.
- **Kafka partition assignment / consumer group rebalance** (e.g. after a broker restart) isn't modeled — it's infrastructure-level Kafka behavior, not application logic, though Milestone F's validation did observe and confirm it doesn't lose messages.
