#!/usr/bin/env python3
"""Test E: seeds N synthetic PENDING outbox_events rows directly into
Postgres, bypassing the connector/signal-ingestion pipeline entirely - this
isolates the Outbox Publisher alone (Postgres -> outbox_events -> Publisher
-> Kafka), per Test E's explicit isolation requirement (no Investigation
Service, no OpenAI, no Prometheus, no Loki involved).

Payload shape matches what OutboxEvent.investigationRequested() actually
produces (same JSON field set investigation-service's real contract
expects), so this is a realistic payload size/shape even though nothing
consumes it downstream in this test.
"""
import argparse
import json
import subprocess
import uuid
from datetime import datetime, timezone


def make_row(run_id_prefix: str, i: int) -> str:
    incident_id = f"INC-BENCH-{run_id_prefix}-{i}"
    run_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    payload = {
        "eventId": f"evt-{uuid.uuid4()}",
        "schemaVersion": "1.0",
        "incidentId": incident_id,
        "primaryService": "outbox-bench-service",
        "environment": "prod",
        "severity": "CRITICAL",
        "firstObservedAt": now_iso,
        "lastObservedAt": now_iso,
        "triggerSignalIds": [f"evt-{uuid.uuid4()}"],
        "investigationRunId": run_id,
        "inputSignalVersion": 1,
    }
    payload_json = json.dumps(payload).replace("'", "''")
    row_id = str(uuid.uuid4())
    aggregate_id = str(uuid.uuid4())
    return f"('{row_id}','Incident','{aggregate_id}','investigation.requested','{payload_json}','PENDING',now())"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--run-id-prefix", required=True)
    parser.add_argument("--container", default="tracemind-postgres-1")
    parser.add_argument("--db", default="tracemind")
    parser.add_argument("--user", default="tracemind")
    parser.add_argument("--batch-size", type=int, default=2000)
    args = parser.parse_args()

    inserted = 0
    for start in range(0, args.count, args.batch_size):
        end = min(start + args.batch_size, args.count)
        rows = [make_row(args.run_id_prefix, i) for i in range(start, end)]
        sql = "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at) VALUES " + ",".join(rows) + ";"
        # SQL is piped via stdin, not passed as a `-c` argument - at batch_size=2000 the
        # generated SQL string (~600-800KB) exceeds the OS's ARG_MAX for a single exec
        # argument (OSError: Argument list too long), confirmed empirically at count=10000.
        proc = subprocess.run(
            ["docker", "exec", "-i", args.container, "psql", "-U", args.user, "-d", args.db],
            input=sql, capture_output=True, text=True,
        )
        if proc.returncode != 0:
            print(f"FAILED at batch starting {start}: {proc.stderr}")
            raise SystemExit(1)
        inserted += (end - start)
        print(f"inserted {inserted}/{args.count}")

    print(f"done: {inserted} PENDING outbox rows seeded (prefix={args.run_id_prefix})")


if __name__ == "__main__":
    main()
