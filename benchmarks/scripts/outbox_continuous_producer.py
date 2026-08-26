#!/usr/bin/env python3
"""Test E Scenario 2: inserts `--rate` synthetic PENDING outbox_events rows
once per second, for `--duration` seconds, while the real (already-running)
OutboxPublisher drains concurrently - determines whether backlog grows
forever, stabilizes, or drains completely at a given producer rate.
"""
import argparse
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone


def make_row(prefix: str, i: int) -> str:
    incident_id = f"INC-BENCH-{prefix}-{i}"
    run_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    payload = {
        "eventId": f"evt-{uuid.uuid4()}", "schemaVersion": "1.0", "incidentId": incident_id,
        "primaryService": "outbox-bench-service", "environment": "prod", "severity": "CRITICAL",
        "firstObservedAt": now_iso, "lastObservedAt": now_iso,
        "triggerSignalIds": [f"evt-{uuid.uuid4()}"], "investigationRunId": run_id, "inputSignalVersion": 1,
    }
    payload_json = json.dumps(payload).replace("'", "''")
    return f"('{uuid.uuid4()}','Incident','{uuid.uuid4()}','investigation.requested','{payload_json}','PENDING',now())"


def insert_batch(container: str, db: str, user: str, prefix: str, start: int, count: int):
    rows = [make_row(prefix, start + i) for i in range(count)]
    sql = "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at) VALUES " + ",".join(rows) + ";"
    subprocess.run(["docker", "exec", "-i", container, "psql", "-U", user, "-d", db],
                    input=sql, capture_output=True, text=True, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, required=True, help="rows inserted per second")
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--run-id-prefix", required=True)
    parser.add_argument("--container", default="tracemind-postgres-1")
    parser.add_argument("--db", default="tracemind")
    parser.add_argument("--user", default="tracemind")
    args = parser.parse_args()

    total = 0
    start_time = time.monotonic()
    tick = 0
    while time.monotonic() - start_time < args.duration:
        tick_start = time.monotonic()
        insert_batch(args.container, args.db, args.user, args.run_id_prefix, total, args.rate)
        total += args.rate
        tick += 1
        elapsed_this_tick = time.monotonic() - tick_start
        if elapsed_this_tick < 1.0:
            time.sleep(1.0 - elapsed_this_tick)
        print(f"tick={tick} total_produced={total}")

    print(f"done: produced {total} rows over {time.monotonic()-start_time:.1f}s (target rate={args.rate}/s)")


if __name__ == "__main__":
    main()
