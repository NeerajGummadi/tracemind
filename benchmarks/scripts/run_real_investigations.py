#!/usr/bin/env python3
"""Test F: fires N real Alertmanager webhooks for payment-service, ONE AT A
TIME (waiting for each investigation to fully complete before firing the
next), so each is a genuinely separate real OpenAI call rather than being
coalesced by Milestone M's storm-suppression logic. Reads each run's
result_payload directly from Postgres (set by InvestigationResultService)
rather than re-consuming Kafka - simpler and exactly as accurate, since
that's the same JSON that was published to investigation.results.v1.
"""
import argparse
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone


def fire_alert(base_url: str, fingerprint: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "status": "firing",
        "alerts": [{
            "status": "firing",
            "labels": {"alertname": "DB_CONNECTION_PRESSURE", "service": "payment-service",
                       "environment": "prod", "severity": "CRITICAL", "instance": "payment-service-2"},
            "annotations": {"summary": "Test F real investigation benchmark"},
            "startsAt": now, "endsAt": "0001-01-01T00:00:00Z",
            "generatorURL": "http://prometheus/graph", "fingerprint": fingerprint,
        }],
    }
    import httpx
    resp = httpx.post(f"{base_url}/integrations/prometheus/alerts", json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()["eventIds"][0]


def psql(sql: str) -> str:
    out = subprocess.run(
        ["docker", "exec", "tracemind-postgres-1", "psql", "-U", "tracemind", "-d", "tracemind", "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=10,
    )
    return out.stdout.strip()


def get_current_run(incident_number: str) -> tuple[str, str, bool]:
    """Returns (run_id, status, needs_reinvestigation). A run showing STALE
    means it was superseded and a follow-up is imminent or already running -
    that is NOT settlement, just an intermediate state. True settlement is
    needs_reinvestigation=false AND the current run is COMPLETED/FAILED -
    the same condition proven correct by hand across Milestones D/M."""
    out = psql(f"""
        SELECT ir.id, ir.status, i.needs_reinvestigation FROM incidents i
        JOIN investigation_runs ir ON ir.id = i.current_investigation_run_id
        WHERE i.incident_number = '{incident_number}';
    """)
    if not out:
        return None, None, None
    run_id, status, needs_reinvestigation = out.split("|")
    return run_id, status, needs_reinvestigation == "t"


def get_result_payload(run_id: str) -> dict | None:
    out = psql(f"SELECT result_payload FROM investigation_runs WHERE id = '{run_id}';")
    if not out:
        return None
    return json.loads(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--base-url", default="http://localhost:8081")
    parser.add_argument("--timeout-per-investigation", type=float, default=30.0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    results = []
    incident_number = None
    last_run_id = None

    for i in range(args.count):
        fingerprint = f"testF-real-{uuid.uuid4()}"
        send_time = time.monotonic()
        event_id = fire_alert(args.base_url, fingerprint)

        if incident_number is None:
            # Discover which incident this created/correlated into, on the first alert.
            # Filtered specifically by primary_service (not just "most recent"), retried
            # rather than a fixed sleep, since ingestion latency can vary.
            for _ in range(20):
                out = psql("SELECT incident_number FROM incidents WHERE primary_service='payment-service' "
                            "AND environment='prod' ORDER BY created_at DESC LIMIT 1;")
                if out:
                    incident_number = out
                    break
                time.sleep(0.5)
            if incident_number is None:
                raise RuntimeError("Incident was never created for payment-service - aborting before any more real calls")

        # Poll until the incident has TRULY settled on a NEW run: current run is
        # COMPLETED/FAILED (not STALE - that means a follow-up is still coming) AND
        # needsReinvestigation is false (nothing else queued behind it).
        deadline = time.monotonic() + args.timeout_per_investigation
        run_id, status = None, None
        while time.monotonic() < deadline:
            run_id, status, needs_reinvestigation = get_current_run(incident_number)
            if run_id and run_id != last_run_id and status in ("COMPLETED", "FAILED") and not needs_reinvestigation:
                break
            time.sleep(0.5)
        settle_time = time.monotonic()

        if run_id == last_run_id or run_id is None:
            results.append({"index": i, "event_id": event_id, "error": "timed out waiting for a new run"})
            print(f"[{i+1}/{args.count}] TIMEOUT waiting for new investigation run")
            continue

        payload = get_result_payload(run_id)
        results.append({
            "index": i, "event_id": event_id, "run_id": run_id, "incident_number": incident_number,
            "client_wall_time_s": round(settle_time - send_time, 3),
            "result": payload,
        })
        last_run_id = run_id
        status_str = payload["status"] if payload else "?"
        metrics = payload.get("metrics") if payload else None
        print(f"[{i+1}/{args.count}] run={run_id} status={status_str} "
              f"totalDurationMs={metrics.get('totalDurationMs') if metrics else '?'} "
              f"openAiLatencyMs={metrics.get('openAiLatencyMs') if metrics else '?'}")

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"wrote {len(results)} results to {args.output}")


if __name__ == "__main__":
    main()
