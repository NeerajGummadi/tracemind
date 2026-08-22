"""Smallest possible deterministic log source for local Loki ingestion - not
a TraceMind service, just a local demo fixture (mirrors
infrastructure/prometheus/demo_metrics_exporter.py's role for metrics)
so Loki has real ingested log lines for the DB-connection-pool-exhaustion
scenario used throughout Milestones G-K. Stdlib only, no new dependency.
Pushes once and exits - unlike the Prometheus exporter, Loki is push-based,
there's nothing to keep serving.

Run directly: python3 demo_log_source.py [loki_base_url, default http://localhost:3100]
"""

import json
import sys
import time
import urllib.request

# 73x matches the exact occurrence count already used by StubLogsCollector's
# "Hikari connection acquisition timeout repeated" narrative, for continuity.
DEMO_LOG_LINES: dict[str, int] = {
    "Hikari connection acquisition timeout": 73,
    "Connection pool exhausted": 5,
    "Database connection unavailable": 3,
}


def push(base_url: str) -> None:
    now_ns = time.time_ns()
    values = []
    for line, count in DEMO_LOG_LINES.items():
        for _ in range(count):
            values.append([str(now_ns), line])
            now_ns += 1  # Loki requires strictly increasing timestamps within a stream push

    values.sort(key=lambda v: int(v[0]))

    payload = {
        "streams": [
            {
                "stream": {"service": "payment-service", "environment": "prod"},
                "values": values,
            }
        ]
    }

    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/loki/api/v1/push",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        print(f"Pushed {len(values)} log lines to Loki, status={response.status}")


if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:3100"
    push(base_url)
