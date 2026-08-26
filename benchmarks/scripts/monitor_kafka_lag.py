#!/usr/bin/env python3
"""Samples Kafka consumer-group lag (via kafka-consumer-groups.sh inside the
broker container) at a fixed interval, for a fixed duration. Reports total
lag summed across partitions per group, plus per-partition detail in the
raw CSV rows.
"""
import argparse
import csv
import re
import subprocess
import time


def describe_group(container: str, group: str) -> list[dict]:
    out = subprocess.run(
        ["docker", "exec", container, "/opt/kafka/bin/kafka-consumer-groups.sh",
         "--bootstrap-server", "localhost:9092", "--describe", "--group", group],
        capture_output=True, text=True, timeout=10,
    )
    rows = []
    for line in out.stdout.splitlines():
        parts = re.split(r"\s+", line.strip())
        if len(parts) >= 6 and parts[0] == group:
            # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            try:
                rows.append({
                    "topic": parts[1], "partition": parts[2],
                    "current_offset": int(parts[3]), "log_end_offset": int(parts[4]),
                    "lag": int(parts[5]),
                })
            except ValueError:
                continue
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="tracemind-kafka-1")
    parser.add_argument("--group", action="append", required=True, help="consumer group name, repeatable")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    rows = []
    start = time.monotonic()
    while time.monotonic() - start < args.duration:
        t = round(time.monotonic() - start, 2)
        for group in args.group:
            partitions = describe_group(args.container, group)
            total_lag = sum(p["lag"] for p in partitions)
            rows.append({"t": t, "group": group, "total_lag": total_lag,
                         "partitions": len(partitions)})
        time.sleep(args.interval)

    if rows:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
