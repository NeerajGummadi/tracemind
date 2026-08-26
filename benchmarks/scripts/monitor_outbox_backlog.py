#!/usr/bin/env python3
"""Test E: polls outbox_events status counts (PENDING/PUBLISHED, and any
other status value actually present - the schema only ever writes PENDING
or PUBLISHED, see benchmark-results.md) at a fixed interval, for a fixed
duration or until PENDING reaches 0, whichever comes first. Writes CSV.
"""
import argparse
import csv
import subprocess
import time


def query_counts(container: str, db: str, user: str) -> dict:
    sql = "SELECT status, COUNT(*) FROM outbox_events GROUP BY status;"
    out = subprocess.run(
        ["docker", "exec", container, "psql", "-U", user, "-d", db, "-t", "-A", "-F,", "-c", sql],
        capture_output=True, text=True, timeout=10,
    )
    counts = {}
    for line in out.stdout.strip().splitlines():
        if not line:
            continue
        status, count = line.split(",")
        counts[status] = int(count)
    return counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="tracemind-postgres-1")
    parser.add_argument("--db", default="tracemind")
    parser.add_argument("--user", default="tracemind")
    parser.add_argument("--interval", type=float, default=3.0)
    parser.add_argument("--max-duration", type=float, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-on-drain", action="store_true",
                         help="Exit as soon as PENDING hits 0 (Scenario 1 bulk-drain measurement). Omit for Scenario 2's continuous-observation runs, where staying near 0 the whole time is itself the finding.")
    args = parser.parse_args()

    rows = []
    start = time.monotonic()
    while time.monotonic() - start < args.max_duration:
        t = round(time.monotonic() - start, 2)
        counts = query_counts(args.container, args.db, args.user)
        row = {"t": t, "pending": counts.get("PENDING", 0), "published": counts.get("PUBLISHED", 0)}
        rows.append(row)
        print(row)
        if args.stop_on_drain and row["pending"] == 0 and len(rows) > 1:
            print("drained")
            break
        time.sleep(args.interval)

    if rows:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
