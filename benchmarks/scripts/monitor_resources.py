#!/usr/bin/env python3
"""Samples CPU%/RSS for given host PIDs and `docker stats` for given
container names, at a fixed interval, for a fixed duration. Run alongside a
load test (as a separate background process) and stop when the load test
stops. Writes CSV, not a running dashboard - this is a benchmark artifact,
not a monitoring tool.
"""
import argparse
import csv
import subprocess
import time


def sample_ps(pid: int) -> tuple[float, float] | None:
    try:
        out = subprocess.run(["ps", "-o", "%cpu,rss", "-p", str(pid)], capture_output=True, text=True, timeout=5)
        lines = out.stdout.strip().splitlines()
        if len(lines) < 2:
            return None
        cpu, rss_kb = lines[1].split()
        return float(cpu), float(rss_kb) / 1024  # MB
    except Exception:
        return None


def sample_docker(container: str) -> tuple[float, float] | None:
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}\t{{.MemUsage}}", container],
            capture_output=True, text=True, timeout=5,
        )
        line = out.stdout.strip()
        if not line:
            return None
        cpu_str, mem_str = line.split("\t")
        cpu = float(cpu_str.replace("%", ""))
        mem_used = mem_str.split("/")[0].strip()
        mem_mb = float(mem_used.replace("GiB", "").replace("MiB", "")) * (1024 if "GiB" in mem_used else 1)
        return cpu, mem_mb
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", action="append", default=[], help="label=pid, repeatable")
    parser.add_argument("--container", action="append", default=[], help="container name, repeatable")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    pids = []
    for p in args.pid:
        label, pid = p.split("=")
        pids.append((label, int(pid)))

    rows = []
    start = time.monotonic()
    while time.monotonic() - start < args.duration:
        t = round(time.monotonic() - start, 2)
        row = {"t": t}
        for label, pid in pids:
            sample = sample_ps(pid)
            row[f"{label}_cpu_pct"] = sample[0] if sample else ""
            row[f"{label}_rss_mb"] = round(sample[1], 1) if sample else ""
        for container in args.container:
            sample = sample_docker(container)
            row[f"{container}_cpu_pct"] = sample[0] if sample else ""
            row[f"{container}_mem_mb"] = round(sample[1], 1) if sample else ""
        rows.append(row)
        time.sleep(args.interval)

    if rows:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
