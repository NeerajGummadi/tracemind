#!/usr/bin/env python3
"""Test B: webhook-acceptance-to-durable-signal-persistence latency.

Runs a modest, single-process, sustainable-rate load (well within what one
process can generate - see Test A's load-generator finding) while recording
(event_id, send_epoch_seconds) for every accepted request. Prints these as
CSV to stdout; a follow-up psql query joins against signals.created_at to
compute the actual delta distribution. This is deliberately a separate,
simple script rather than added complexity in load_connector.py's
multiprocess path, since it only needs to run at rates a single process
already handles cleanly.
"""
import argparse
import asyncio
import json
import time

import httpx

from load_connector import RequestPlan, build_payload


async def main_async(base_url: str, rps: float, duration_s: float, run_id: str,
                      service_pool_size: int, service_prefix: str, output: str):
    url = f"{base_url}/integrations/prometheus/alerts"
    count = int(rps * duration_s)
    plan = RequestPlan(count, run_id, service_pool_size, ["prod", "staging"], 0.0,
                        service_prefix=service_prefix)
    interval = 1.0 / rps
    records = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        wall_start = time.monotonic()
        for i, (fingerprint, starts_at, service, environment) in enumerate(plan.items):
            target = wall_start + i * interval
            now = time.monotonic()
            if target > now:
                await asyncio.sleep(target - now)
            send_epoch = time.time()
            payload = build_payload(fingerprint, starts_at, service, environment)
            resp = await client.post(url, json=payload)
            if resp.status_code == 202:
                for eid in resp.json().get("eventIds", []):
                    records.append((eid, send_epoch))

    with open(output, "w") as f:
        for eid, epoch in records:
            f.write(f"{eid},{epoch}\n")
    print(f"wrote {len(records)} records to {output}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8081")
    parser.add_argument("--rps", type=float, default=200)
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--run-id", default="persistlat")
    parser.add_argument("--service-pool-size", type=int, default=200)
    parser.add_argument("--service-prefix", default="svcplat")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    asyncio.run(main_async(args.base_url, args.rps, args.duration, args.run_id,
                            args.service_pool_size, args.service_prefix, args.output))


if __name__ == "__main__":
    main()
