#!/usr/bin/env python3
"""Open-loop HTTP load generator for POST /integrations/prometheus/alerts.

Open-loop = requests are scheduled at fixed target-rate intervals regardless
of how fast responses come back (a real alert source doesn't slow down
because your server is slow) - this is what makes "offered RPS" meaningful,
unlike a closed-loop "N workers looping" generator where achieved throughput
is capped by concurrency rather than by the target rate.

Deterministic service/environment cardinality (Milestone N clarification 3):
services are named service-0000..service-{pool_size-1} and rotated by
request index, so Kafka's environment:service partition key gets realistic
spread instead of collapsing onto one hot incident/partition. Test D
(coalescing stress) is the deliberate exception and does not use this script
in its default mode - see investigation_lifecycle_stress.py.

Does not persist full per-request logs (avoids "enormous raw logs" per the
milestone's Result Artifacts instruction) - only a summary (percentiles,
error/status breakdown) plus a bounded raw sample are written to the output
JSON. eventIds are read back from each response body only when
--capture-event-ids is passed (needed for Test C's correctness check) since
parsing the response body adds generator-side overhead that would skew a
pure-throughput run.
"""
import argparse
import asyncio
import hashlib
import json
import statistics
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

RAW_SAMPLE_CAP = 2000


def build_payload(fingerprint: str, starts_at: str, service: str, environment: str) -> dict:
    return {
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "DB_CONNECTION_PRESSURE",
                    "service": service,
                    "environment": environment,
                    "severity": "CRITICAL",
                    "instance": f"{service}-1",
                },
                "annotations": {"summary": "synthetic benchmark alert"},
                "startsAt": starts_at,
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "http://prometheus/graph",
                "fingerprint": fingerprint,
            }
        ],
    }


class RequestPlan:
    """Precomputes every request's (fingerprint, startsAt, service) before
    the run starts, so the hot send loop does no bookkeeping beyond firing
    HTTP calls - keeps generator-side overhead out of the measured latency.

    index_offset lets multiple workers each generate a disjoint slice of one
    logical plan while still rotating through the service pool using the
    GLOBAL request index, so the combined multi-worker traffic has the same
    deterministic service/environment distribution as a single-process run
    would have produced. duplicate_rate sampling is per-worker (each worker
    keeps its own small duplicate pool) - fine for Test C's purposes, since
    the point is a controlled aggregate duplicate rate, not cross-worker
    duplicate pairs specifically."""

    def __init__(self, count: int, run_id: str, service_pool_size: int, environments: list[str],
                 duplicate_rate: float, index_offset: int = 0, seed_duplicates_from: int = 50,
                 service_prefix: str = "service", fixed_duplicate_pool_size: int = 0):
        self._service_pool_size = service_pool_size
        self._service_prefix = service_prefix
        self._environments = environments
        self.items: list[tuple[str, str, str, str]] = []  # (fingerprint, startsAt, service, environment)

        base_time = datetime.now(timezone.utc)

        if fixed_duplicate_pool_size > 0:
            # "Concentrated retry burst" mode (Test C scenario 3): every request
            # reuses one of a small, FIXED, pre-generated set of K
            # (fingerprint, startsAt) pairs - not a probabilistic duplicate rate
            # against a growing pool, but the same handful of identical alerts
            # hammered repeatedly, exactly like a real retry storm.
            fixed_pool = []
            for k in range(fixed_duplicate_pool_size):
                service = f"{service_prefix}-{k:04d}"
                environment = environments[k % len(environments)]
                fingerprint = f"burst-{run_id}-{k}"
                starts_at = base_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                fixed_pool.append((fingerprint, starts_at, service, environment))
            for local_i in range(count):
                self.items.append(fixed_pool[local_i % fixed_duplicate_pool_size])
            return

        # Evenly-spaced reuse (every Nth request, N = round(1/duplicate_rate)) rather than a
        # "first 1000*rate reused, rest new" block pattern - the block version is lumpy (near-100%
        # duplicate for a stretch, then near-0%) and undercounts entirely for small request counts
        # that never reach the 1000-block boundary. This gives a uniform interleave and an exact
        # achieved rate, verified against the target in the idempotency summary.
        reuse_period = max(1, round(1 / duplicate_rate)) if duplicate_rate > 0 else 0
        seen_pool: list[tuple[str, str, str, str]] = []
        for local_i in range(count):
            i = index_offset + local_i
            reuse = duplicate_rate > 0 and len(seen_pool) >= seed_duplicates_from and (local_i % reuse_period) == 0
            if reuse:
                item = seen_pool[local_i % len(seen_pool)]
            else:
                service = f"{self._service_prefix}-{i % service_pool_size:04d}"
                environment = environments[i % len(environments)]
                fingerprint = f"bench-{run_id}-{i}"
                starts_at = (base_time + timedelta(microseconds=i)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                item = (fingerprint, starts_at, service, environment)
                seen_pool.append(item)
            self.items.append(item)


async def send_one(client: httpx.AsyncClient, url: str, item: tuple[str, str, str, str],
                    capture_event_ids: bool, results: list, sem: asyncio.Semaphore,
                    timing_records: list | None = None):
    fingerprint, starts_at, service, environment = item
    payload = build_payload(fingerprint, starts_at, service, environment)
    async with sem:
        send_epoch = time.time()
        start = time.monotonic()
        try:
            resp = await client.post(url, json=payload)
            latency_ms = (time.monotonic() - start) * 1000
            event_ids = None
            if capture_event_ids and resp.status_code == 202:
                try:
                    event_ids = resp.json().get("eventIds")
                    if timing_records is not None and event_ids:
                        for eid in event_ids:
                            timing_records.append((eid, send_epoch))
                except Exception:
                    event_ids = None
            results.append((latency_ms, resp.status_code, fingerprint, starts_at, event_ids))
        except httpx.HTTPError as e:
            latency_ms = (time.monotonic() - start) * 1000
            results.append((latency_ms, -1, fingerprint, starts_at, None, str(e)))


async def run_load_raw(base_url: str, rps: float, count: int, run_id: str, service_pool_size: int,
                        environments: list[str], duplicate_rate: float, max_in_flight: int,
                        capture_event_ids: bool, index_offset: int, service_prefix: str,
                        fixed_duplicate_pool_size: int = 0, duplicate_seed_count: int = 50) -> tuple[list[tuple], float]:
    """One worker's share of the load: sends `count` requests at `rps`,
    returns raw per-request results plus this worker's own wall-clock
    elapsed. Percentiles must be computed AFTER merging all workers' raw
    results, never by averaging each worker's own percentiles."""
    url = f"{base_url}/integrations/prometheus/alerts"
    plan = RequestPlan(count, run_id, service_pool_size, environments, duplicate_rate, index_offset,
                        service_prefix=service_prefix, fixed_duplicate_pool_size=fixed_duplicate_pool_size,
                        seed_duplicates_from=duplicate_seed_count)

    results: list[tuple] = []
    sem = asyncio.Semaphore(max_in_flight)
    interval = 1.0 / rps if rps > 0 else 0

    limits = httpx.Limits(max_connections=max_in_flight * 2, max_keepalive_connections=max_in_flight)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        wall_start = time.monotonic()
        tasks = []
        for i, item in enumerate(plan.items):
            target_send_time = wall_start + i * interval
            now = time.monotonic()
            if target_send_time > now:
                await asyncio.sleep(target_send_time - now)
            tasks.append(asyncio.create_task(send_one(client, url, item, capture_event_ids, results, sem)))
        await asyncio.gather(*tasks)
        wall_elapsed = time.monotonic() - wall_start

    return results, wall_elapsed


def worker_entrypoint(worker_args: dict) -> tuple[list[tuple], float]:
    """Top-level (picklable) entrypoint run in each worker process - each
    worker gets its own event loop via asyncio.run()."""
    return asyncio.run(run_load_raw(**worker_args))


def run_load(base_url: str, rps: float, duration_s: float, run_id: str, service_pool_size: int,
             environments: list[str], duplicate_rate: float, max_in_flight: int,
             capture_event_ids: bool, workers: int, service_prefix: str = "service",
             fixed_duplicate_pool_size: int = 0, duplicate_seed_count: int = 50) -> dict:
    """Partitions the target rate/count across `workers` OS processes
    (bypassing the GIL/single-event-loop ceiling one process hits well
    below 1000 req/s - see Test A's 1000 RPS diagnostic) and merges their
    raw results before computing percentiles."""
    total_count = int(rps * duration_s)
    per_worker_counts = [total_count // workers] * workers
    for i in range(total_count % workers):
        per_worker_counts[i] += 1
    per_worker_rps = rps / workers
    per_worker_in_flight = max(1, max_in_flight // workers)

    worker_args_list = []
    offset = 0
    for w, wcount in enumerate(per_worker_counts):
        worker_args_list.append({
            "base_url": base_url, "rps": per_worker_rps, "count": wcount,
            "run_id": f"{run_id}-w{w}", "service_pool_size": service_pool_size,
            "environments": environments, "duplicate_rate": duplicate_rate,
            "max_in_flight": per_worker_in_flight, "capture_event_ids": capture_event_ids,
            "index_offset": offset, "service_prefix": service_prefix,
            "fixed_duplicate_pool_size": fixed_duplicate_pool_size,
            "duplicate_seed_count": duplicate_seed_count,
        })
        offset += wcount

    if workers == 1:
        results, wall_elapsed = worker_entrypoint(worker_args_list[0])
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            pool_start = time.monotonic()
            worker_outputs = list(pool.map(worker_entrypoint, worker_args_list))
            wall_elapsed = time.monotonic() - pool_start
        results = [r for worker_results, _ in worker_outputs for r in worker_results]

    return summarize(results, rps, total_count, wall_elapsed, capture_event_ids)


def summarize(results: list[tuple], offered_rps: float, offered_count: int, wall_elapsed: float,
              capture_event_ids: bool) -> dict:
    latencies = sorted(r[0] for r in results)
    statuses = [r[1] for r in results]
    status_hist: dict[str, int] = {}
    for s in statuses:
        key = str(s) if s != -1 else "CONNECTION_ERROR"
        status_hist[key] = status_hist.get(key, 0) + 1

    success = sum(1 for s in statuses if s == 202)
    error_rate = 1 - (success / len(statuses)) if statuses else None

    def pct(p):
        if not latencies:
            return None
        idx = min(len(latencies) - 1, int(len(latencies) * p))
        return round(latencies[idx], 2)

    summary = {
        "offered_rps": offered_rps,
        "offered_count": offered_count,
        "completed_count": len(results),
        "achieved_rps": round(len(results) / wall_elapsed, 2) if wall_elapsed > 0 else None,
        "wall_elapsed_s": round(wall_elapsed, 3),
        "latency_ms": {
            "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99),
            "max": round(latencies[-1], 2) if latencies else None,
            "mean": round(statistics.mean(latencies), 2) if latencies else None,
        },
        "error_rate": round(error_rate, 4) if error_rate is not None else None,
        "status_distribution": status_hist,
    }

    raw_sample = results[:RAW_SAMPLE_CAP]
    summary["raw_sample"] = [
        {"latency_ms": round(r[0], 2), "status": r[1]} for r in raw_sample
    ]

    if capture_event_ids:
        sent_pairs = [(r[2], r[3]) for r in results]
        returned_event_ids = [eid for r in results if r[4] for eid in r[4]]
        summary["idempotency"] = {
            "requests_sent": len(sent_pairs),
            "distinct_fingerprint_startsat_pairs_sent": len(set(sent_pairs)),
            "event_ids_returned": len(returned_event_ids),
            "distinct_event_ids_returned": len(set(returned_event_ids)),
        }

    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8081")
    parser.add_argument("--rps", type=float, required=True)
    parser.add_argument("--duration", type=float, required=True, help="seconds")
    parser.add_argument("--run-id", default=str(int(time.time())))
    parser.add_argument("--service-pool-size", type=int, default=100)
    parser.add_argument("--service-prefix", default="service",
                         help="Give each test tier a distinct prefix to avoid coalescing into a prior tier's still-open incidents within the 5-minute correlation window")
    parser.add_argument("--environments", default="prod,staging")
    parser.add_argument("--duplicate-rate", type=float, default=0.0, help="0.0-1.0")
    parser.add_argument("--fixed-duplicate-pool-size", type=int, default=0,
                         help="Test C scenario 3: replay only this many distinct events, over and over (concentrated retry burst) - overrides --duplicate-rate")
    parser.add_argument("--duplicate-seed-count", type=int, default=50,
                         help="Requests sent before --duplicate-rate starts injecting reuse - must be well below total request count or no duplicates will appear")
    parser.add_argument("--max-in-flight", type=int, default=500)
    parser.add_argument("--capture-event-ids", action="store_true")
    parser.add_argument("--workers", type=int, default=1,
                         help="OS processes to spread request generation across - see README note on single-process ceiling")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    environments = args.environments.split(",")
    summary = run_load(
        args.base_url, args.rps, args.duration, args.run_id, args.service_pool_size,
        environments, args.duplicate_rate, args.max_in_flight, args.capture_event_ids, args.workers,
        args.service_prefix, args.fixed_duplicate_pool_size, args.duplicate_seed_count,
    )
    summary["config"] = vars(args)

    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps({k: v for k, v in summary.items() if k != "raw_sample"}, indent=2))


if __name__ == "__main__":
    main()
