#!/usr/bin/env python3
"""Test B's isolation strategy: a throwaway consumer for
investigation.requested.v1 that does nothing but count/timestamp receipts
and commit offsets - the real investigation-service is not run at all, so
no OpenAI calls can occur no matter how many incidents Test B's traffic
creates. This measures Connector -> Kafka -> Incident Service -> Postgres ->
Outbox -> investigation.requested.v1 publish, cleanly stopping at that
topic boundary, with zero changes to any production code.

Uses a fresh consumer group with auto_offset_reset=latest so it only counts
messages produced AFTER it starts - any pre-existing backlog on the topic
(from earlier milestones or Test A's own incident-creation side effects)
is deliberately excluded, not drained into this measurement.
"""
import argparse
import csv
import time

from aiokafka import AIOKafkaConsumer
import asyncio


async def run(bootstrap_servers: str, topic: str, group_id: str, duration_s: float, output_csv: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    rows = []
    received = 0
    start = time.monotonic()
    last_commit = start
    try:
        while time.monotonic() - start < duration_s:
            # getmany()'s own timeout_ms already returns (possibly empty) rather than
            # raising - wrapping it in an outer asyncio.wait_for risks cancelling a
            # mid-flight fetch, which can silently drop records from this count while
            # aiokafka's internal position still advances (confirmed via Kafka's own
            # offset/lag reporting during Test B's 700 RPS tier - see benchmark-results.md).
            batch = await consumer.getmany(timeout_ms=500, max_records=500)
            batch_count = sum(len(records) for records in batch.values())
            received += batch_count
            now = time.monotonic()
            if batch_count or (now - last_commit) > 1.0:
                await consumer.commit()
                last_commit = now
            rows.append({"t": round(now - start, 2), "received_total": received, "batch_size": batch_count})
    finally:
        await consumer.commit()
        await consumer.stop()

    if rows:
        with open(output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    elapsed = time.monotonic() - start
    print(f"received={received} elapsed_s={elapsed:.2f} rate={received/elapsed:.2f}/s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="investigation.requested.v1")
    parser.add_argument("--group-id", default="benchmark-sink")
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    asyncio.run(run(args.bootstrap_servers, args.topic, args.group_id, args.duration, args.output))


if __name__ == "__main__":
    main()
