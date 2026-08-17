#!/usr/bin/env sh
# Explicit topic creation for local dev, per docs/architecture/engineering-blueprint.md
# Section 7. Topics are created explicitly rather than relying on broker
# auto-create so partition counts are documented, not incidental.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

for topic in signals.received.v1 investigation.requested.v1; do
  "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done
