#!/usr/bin/env sh
# Explicit topic creation for local dev, per docs/architecture/engineering-blueprint.md
# Section 7. Topics are created explicitly rather than relying on broker
# auto-create so partition counts are documented, not incidental.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

# investigation.results.v1: the blueprint's canonical flow (Section 5, step 20)
# names this event "investigation.completed" without giving it an explicit
# topic name in Section 7's Kafka Architecture list - investigation-service
# (Milestone G) was built against the topic name "investigation.results.v1"
# as explicitly instructed, and this is flagged as a discrepancy to reconcile
# in the blueprint itself, not resolved unilaterally here.
for topic in signals.received.v1 investigation.requested.v1 investigation.results.v1; do
  "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done
