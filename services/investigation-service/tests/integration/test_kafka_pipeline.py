import asyncio
import json
import time

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer

from investigation_service.collectors.stub import StubDependencyCollector, StubLogsCollector, StubMetricsCollector
from investigation_service.evidence.aggregator import EvidenceAggregator
from investigation_service.kafka.consumer import InvestigationRequestConsumer
from investigation_service.kafka.publisher import InvestigationResultPublisher
from investigation_service.orchestration.orchestrator import InvestigationOrchestrator

REQUESTED_TOPIC = "investigation.requested.v1"
RESULTS_TOPIC = "investigation.results.v1"


@pytest.fixture(scope="module")
def kafka_container():
    # testcontainers' KafkaContainer injects a Confluent-image-specific start
    # script, so the apache/kafka image used elsewhere in this project (see
    # docker-compose.yml) isn't compatible with this wrapper - use its
    # supported default instead. Independent, lower-stakes choice: this test
    # just needs a real broker, not the same image as production.
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture
async def bootstrap_servers(kafka_container):
    servers = kafka_container.get_bootstrap_server()
    admin = AIOKafkaAdminClient(bootstrap_servers=servers)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(REQUESTED_TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(RESULTS_TOPIC, num_partitions=1, replication_factor=1),
        ])
    finally:
        await admin.close()
    return servers


@pytest.mark.asyncio
async def test_investigation_requested_produces_stub_result_on_results_topic(bootstrap_servers):
    consumer = AIOKafkaConsumer(
        REQUESTED_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id="investigation-service-test",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await consumer.start()
    await producer.start()

    try:
        orchestrator = InvestigationOrchestrator(
            metrics_collector=StubMetricsCollector(),
            logs_collector=StubLogsCollector(),
            dependency_collector=StubDependencyCollector(),
            aggregator=EvidenceAggregator(),
        )
        publisher = InvestigationResultPublisher(producer, RESULTS_TOPIC)
        pipeline = InvestigationRequestConsumer(consumer, orchestrator, publisher)
        pipeline_task = asyncio.create_task(pipeline.run())

        # Same shape and casing Java's OutboxPublisher actually produces, including
        # epoch-seconds-with-fractional-nanos Instant serialization, not ISO-8601.
        now_epoch = time.time()
        request_payload = {
            "eventId": "evt-test-1",
            "schemaVersion": "1.0",
            "incidentId": "INC-42",
            "primaryService": "payment-service",
            "environment": "prod",
            "severity": "CRITICAL",
            "firstObservedAt": now_epoch,
            "lastObservedAt": now_epoch,
            "triggerSignalIds": ["evt-1"],
        }

        result_consumer = AIOKafkaConsumer(
            RESULTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id="test-result-reader",
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await result_consumer.start()
        try:
            await producer.send_and_wait(
                REQUESTED_TOPIC,
                key=b"INC-42",
                value=json.dumps(request_payload).encode("utf-8"),
            )

            record = await asyncio.wait_for(result_consumer.getone(), timeout=15)

            assert record.key == b"INC-42"
            result = json.loads(record.value)
            assert result["incidentId"] == "INC-42"
            assert result["status"] == "EVIDENCE_COLLECTED"
            assert len(result["evidence"]["metrics"]) == 1
            assert len(result["evidence"]["logs"]) == 1
            assert len(result["evidence"]["dependencies"]) == 1
            assert result["evidence"]["incidentId"] == "INC-42"
        finally:
            await result_consumer.stop()
    finally:
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await producer.stop()
