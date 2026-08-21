import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer

from investigation_service.ai.ai_investigation_service import AIInvestigationService
from investigation_service.ai.prompt_builder import PromptBuilder
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


async def consume_until_key(consumer: AIOKafkaConsumer, expected_key: bytes, timeout_seconds: float = 15) -> dict:
    """Both integration tests share one Kafka container (module-scoped) and
    therefore one results topic - a fresh consumer group with earliest offset
    sees every prior test's messages too, so this must filter by key rather
    than assume the first message read is the relevant one."""
    async def _find():
        async for record in consumer:
            if record.key == expected_key:
                return json.loads(record.value)
    return await asyncio.wait_for(_find(), timeout=timeout_seconds)


def mock_openai_client_returning(rca_json: dict) -> MagicMock:
    """Never calls the real OpenAI API - this proves the pipeline plumbing
    (prompt building, response validation, orchestration, publishing) works
    end-to-end without depending on network access or a real API key."""
    client = MagicMock()
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(rca_json)))]
    )
    client.chat.completions.create = AsyncMock(return_value=fake_response)
    return client


@pytest.mark.asyncio
async def test_investigation_requested_flows_through_ai_reasoning_to_published_result(bootstrap_servers):
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
        # Cites exactly the evidence IDs the stub collectors deterministically
        # produce for incident INC-42 (see StubMetricsCollector etc.), so the
        # AIInvestigationService's evidence-grounding check passes.
        mock_rca = {
            "incidentId": "INC-42",
            "summary": "Database connection pool exhaustion",
            "probableRootCause": "Slow queries exhausted the connection pool",
            "confidence": 0.85,
            "supportingEvidenceIds": ["E-INC-42-METRIC-1", "E-INC-42-LOG-1"],
            "remediationSteps": ["Inspect slow queries", "Increase pool size"],
        }
        ai_investigation_service = AIInvestigationService(
            client=mock_openai_client_returning(mock_rca),
            prompt_builder=PromptBuilder(),
            model="gpt-4o-mini",
            temperature=0.0,
            max_output_tokens=1000,
        )
        orchestrator = InvestigationOrchestrator(
            metrics_collector=StubMetricsCollector(),
            logs_collector=StubLogsCollector(),
            dependency_collector=StubDependencyCollector(),
            aggregator=EvidenceAggregator(),
            ai_investigation_service=ai_investigation_service,
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

            result = await consume_until_key(result_consumer, b"INC-42")

            assert result["incidentId"] == "INC-42"
            assert result["status"] == "COMPLETED"
            assert result["failureReason"] is None
            assert result["rootCauseAnalysis"]["probableRootCause"] == "Slow queries exhausted the connection pool"
            assert result["rootCauseAnalysis"]["confidence"] == 0.85
            assert result["rootCauseAnalysis"]["supportingEvidenceIds"] == ["E-INC-42-METRIC-1", "E-INC-42-LOG-1"]
            # Evidence is still published alongside the RCA, not replaced by it.
            assert len(result["evidence"]["metrics"]) == 1
            assert len(result["evidence"]["logs"]) == 1
            assert len(result["evidence"]["dependencies"]) == 1
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


@pytest.mark.asyncio
async def test_ai_failure_still_publishes_a_failed_result_with_evidence(bootstrap_servers):
    consumer = AIOKafkaConsumer(
        REQUESTED_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id="investigation-service-test-failure",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await consumer.start()
    await producer.start()

    try:
        broken_client = MagicMock()
        broken_client.chat.completions.create = AsyncMock(
            return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="not json"))])
        )
        ai_investigation_service = AIInvestigationService(
            client=broken_client, prompt_builder=PromptBuilder(), model="gpt-4o-mini",
            temperature=0.0, max_output_tokens=1000,
        )
        orchestrator = InvestigationOrchestrator(
            metrics_collector=StubMetricsCollector(),
            logs_collector=StubLogsCollector(),
            dependency_collector=StubDependencyCollector(),
            aggregator=EvidenceAggregator(),
            ai_investigation_service=ai_investigation_service,
        )
        publisher = InvestigationResultPublisher(producer, RESULTS_TOPIC)
        pipeline = InvestigationRequestConsumer(consumer, orchestrator, publisher)
        pipeline_task = asyncio.create_task(pipeline.run())

        now_epoch = time.time()
        request_payload = {
            "eventId": "evt-test-2",
            "schemaVersion": "1.0",
            "incidentId": "INC-43",
            "primaryService": "payment-service",
            "environment": "prod",
            "severity": "CRITICAL",
            "firstObservedAt": now_epoch,
            "lastObservedAt": now_epoch,
            "triggerSignalIds": ["evt-2"],
        }

        result_consumer = AIOKafkaConsumer(
            RESULTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id="test-result-reader-failure",
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await result_consumer.start()
        try:
            await producer.send_and_wait(
                REQUESTED_TOPIC,
                key=b"INC-43",
                value=json.dumps(request_payload).encode("utf-8"),
            )

            result = await consume_until_key(result_consumer, b"INC-43")

            assert result["status"] == "FAILED"
            assert result["failureReason"] == "MALFORMED_RESPONSE"
            assert result["rootCauseAnalysis"] is None
            # The investigation didn't crash, and evidence is still preserved.
            assert len(result["evidence"]["metrics"]) == 1
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
