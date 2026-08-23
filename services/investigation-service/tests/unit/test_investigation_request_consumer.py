from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.investigation_result import InvestigationResult
from investigation_service.kafka.consumer import InvestigationRequestConsumer


@dataclass
class FakeRecord:
    value: bytes
    offset: int


class FakeConsumer:
    """Minimal async-iterable + manual-commit stand-in for AIOKafkaConsumer."""

    def __init__(self, records: list[FakeRecord]):
        self._records = records
        self.commit_count = 0

    def __aiter__(self):
        return self._iterator()

    async def _iterator(self):
        for record in self._records:
            yield record

    async def commit(self):
        self.commit_count += 1


class FakeOrchestrator:
    def __init__(self):
        self.calls: list[str] = []

    async def investigate(self, request: InvestigationRequestedV1) -> InvestigationResult:
        self.calls.append(request.investigation_run_id)
        return InvestigationResult(
            incident_id=request.incident_id,
            investigation_run_id=request.investigation_run_id,
            status="COMPLETED",
            evidence=EvidenceBundle(
                incident_id=request.incident_id, metrics=[], logs=[], dependencies=[],
                collected_at=datetime.now(timezone.utc),
            ),
            generated_at=datetime.now(timezone.utc),
        )


class FakePublisher:
    def __init__(self):
        self.published: list[InvestigationResult] = []

    async def publish(self, result: InvestigationResult) -> None:
        self.published.append(result)


def make_request(investigation_run_id: str) -> InvestigationRequestedV1:
    now = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-1", primary_service="payment-service",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"], investigation_run_id=investigation_run_id, input_signal_version=1,
    )


@pytest.mark.asyncio
async def test_single_request_investigates_once_and_commits():
    request = make_request("run-1")
    records = [FakeRecord(value=request.model_dump_json(by_alias=True).encode(), offset=0)]
    consumer = FakeConsumer(records)
    orchestrator = FakeOrchestrator()
    publisher = FakePublisher()

    await InvestigationRequestConsumer(consumer, orchestrator, publisher).run()

    assert orchestrator.calls == ["run-1"]
    assert len(publisher.published) == 1
    assert consumer.commit_count == 1


@pytest.mark.asyncio
async def test_duplicate_investigation_run_id_does_not_trigger_a_second_ai_call():
    request = make_request("run-1")
    payload = request.model_dump_json(by_alias=True).encode()
    # Same investigationRunId delivered twice - simulates a Kafka redelivery
    # (e.g. publish succeeded but the following commit failed/timed out).
    records = [FakeRecord(value=payload, offset=0), FakeRecord(value=payload, offset=1)]
    consumer = FakeConsumer(records)
    orchestrator = FakeOrchestrator()
    publisher = FakePublisher()

    await InvestigationRequestConsumer(consumer, orchestrator, publisher).run()

    assert orchestrator.calls == ["run-1"]
    assert len(publisher.published) == 1
    # Both deliveries are still acknowledged, so the duplicate doesn't loop forever redelivering.
    assert consumer.commit_count == 2


@pytest.mark.asyncio
async def test_distinct_investigation_run_ids_both_investigate():
    payload1 = make_request("run-1").model_dump_json(by_alias=True).encode()
    payload2 = make_request("run-2").model_dump_json(by_alias=True).encode()
    records = [FakeRecord(value=payload1, offset=0), FakeRecord(value=payload2, offset=1)]
    consumer = FakeConsumer(records)
    orchestrator = FakeOrchestrator()
    publisher = FakePublisher()

    await InvestigationRequestConsumer(consumer, orchestrator, publisher).run()

    assert orchestrator.calls == ["run-1", "run-2"]
    assert len(publisher.published) == 2
