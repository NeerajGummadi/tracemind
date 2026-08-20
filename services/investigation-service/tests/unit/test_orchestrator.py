from datetime import datetime, timezone

import pytest

from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.evidence.aggregator import EvidenceAggregator
from investigation_service.orchestration.orchestrator import InvestigationOrchestrator


class FakeMetricsCollector:
    async def collect(self, request):
        return [MetricEvidence(evidence_id="E-M", entity="x", fact="f", observed_at=request.last_observed_at, value=1.0, unit="u")]


class FakeLogsCollector:
    async def collect(self, request):
        return [LogEvidence(evidence_id="E-L", entity="x", fact="f", observed_at=request.last_observed_at, occurrences=1)]


class FakeDependencyCollector:
    async def collect(self, request):
        return [DependencyEvidence(evidence_id="E-D", entity="x", fact="f", observed_at=request.last_observed_at, depends_on="y")]


def make_request() -> InvestigationRequestedV1:
    now = datetime.now(timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-9", primary_service="svc",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"],
    )


@pytest.mark.asyncio
async def test_investigate_runs_all_collectors_and_produces_stub_result():
    orchestrator = InvestigationOrchestrator(
        metrics_collector=FakeMetricsCollector(),
        logs_collector=FakeLogsCollector(),
        dependency_collector=FakeDependencyCollector(),
        aggregator=EvidenceAggregator(),
    )

    result = await orchestrator.investigate(make_request())

    assert result.incident_id == "INC-9"
    assert result.status == "EVIDENCE_COLLECTED"
    assert len(result.evidence.metrics) == 1
    assert len(result.evidence.logs) == 1
    assert len(result.evidence.dependencies) == 1
    assert result.evidence.incident_id == "INC-9"


@pytest.mark.asyncio
async def test_investigate_runs_collectors_concurrently():
    import asyncio

    call_order = []

    class SlowMetricsCollector:
        async def collect(self, request):
            call_order.append("metrics-start")
            await asyncio.sleep(0.05)
            call_order.append("metrics-end")
            return []

    class SlowLogsCollector:
        async def collect(self, request):
            call_order.append("logs-start")
            await asyncio.sleep(0.01)
            call_order.append("logs-end")
            return []

    class SlowDependencyCollector:
        async def collect(self, request):
            call_order.append("deps-start")
            await asyncio.sleep(0.01)
            call_order.append("deps-end")
            return []

    orchestrator = InvestigationOrchestrator(
        metrics_collector=SlowMetricsCollector(),
        logs_collector=SlowLogsCollector(),
        dependency_collector=SlowDependencyCollector(),
        aggregator=EvidenceAggregator(),
    )

    await orchestrator.investigate(make_request())

    # If sequential, logs/deps would fully finish before metrics starts.
    # Concurrent execution means logs and deps finish while metrics is still sleeping.
    assert call_order.index("logs-end") < call_order.index("metrics-end")
    assert call_order.index("deps-end") < call_order.index("metrics-end")
