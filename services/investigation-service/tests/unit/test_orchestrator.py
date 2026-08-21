import asyncio
from datetime import datetime, timezone

import pytest

from investigation_service.ai.ai_investigation_service import AIInvestigationError
from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis
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


class FakeAIInvestigationServiceSuccess:
    def __init__(self):
        self.received_bundle = None

    async def investigate(self, evidence_bundle):
        self.received_bundle = evidence_bundle
        return RootCauseAnalysis(
            incident_id=evidence_bundle.incident_id,
            summary="s",
            probable_root_cause="p",
            confidence=0.9,
            supporting_evidence_ids=["E-M"],
            remediation_steps=["do the thing"],
        )


class FakeAIInvestigationServiceFailure:
    async def investigate(self, evidence_bundle):
        raise AIInvestigationError("TIMEOUT", "simulated timeout")


def make_request() -> InvestigationRequestedV1:
    now = datetime.now(timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-9", primary_service="svc",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"],
    )


def build_orchestrator(ai_service, metrics=None, logs=None, deps=None) -> InvestigationOrchestrator:
    return InvestigationOrchestrator(
        metrics_collector=metrics or FakeMetricsCollector(),
        logs_collector=logs or FakeLogsCollector(),
        dependency_collector=deps or FakeDependencyCollector(),
        aggregator=EvidenceAggregator(),
        ai_investigation_service=ai_service,
    )


@pytest.mark.asyncio
async def test_investigate_collects_evidence_and_completes_with_rca_on_ai_success():
    ai_service = FakeAIInvestigationServiceSuccess()
    orchestrator = build_orchestrator(ai_service)

    result = await orchestrator.investigate(make_request())

    assert result.incident_id == "INC-9"
    assert result.status == "COMPLETED"
    assert result.failure_reason is None
    assert result.root_cause_analysis is not None
    assert result.root_cause_analysis.incident_id == "INC-9"
    assert len(result.evidence.metrics) == 1
    assert len(result.evidence.logs) == 1
    assert len(result.evidence.dependencies) == 1
    # The AI service received the aggregated bundle, not the raw request.
    assert ai_service.received_bundle.incident_id == "INC-9"


@pytest.mark.asyncio
async def test_investigate_returns_failed_result_on_ai_failure_without_crashing():
    orchestrator = build_orchestrator(FakeAIInvestigationServiceFailure())

    result = await orchestrator.investigate(make_request())

    assert result.status == "FAILED"
    assert result.failure_reason == "TIMEOUT"
    assert result.root_cause_analysis is None
    # Evidence is still present even though AI reasoning failed (blueprint Section 31).
    assert len(result.evidence.metrics) == 1


@pytest.mark.asyncio
async def test_investigate_runs_collectors_concurrently():
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

    orchestrator = build_orchestrator(
        FakeAIInvestigationServiceSuccess(),
        metrics=SlowMetricsCollector(),
        logs=SlowLogsCollector(),
        deps=SlowDependencyCollector(),
    )

    await orchestrator.investigate(make_request())

    # If sequential, logs/deps would fully finish before metrics starts.
    # Concurrent execution means logs and deps finish while metrics is still sleeping.
    assert call_order.index("logs-end") < call_order.index("metrics-end")
    assert call_order.index("deps-end") < call_order.index("metrics-end")
