import asyncio
from datetime import datetime, timezone

import pytest

from investigation_service.ai.ai_investigation_service import AICallMetrics, AIInvestigationError, AIInvestigationOutcome
from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis
from investigation_service.evidence.aggregator import EvidenceAggregator
from investigation_service.observability import cost_estimator
from investigation_service.observability.cost_estimator import ModelPricing
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
        rca = RootCauseAnalysis(
            incident_id=evidence_bundle.incident_id,
            summary="s",
            probable_root_cause="p",
            confidence=0.9,
            supporting_evidence_ids=["E-M"],
            remediation_steps=["do the thing"],
        )
        metrics = AICallMetrics(
            ai_latency_ms=42.0,
            model_requested="gpt-4o-mini",
            model_returned="gpt-4o-mini-2024-07-18",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
        )
        return AIInvestigationOutcome(rca=rca, metrics=metrics)


class FakeAIInvestigationServiceFailure:
    async def investigate(self, evidence_bundle):
        raise AIInvestigationError(
            "TIMEOUT", "simulated timeout",
            AICallMetrics(ai_latency_ms=30.0, model_requested="gpt-4o-mini"),
        )


def make_request() -> InvestigationRequestedV1:
    now = datetime.now(timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-9", primary_service="svc",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"], investigation_run_id="run-1", input_signal_version=1,
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


@pytest.mark.asyncio
async def test_total_investigation_duration_is_captured():
    orchestrator = build_orchestrator(FakeAIInvestigationServiceSuccess())

    result = await orchestrator.investigate(make_request())

    assert result.metrics is not None
    assert result.metrics.total_duration_ms is not None
    assert result.metrics.total_duration_ms >= 0


@pytest.mark.asyncio
async def test_evidence_collection_duration_reflects_wall_clock_not_sum_of_parts():
    class SlowMetricsCollector:
        async def collect(self, request):
            await asyncio.sleep(0.05)
            return []

    class SlowLogsCollector:
        async def collect(self, request):
            await asyncio.sleep(0.05)
            return []

    class SlowDependencyCollector:
        async def collect(self, request):
            await asyncio.sleep(0.05)
            return []

    orchestrator = build_orchestrator(
        FakeAIInvestigationServiceSuccess(),
        metrics=SlowMetricsCollector(),
        logs=SlowLogsCollector(),
        deps=SlowDependencyCollector(),
    )

    result = await orchestrator.investigate(make_request())

    # Three ~50ms collectors run concurrently: total should be close to 50ms,
    # nowhere near the ~150ms it would be if they ran sequentially/summed.
    assert result.metrics.evidence_collection_duration_ms is not None
    assert result.metrics.evidence_collection_duration_ms < 120


@pytest.mark.asyncio
async def test_ai_metrics_are_propagated_into_investigation_metrics_on_success():
    orchestrator = build_orchestrator(FakeAIInvestigationServiceSuccess())

    result = await orchestrator.investigate(make_request())

    assert result.metrics.open_ai_latency_ms == 42.0
    assert result.metrics.prompt_tokens == 100
    assert result.metrics.completion_tokens == 50
    assert result.metrics.total_tokens == 150
    assert result.metrics.model == "gpt-4o-mini-2024-07-18"  # prefers model_returned over model_requested


@pytest.mark.asyncio
async def test_failed_result_retains_available_timing_metrics():
    orchestrator = build_orchestrator(FakeAIInvestigationServiceFailure())

    result = await orchestrator.investigate(make_request())

    assert result.status == "FAILED"
    assert result.metrics is not None
    assert result.metrics.total_duration_ms is not None
    assert result.metrics.evidence_collection_duration_ms is not None
    assert result.metrics.open_ai_latency_ms == 30.0
    # No response was ever received for a TIMEOUT, so no token counts.
    assert result.metrics.prompt_tokens is None
    assert result.metrics.model == "gpt-4o-mini"  # falls back to model_requested


@pytest.mark.asyncio
async def test_estimated_cost_is_populated_when_pricing_is_configured(monkeypatch):
    monkeypatch.setitem(
        cost_estimator.MODEL_PRICING,
        "gpt-4o-mini-2024-07-18",
        ModelPricing(input_cost_per_million_tokens=1.0, output_cost_per_million_tokens=2.0),
    )
    orchestrator = build_orchestrator(FakeAIInvestigationServiceSuccess())

    result = await orchestrator.investigate(make_request())

    # 100 prompt tokens * $1/1M + 50 completion tokens * $2/1M
    assert result.metrics.estimated_api_cost_usd == pytest.approx(0.0001 + 0.0001)


@pytest.mark.asyncio
async def test_estimated_cost_is_none_when_pricing_is_unconfigured():
    orchestrator = build_orchestrator(FakeAIInvestigationServiceSuccess())

    result = await orchestrator.investigate(make_request())

    # No entry for "gpt-4o-mini-2024-07-18" in MODEL_PRICING by default.
    assert result.metrics.estimated_api_cost_usd is None
