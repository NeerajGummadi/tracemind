from datetime import datetime, timezone

import pytest

from investigation_service.collectors.stub import StubDependencyCollector, StubLogsCollector, StubMetricsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


def make_request() -> InvestigationRequestedV1:
    now = datetime.now(timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1",
        schema_version="1.0",
        incident_id="INC-1",
        primary_service="payment-service",
        environment="prod",
        severity="CRITICAL",
        first_observed_at=now,
        last_observed_at=now,
        trigger_signal_ids=["evt-1"],
        investigation_run_id="run-1",
        input_signal_version=1,
    )


@pytest.mark.asyncio
async def test_metrics_collector_returns_deterministic_evidence():
    request = make_request()

    result = await StubMetricsCollector().collect(request)

    assert len(result) == 1
    assert result[0].evidence_id == "E-INC-1-METRIC-1"
    assert result[0].entity == "payment-service-db"
    assert result[0].value == 100.0


@pytest.mark.asyncio
async def test_logs_collector_returns_deterministic_evidence():
    request = make_request()

    result = await StubLogsCollector().collect(request)

    assert len(result) == 1
    assert result[0].evidence_id == "E-INC-1-LOG-1"
    assert result[0].occurrences == 73


@pytest.mark.asyncio
async def test_dependency_collector_returns_deterministic_evidence():
    request = make_request()

    result = await StubDependencyCollector().collect(request)

    assert len(result) == 1
    assert result[0].depends_on == "payment-service-db"


@pytest.mark.asyncio
async def test_collectors_are_deterministic_across_calls():
    request = make_request()

    first = await StubMetricsCollector().collect(request)
    second = await StubMetricsCollector().collect(request)

    assert first == second
