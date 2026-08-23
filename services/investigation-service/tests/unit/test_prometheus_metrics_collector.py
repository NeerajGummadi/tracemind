import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from investigation_service.collectors.prometheus import PrometheusMetricsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

METRIC_NAMES = [
    "db_connection_pool_active",
    "db_connection_pool_max",
    "db_connection_pool_utilization_percent",
]


def make_request() -> InvestigationRequestedV1:
    now = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-1", primary_service="payment-service",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"], investigation_run_id="run-1", input_signal_version=1,
    )


def query_metric_name(request: httpx.Request) -> str:
    """Every query is metric_name{service="...",environment="..."} - the
    metric name is everything before the first '{'."""
    query = parse_qs(urlparse(str(request.url)).query)["query"][0]
    return query.split("{")[0]


def success_response(result: list[dict]) -> httpx.Response:
    return httpx.Response(200, json={"status": "success", "data": {"resultType": "matrix", "result": result}})


def one_series(*values: tuple[float, str]) -> dict:
    return {
        "metric": {"service": "payment-service", "environment": "prod"},
        "values": [[ts, v] for ts, v in values],
    }


def make_collector(handler, window_seconds=300, max_series=10) -> PrometheusMetricsCollector:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return PrometheusMetricsCollector(
        client=client, base_url="http://prometheus.test", window_seconds=window_seconds, max_series=max_series
    )


@pytest.mark.asyncio
async def test_successful_response_maps_to_metric_evidence_for_all_metrics():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([one_series((1755000000, "100"))])

    evidence = await make_collector(handler).collect(make_request())

    assert len(evidence) == len(METRIC_NAMES)
    utilization = next(e for e in evidence if "utilization" in e.evidence_id)
    assert utilization.value == 100.0
    assert utilization.unit == "percent"
    assert "100.0%" in utilization.fact
    assert utilization.entity == "payment-service-db"


@pytest.mark.asyncio
async def test_multiple_samples_uses_the_last_value_in_the_series():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([one_series((1755000000, "40"), (1755000015, "70"), (1755000030, "100"))])

    evidence = await make_collector(handler).collect(make_request())

    assert all(e.value == 100.0 for e in evidence)


@pytest.mark.asyncio
async def test_empty_result_produces_no_evidence_for_that_metric():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([])

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_malformed_result_is_skipped_without_crashing():
    def handler(request: httpx.Request) -> httpx.Response:
        # "values" missing entirely - malformed relative to the expected shape.
        return success_response([{"metric": {"service": "payment-service"}}])

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_non_numeric_value_is_skipped_without_crashing():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([one_series((1755000000, "not-a-number"))])

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_timeout_on_one_metric_does_not_prevent_others_from_succeeding():
    def handler(request: httpx.Request) -> httpx.Response:
        if query_metric_name(request) == "db_connection_pool_active":
            raise httpx.TimeoutException("simulated timeout", request=request)
        return success_response([one_series((1755000000, "100"))])

    evidence = await make_collector(handler).collect(make_request())

    # Only the two metrics that didn't time out produced evidence.
    assert len(evidence) == 2
    assert not any("db_connection_pool_active" in e.evidence_id for e in evidence)


@pytest.mark.asyncio
async def test_prometheus_unavailable_returns_empty_list_without_raising():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("simulated connection refused", request=request)

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_http_5xx_is_handled_gracefully():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal server error")

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_series_beyond_max_series_are_dropped():
    def handler(request: httpx.Request) -> httpx.Response:
        many_series = [one_series((1755000000, str(i))) for i in range(5)]
        return success_response(many_series)

    evidence = await make_collector(handler, max_series=2).collect(make_request())

    # 2 series kept per metric * 3 metrics.
    assert len(evidence) == 2 * len(METRIC_NAMES)


@pytest.mark.asyncio
async def test_evidence_ids_are_deterministic_across_calls():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([one_series((1755000000, "100"))])

    collector = make_collector(handler)
    first = await collector.collect(make_request())
    second = await collector.collect(make_request())

    assert sorted(e.evidence_id for e in first) == sorted(e.evidence_id for e in second)


@pytest.mark.asyncio
async def test_query_window_pads_the_incident_time_range():
    captured_params = {}

    def handler(request: httpx.Request) -> httpx.Response:
        query_params = parse_qs(urlparse(str(request.url)).query)
        captured_params[query_metric_name(request)] = query_params
        return success_response([])

    await make_collector(handler, window_seconds=300).collect(make_request())

    params = captured_params["db_connection_pool_active"]
    incident_ts = make_request().first_observed_at.timestamp()
    assert float(params["start"][0]) == pytest.approx(incident_ts - 300)
    assert float(params["end"][0]) == pytest.approx(incident_ts + 300)


@pytest.mark.asyncio
async def test_query_uses_primary_service_and_environment_as_label_matchers():
    captured_queries = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlparse(str(request.url)).query)["query"][0]
        captured_queries.append(query)
        return success_response([])

    await make_collector(handler).collect(make_request())

    assert all('service="payment-service"' in q for q in captured_queries)
    assert all('environment="prod"' in q for q in captured_queries)
