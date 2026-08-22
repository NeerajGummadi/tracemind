from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from investigation_service.collectors.loki import LokiLogsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


def make_request() -> InvestigationRequestedV1:
    now = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-1", primary_service="payment-service",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"],
    )


def success_response(result: list[dict]) -> httpx.Response:
    return httpx.Response(200, json={"status": "success", "data": {"resultType": "streams", "result": result}})


def stream(values: list[tuple[int, str]]) -> dict:
    return {
        "stream": {"service": "payment-service", "environment": "prod"},
        "values": [[str(ts), line] for ts, line in values],
    }


def make_collector(handler, window_seconds=300, max_entries=200) -> LokiLogsCollector:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return LokiLogsCollector(
        client=client, base_url="http://loki.test", window_seconds=window_seconds, max_entries=max_entries
    )


@pytest.mark.asyncio
async def test_successful_response_maps_to_log_evidence():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([stream([(1755000000000000000, "Hikari connection acquisition timeout")])])

    evidence = await make_collector(handler).collect(make_request())

    assert len(evidence) == 1
    assert evidence[0].occurrences == 1
    assert evidence[0].entity == "payment-service"
    assert "Hikari connection acquisition timeout" in evidence[0].fact


@pytest.mark.asyncio
async def test_multiple_entries_of_the_same_message_are_grouped_with_a_count():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([
            stream([
                (1755000000000000000, "Hikari connection acquisition timeout"),
                (1755000001000000000, "Hikari connection acquisition timeout"),
                (1755000002000000000, "Hikari connection acquisition timeout"),
            ])
        ])

    evidence = await make_collector(handler).collect(make_request())

    assert len(evidence) == 1
    assert evidence[0].occurrences == 3


@pytest.mark.asyncio
async def test_multiple_distinct_messages_produce_separate_evidence_items():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([
            stream([
                (1755000000000000000, "Hikari connection acquisition timeout"),
                (1755000001000000000, "Connection pool exhausted"),
            ])
        ])

    evidence = await make_collector(handler).collect(make_request())

    assert len(evidence) == 2
    facts = {e.fact for e in evidence}
    assert any("Hikari" in f for f in facts)
    assert any("pool exhausted" in f for f in facts)


@pytest.mark.asyncio
async def test_empty_response_produces_no_evidence():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([])

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_timeout_returns_empty_list_without_raising():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("simulated timeout", request=request)

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_malformed_top_level_response_is_handled_gracefully():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "success", "data": {}})  # missing "result"

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_malformed_individual_entry_is_skipped_without_crashing():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([
            {
                "stream": {"service": "payment-service"},
                "values": [["not-a-timestamp", "some log line"]],
            }
        ])

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_http_5xx_is_handled_gracefully():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal server error")

    evidence = await make_collector(handler).collect(make_request())

    assert evidence == []


@pytest.mark.asyncio
async def test_entries_beyond_max_entries_are_dropped():
    def handler(request: httpx.Request) -> httpx.Response:
        many_lines = [(1755000000000000000 + i, f"distinct message {i}") for i in range(10)]
        return success_response([stream(many_lines)])

    evidence = await make_collector(handler, max_entries=4).collect(make_request())

    assert len(evidence) == 4


@pytest.mark.asyncio
async def test_evidence_ids_are_deterministic_across_calls():
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response([
            stream([
                (1755000000000000000, "Connection pool exhausted"),
                (1755000001000000000, "Hikari connection acquisition timeout"),
            ])
        ])

    collector = make_collector(handler)
    first = await collector.collect(make_request())
    second = await collector.collect(make_request())

    assert [e.evidence_id for e in first] == [e.evidence_id for e in second]


@pytest.mark.asyncio
async def test_query_uses_nanosecond_timestamps_and_label_matchers():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        params = parse_qs(urlparse(str(request.url)).query)
        captured["query"] = params["query"][0]
        captured["start"] = params["start"][0]
        captured["end"] = params["end"][0]
        return success_response([])

    await make_collector(handler, window_seconds=300).collect(make_request())

    assert 'service="payment-service"' in captured["query"]
    assert 'environment="prod"' in captured["query"]
    incident_ts = make_request().first_observed_at.timestamp()
    expected_start_ns = int((incident_ts - 300) * 1_000_000_000)
    # Nanosecond precision, not the float-seconds format Prometheus's API uses.
    assert int(captured["start"]) == expected_start_ns
