import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import openai
import pytest

from investigation_service.ai.ai_investigation_service import AIInvestigationError, AIInvestigationService
from investigation_service.ai.prompt_builder import PromptBuilder
from investigation_service.contracts.evidence import EvidenceBundle, LogEvidence, MetricEvidence


def make_bundle() -> EvidenceBundle:
    now = datetime.now(timezone.utc)
    return EvidenceBundle(
        incident_id="INC-1",
        metrics=[MetricEvidence(evidence_id="E-M1", entity="svc-db", fact="pool full", observed_at=now, value=100.0, unit="percent")],
        logs=[LogEvidence(evidence_id="E-L1", entity="svc", fact="timeout", observed_at=now, occurrences=5)],
        dependencies=[],
        collected_at=now,
    )


_DEFAULT_USAGE = SimpleNamespace(prompt_tokens=120, completion_tokens=45, total_tokens=165)


def fake_response(content: str, model: str = "gpt-4o-mini-2024-07-18", usage=_DEFAULT_USAGE):
    """Minimal stand-in for the OpenAI SDK's ChatCompletion response shape:
    response.choices[0].message.content, response.model, response.usage.
    A sentinel default (not None) so callers can explicitly pass usage=None
    to simulate the SDK's response.usage being genuinely absent, without
    that colliding with "no override given"."""
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
        model=model,
        usage=usage,
    )


def valid_rca_json(incident_id: str = "INC-1", evidence_ids: list[str] | None = None) -> str:
    return json.dumps({
        "incidentId": incident_id,
        "summary": "Database connection pool exhaustion",
        "probableRootCause": "Slow queries exhausted the connection pool",
        "confidence": 0.85,
        "supportingEvidenceIds": evidence_ids if evidence_ids is not None else ["E-M1", "E-L1"],
        "remediationSteps": ["Inspect slow queries"],
    })


def make_service(client: AsyncMock) -> AIInvestigationService:
    return AIInvestigationService(
        client=client, prompt_builder=PromptBuilder(), model="gpt-4o-mini",
        temperature=0.0, max_output_tokens=1000,
    )


@pytest.mark.asyncio
async def test_valid_response_returns_root_cause_analysis_and_metrics():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json()))

    outcome = await make_service(client).investigate(make_bundle())

    assert outcome.rca.incident_id == "INC-1"
    assert outcome.rca.supporting_evidence_ids == ["E-M1", "E-L1"]
    client.chat.completions.create.assert_awaited_once()
    # Configured params reached the SDK call.
    _, kwargs = client.chat.completions.create.call_args
    assert kwargs["temperature"] == 0.0
    assert kwargs["max_completion_tokens"] == 1000


@pytest.mark.asyncio
async def test_ai_latency_is_captured_and_positive():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json()))

    outcome = await make_service(client).investigate(make_bundle())

    assert outcome.metrics.ai_latency_ms >= 0
    assert isinstance(outcome.metrics.ai_latency_ms, float)


@pytest.mark.asyncio
async def test_token_usage_is_mapped_correctly_from_response():
    client = MagicMock()
    usage = SimpleNamespace(prompt_tokens=200, completion_tokens=80, total_tokens=280)
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json(), usage=usage))

    outcome = await make_service(client).investigate(make_bundle())

    assert outcome.metrics.prompt_tokens == 200
    assert outcome.metrics.completion_tokens == 80
    assert outcome.metrics.total_tokens == 280


@pytest.mark.asyncio
async def test_model_requested_and_returned_are_both_captured():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        return_value=fake_response(valid_rca_json(), model="gpt-4o-mini-2024-07-18")
    )

    outcome = await make_service(client).investigate(make_bundle())

    assert outcome.metrics.model_requested == "gpt-4o-mini"
    assert outcome.metrics.model_returned == "gpt-4o-mini-2024-07-18"


@pytest.mark.asyncio
async def test_absent_usage_data_does_not_crash_the_investigation():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json(), usage=None))

    outcome = await make_service(client).investigate(make_bundle())

    assert outcome.rca is not None
    assert outcome.metrics.prompt_tokens is None
    assert outcome.metrics.completion_tokens is None
    assert outcome.metrics.total_tokens is None


@pytest.mark.asyncio
async def test_malformed_json_raises_with_correct_reason_and_preserves_metrics():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response("not valid json {{{"))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "MALFORMED_RESPONSE"
    # A real response came back (bad content, but a response) - usage/model are still known.
    assert exc_info.value.metrics.prompt_tokens == 120
    assert exc_info.value.metrics.model_returned == "gpt-4o-mini-2024-07-18"


@pytest.mark.asyncio
async def test_schema_validation_failure_raises_with_correct_reason_and_preserves_metrics():
    incomplete = json.dumps({"incidentId": "INC-1", "summary": "not enough fields"})
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(incomplete))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "SCHEMA_VALIDATION_FAILED"
    assert exc_info.value.metrics.total_tokens == 165


@pytest.mark.asyncio
async def test_hallucinated_evidence_id_is_rejected():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        return_value=fake_response(valid_rca_json(evidence_ids=["E-DOES-NOT-EXIST"]))
    )

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "SCHEMA_VALIDATION_FAILED"


@pytest.mark.asyncio
async def test_mismatched_incident_id_is_rejected():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json(incident_id="INC-WRONG")))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "SCHEMA_VALIDATION_FAILED"


@pytest.mark.asyncio
async def test_timeout_raises_with_correct_reason_and_still_captures_latency():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(side_effect=openai.APITimeoutError(request=MagicMock()))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "TIMEOUT"
    # No response was ever received, but latency (how long we waited) is still knowable.
    assert exc_info.value.metrics.ai_latency_ms >= 0
    assert exc_info.value.metrics.model_requested == "gpt-4o-mini"
    assert exc_info.value.metrics.prompt_tokens is None


@pytest.mark.asyncio
async def test_rate_limit_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        side_effect=openai.RateLimitError(message="rate limited", response=MagicMock(status_code=429), body=None)
    )

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "RATE_LIMITED"
    assert exc_info.value.metrics.ai_latency_ms >= 0


@pytest.mark.asyncio
async def test_generic_api_error_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        side_effect=openai.APIError(message="server exploded", request=MagicMock(), body=None)
    )

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "API_ERROR"
    assert exc_info.value.metrics.ai_latency_ms >= 0
