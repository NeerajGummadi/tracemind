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


def fake_response(content: str):
    """Minimal stand-in for the OpenAI SDK's ChatCompletion response shape:
    response.choices[0].message.content."""
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


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
async def test_valid_response_returns_root_cause_analysis():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(valid_rca_json()))

    rca = await make_service(client).investigate(make_bundle())

    assert rca.incident_id == "INC-1"
    assert rca.supporting_evidence_ids == ["E-M1", "E-L1"]
    client.chat.completions.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_json_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response("not valid json {{{"))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "MALFORMED_RESPONSE"


@pytest.mark.asyncio
async def test_schema_validation_failure_raises_with_correct_reason():
    incomplete = json.dumps({"incidentId": "INC-1", "summary": "not enough fields"})
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=fake_response(incomplete))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "SCHEMA_VALIDATION_FAILED"


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
async def test_timeout_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(side_effect=openai.APITimeoutError(request=MagicMock()))

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "TIMEOUT"


@pytest.mark.asyncio
async def test_rate_limit_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        side_effect=openai.RateLimitError(message="rate limited", response=MagicMock(status_code=429), body=None)
    )

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "RATE_LIMITED"


@pytest.mark.asyncio
async def test_generic_api_error_raises_with_correct_reason():
    client = MagicMock()
    client.chat.completions.create = AsyncMock(
        side_effect=openai.APIError(message="server exploded", request=MagicMock(), body=None)
    )

    with pytest.raises(AIInvestigationError) as exc_info:
        await make_service(client).investigate(make_bundle())

    assert exc_info.value.reason == "API_ERROR"
