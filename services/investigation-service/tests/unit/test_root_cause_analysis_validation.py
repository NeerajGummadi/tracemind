import pytest
from pydantic import ValidationError

from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis


def valid_payload() -> dict:
    return {
        "incidentId": "INC-1",
        "summary": "Database connection pool exhaustion",
        "probableRootCause": "Connection pool exhausted due to slow queries",
        "confidence": 0.82,
        "supportingEvidenceIds": ["E-1", "E-2"],
        "remediationSteps": ["Inspect long-running queries", "Increase pool size"],
    }


def test_valid_payload_parses():
    rca = RootCauseAnalysis.model_validate(valid_payload())

    assert rca.incident_id == "INC-1"
    assert rca.confidence == 0.82
    assert rca.supporting_evidence_ids == ["E-1", "E-2"]


@pytest.mark.parametrize("missing_field", ["incidentId", "summary", "probableRootCause", "confidence", "supportingEvidenceIds", "remediationSteps"])
def test_missing_required_field_is_rejected(missing_field):
    payload = valid_payload()
    del payload[missing_field]

    with pytest.raises(ValidationError):
        RootCauseAnalysis.model_validate(payload)


@pytest.mark.parametrize("bad_confidence", [-0.1, 1.1, 2.0])
def test_confidence_out_of_range_is_rejected(bad_confidence):
    payload = valid_payload()
    payload["confidence"] = bad_confidence

    with pytest.raises(ValidationError):
        RootCauseAnalysis.model_validate(payload)


def test_empty_supporting_evidence_ids_is_rejected():
    payload = valid_payload()
    payload["supportingEvidenceIds"] = []

    with pytest.raises(ValidationError):
        RootCauseAnalysis.model_validate(payload)


def test_wrong_type_is_rejected():
    payload = valid_payload()
    payload["confidence"] = "high"  # should be a float

    with pytest.raises(ValidationError):
        RootCauseAnalysis.model_validate(payload)
