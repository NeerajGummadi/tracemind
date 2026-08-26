from datetime import datetime, timezone

import pytest

from investigation_service.ai.ai_test_double import DeterministicAIInvestigationDouble
from investigation_service.contracts.evidence import DependencyEvidence, EvidenceBundle


@pytest.mark.asyncio
async def test_double_returns_valid_rca_grounded_in_real_evidence():
    bundle = EvidenceBundle(
        incident_id="INC-1", metrics=[], logs=[],
        dependencies=[DependencyEvidence(
            evidence_id="E-INC-1-DEP-1", entity="payment-service", fact="f",
            observed_at=datetime.now(timezone.utc), depends_on="payment-service-db",
        )],
        collected_at=datetime.now(timezone.utc),
    )

    outcome = await DeterministicAIInvestigationDouble().investigate(bundle)

    assert outcome.rca.incident_id == "INC-1"
    assert outcome.rca.supporting_evidence_ids == ["E-INC-1-DEP-1"]
    assert outcome.metrics.model_requested == "ai-test-double"


@pytest.mark.asyncio
async def test_double_falls_back_to_placeholder_for_empty_bundle():
    bundle = EvidenceBundle(
        incident_id="INC-2", metrics=[], logs=[], dependencies=[],
        collected_at=datetime.now(timezone.utc),
    )

    outcome = await DeterministicAIInvestigationDouble().investigate(bundle)

    assert outcome.rca.supporting_evidence_ids == ["E-INC-2-SYNTHETIC-0"]
