from datetime import datetime, timezone

from investigation_service.ai.prompt_builder import PromptBuilder
from investigation_service.contracts.evidence import DependencyEvidence, EvidenceBundle, LogEvidence, MetricEvidence


def make_bundle() -> EvidenceBundle:
    now = datetime.now(timezone.utc)
    return EvidenceBundle(
        incident_id="INC-7",
        metrics=[MetricEvidence(evidence_id="E-M1", entity="svc-db", fact="pool full", observed_at=now, value=100.0, unit="percent")],
        logs=[LogEvidence(evidence_id="E-L1", entity="svc", fact="timeout repeated", observed_at=now, occurrences=42)],
        dependencies=[DependencyEvidence(evidence_id="E-D1", entity="svc", fact="svc depends on db", observed_at=now, depends_on="db")],
        collected_at=now,
    )


def test_prompt_separates_facts_from_instructions():
    prompt = PromptBuilder().build(make_bundle())

    # Instructions (system prompt) contain rules, not evidence content.
    assert "RULES" in prompt.system_prompt
    assert "E-M1" not in prompt.system_prompt
    assert "pool full" not in prompt.system_prompt

    # Facts (user prompt) contain evidence content, not behavioral rules.
    assert "RULES" not in prompt.user_prompt
    assert "E-M1" in prompt.user_prompt


def test_prompt_includes_incident_summary():
    prompt = PromptBuilder().build(make_bundle())

    assert "INC-7" in prompt.user_prompt
    assert "INCIDENT SUMMARY" in prompt.user_prompt


def test_prompt_includes_all_three_evidence_types():
    prompt = PromptBuilder().build(make_bundle())

    assert "METRIC EVIDENCE" in prompt.user_prompt and "E-M1" in prompt.user_prompt
    assert "LOG EVIDENCE" in prompt.user_prompt and "E-L1" in prompt.user_prompt
    assert "DEPENDENCY EVIDENCE" in prompt.user_prompt and "E-D1" in prompt.user_prompt


def test_prompt_handles_empty_evidence_gracefully():
    now = datetime.now(timezone.utc)
    empty_bundle = EvidenceBundle(incident_id="INC-8", metrics=[], logs=[], dependencies=[], collected_at=now)

    prompt = PromptBuilder().build(empty_bundle)

    assert "(none collected)" in prompt.user_prompt


def test_system_prompt_embeds_the_actual_response_schema():
    prompt = PromptBuilder().build(make_bundle())

    # Embeds RootCauseAnalysis's real schema, not a hand-written duplicate
    # that could drift out of sync with the model Pydantic actually validates.
    assert "probableRootCause" in prompt.system_prompt or "probable_root_cause" in prompt.system_prompt
    assert "supportingEvidenceIds" in prompt.system_prompt or "supporting_evidence_ids" in prompt.system_prompt
