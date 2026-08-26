from investigation_service.ai.ai_investigation_service import AICallMetrics, AIInvestigationOutcome
from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis


class DeterministicAIInvestigationDouble:
    """Test-only stand-in for AIInvestigationService (Milestone N Test D -
    investigation lifecycle/coalescing stress). Returns a fixed, valid
    RootCauseAnalysis instantly, with no OpenAI call. Implements the same
    investigate(evidence) -> AIInvestigationOutcome contract as the real
    service, so the orchestrator, Kafka publish, and Incident Service's real
    result-consumption/lifecycle logic are all still exercised for real -
    only the AI reasoning step itself is synthetic.

    Only constructed when AI_TEST_DOUBLE=true (see main.py) - unset/false in
    every other path, so production behavior is unchanged unless explicitly
    opted into for this one benchmark.
    """

    async def investigate(self, evidence: EvidenceBundle) -> AIInvestigationOutcome:
        evidence_ids = [item.evidence_id for item in (*evidence.metrics, *evidence.logs, *evidence.dependencies)]
        # Real evidence is preferred (grounds the RCA the same way a real AI
        # response must); a placeholder only covers the edge case of a fully
        # empty bundle, which nothing downstream cross-checks for this double.
        cited = evidence_ids[:1] if evidence_ids else [f"E-{evidence.incident_id}-SYNTHETIC-0"]

        rca = RootCauseAnalysis(
            incident_id=evidence.incident_id,
            summary="Deterministic test-double RCA (Milestone N Test D, AI_TEST_DOUBLE=true - no real OpenAI call).",
            probable_root_cause="Synthetic root cause generated for investigation lifecycle/coalescing stress testing.",
            confidence=1.0,
            supporting_evidence_ids=cited,
            remediation_steps=["N/A - synthetic benchmark result, not a real investigation"],
        )
        metrics = AICallMetrics(
            ai_latency_ms=0.1,
            model_requested="ai-test-double",
            model_returned="ai-test-double",
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=0,
        )
        return AIInvestigationOutcome(rca=rca, metrics=metrics)
