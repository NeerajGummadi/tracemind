from pydantic import Field

from investigation_service.contracts.base import CamelModel


class RootCauseAnalysis(CamelModel):
    """The LLM's structured output - validated with Pydantic before it's
    trusted anywhere else in the system (blueprint Section 37: no evidence,
    no high-confidence claim). min_length=1 on supporting_evidence_ids
    enforces that every RCA cites at least one piece of evidence; whether
    those IDs actually exist in the EvidenceBundle is checked separately by
    AIInvestigationService, since that requires context this model alone
    doesn't have."""

    incident_id: str
    summary: str
    probable_root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence_ids: list[str] = Field(min_length=1)
    remediation_steps: list[str]
