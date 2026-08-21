from datetime import datetime
from typing import Literal

from investigation_service.contracts.base import CamelModel
from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.investigation_metrics import InvestigationMetrics
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis

# "COMPLETED"/"FAILED" match the Investigation Service's own component
# responsibilities in the blueprint verbatim ("publish investigation
# completion/failure"). Deliberately not one of blueprint Section 19's AI
# verification states (VERIFIED / PARTIALLY_VERIFIED / UNVERIFIED /
# INSUFFICIENT_EVIDENCE) - those presuppose a critic/verifier stage that
# doesn't exist yet (only hypothesis generation, this milestone).
InvestigationStatus = Literal["COMPLETED", "FAILED"]

FailureReason = Literal[
    "TIMEOUT",
    "MALFORMED_RESPONSE",
    "SCHEMA_VALIDATION_FAILED",
    "RATE_LIMITED",
    "API_ERROR",
]


class InvestigationResult(CamelModel):
    """On COMPLETED, root_cause_analysis is populated and failure_reason is
    None. On FAILED, the reverse - but evidence is always present either way
    (blueprint Section 31: stay useful when AI is unavailable, evidence
    should still reach the dashboard even if reasoning failed)."""

    incident_id: str
    schema_version: str = "1.0"
    status: InvestigationStatus
    evidence: EvidenceBundle
    root_cause_analysis: RootCauseAnalysis | None = None
    failure_reason: FailureReason | None = None
    metrics: InvestigationMetrics | None = None
    generated_at: datetime
