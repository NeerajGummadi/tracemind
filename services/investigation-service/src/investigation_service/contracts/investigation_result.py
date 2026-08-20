from datetime import datetime
from typing import Literal

from investigation_service.contracts.base import CamelModel
from investigation_service.contracts.evidence import EvidenceBundle

# Deliberately not one of blueprint Section 19's AI verification states
# (VERIFIED / PARTIALLY_VERIFIED / UNVERIFIED / INSUFFICIENT_EVIDENCE) - those
# presuppose an RCA exists to verify, which doesn't happen until Milestone H.
InvestigationStatus = Literal["EVIDENCE_COLLECTED"]


class InvestigationResult(CamelModel):
    """Milestone G stub: evidence only, no hypotheses, no RCA, no AI content."""

    incident_id: str
    schema_version: str = "1.0"
    status: InvestigationStatus = "EVIDENCE_COLLECTED"
    evidence: EvidenceBundle
    generated_at: datetime
