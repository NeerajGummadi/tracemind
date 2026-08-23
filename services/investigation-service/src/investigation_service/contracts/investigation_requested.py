from datetime import datetime

from investigation_service.contracts.base import CamelModel


class InvestigationRequestedV1(CamelModel):
    """Mirrors the Java contract field-for-field (blueprint Section 6) -
    see services/incident-service/.../contract/InvestigationRequestedV1.java."""

    event_id: str
    schema_version: str
    incident_id: str
    primary_service: str
    environment: str
    severity: str
    first_observed_at: datetime
    last_observed_at: datetime
    trigger_signal_ids: list[str]
    # Milestone M - identifies exactly one AI investigation attempt; an
    # incident may have had several, so incidentId alone is not enough to
    # match a request to "this specific run" for idempotency/result-matching.
    investigation_run_id: str
    input_signal_version: int
