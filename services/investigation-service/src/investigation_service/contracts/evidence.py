from datetime import datetime

from investigation_service.contracts.base import CamelModel


class MetricEvidence(CamelModel):
    """Loosely modeled on blueprint Section 12's METRIC evidence example."""

    evidence_id: str
    entity: str
    fact: str
    observed_at: datetime
    value: float
    unit: str


class LogEvidence(CamelModel):
    """Loosely modeled on blueprint Section 12's LOG_PATTERN evidence example."""

    evidence_id: str
    entity: str
    fact: str
    observed_at: datetime
    occurrences: int


class DependencyEvidence(CamelModel):
    """A single edge from the (stubbed, for now) dependency graph, blueprint Section 13."""

    evidence_id: str
    entity: str
    fact: str
    observed_at: datetime
    depends_on: str


class EvidenceBundle(CamelModel):
    """The output of EvidenceAggregator: a deterministic merge, not yet the full
    Evidence Correlation Engine (temporal alignment, entity matching, graph
    construction - blueprint's "Evidence Correlation Engine" section) that a
    later milestone will build on top of this."""

    incident_id: str
    metrics: list[MetricEvidence]
    logs: list[LogEvidence]
    dependencies: list[DependencyEvidence]
    collected_at: datetime
