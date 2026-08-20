from datetime import datetime, timezone

from investigation_service.contracts.evidence import DependencyEvidence, EvidenceBundle, LogEvidence, MetricEvidence


class EvidenceAggregator:
    """A deterministic merge of the three collectors' output into one bundle.
    Not yet the full Evidence Correlation Engine (no temporal alignment, no
    entity matching, no graph construction) - that's future work built on
    top of this foundation."""

    def aggregate(
        self,
        incident_id: str,
        metrics: list[MetricEvidence],
        logs: list[LogEvidence],
        dependencies: list[DependencyEvidence],
    ) -> EvidenceBundle:
        return EvidenceBundle(
            incident_id=incident_id,
            metrics=metrics,
            logs=logs,
            dependencies=dependencies,
            collected_at=datetime.now(timezone.utc),
        )
