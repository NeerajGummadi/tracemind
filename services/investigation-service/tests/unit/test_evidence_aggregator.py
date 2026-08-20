from datetime import datetime, timezone

from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.evidence.aggregator import EvidenceAggregator


def test_aggregate_merges_all_three_evidence_types_into_one_bundle():
    now = datetime.now(timezone.utc)
    metrics = [MetricEvidence(evidence_id="E-1", entity="svc-db", fact="pool full", observed_at=now, value=100.0, unit="percent")]
    logs = [LogEvidence(evidence_id="E-2", entity="svc", fact="timeout", observed_at=now, occurrences=5)]
    dependencies = [DependencyEvidence(evidence_id="E-3", entity="svc", fact="svc depends on db", observed_at=now, depends_on="db")]

    bundle = EvidenceAggregator().aggregate("INC-1", metrics, logs, dependencies)

    assert bundle.incident_id == "INC-1"
    assert bundle.metrics == metrics
    assert bundle.logs == logs
    assert bundle.dependencies == dependencies
    assert bundle.collected_at is not None


def test_aggregate_handles_empty_evidence_lists():
    bundle = EvidenceAggregator().aggregate("INC-2", [], [], [])

    assert bundle.incident_id == "INC-2"
    assert bundle.metrics == []
    assert bundle.logs == []
    assert bundle.dependencies == []
