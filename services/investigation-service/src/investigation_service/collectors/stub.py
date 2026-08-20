"""Deterministic stub collectors - fixed canned data, not random, so results
are reproducible in tests. Real Prometheus/Elasticsearch/topology-backed
collectors are explicitly out of scope for this milestone."""

from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


class StubMetricsCollector:
    async def collect(self, request: InvestigationRequestedV1) -> list[MetricEvidence]:
        return [
            MetricEvidence(
                evidence_id=f"E-{request.incident_id}-METRIC-1",
                entity=f"{request.primary_service}-db",
                fact="Connection pool utilization reached 100%",
                observed_at=request.last_observed_at,
                value=100.0,
                unit="percent",
            )
        ]


class StubLogsCollector:
    async def collect(self, request: InvestigationRequestedV1) -> list[LogEvidence]:
        return [
            LogEvidence(
                evidence_id=f"E-{request.incident_id}-LOG-1",
                entity=request.primary_service,
                fact="Hikari connection acquisition timeout repeated",
                observed_at=request.last_observed_at,
                occurrences=73,
            )
        ]


class StubDependencyCollector:
    async def collect(self, request: InvestigationRequestedV1) -> list[DependencyEvidence]:
        return [
            DependencyEvidence(
                evidence_id=f"E-{request.incident_id}-DEP-1",
                entity=request.primary_service,
                fact=f"{request.primary_service} depends on {request.primary_service}-db",
                observed_at=request.last_observed_at,
                depends_on=f"{request.primary_service}-db",
            )
        ]
