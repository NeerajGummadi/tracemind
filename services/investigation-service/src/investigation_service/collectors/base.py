from typing import Protocol

from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


class MetricsCollector(Protocol):
    """Real implementation (Prometheus-backed) is future work - this interface
    is what the orchestrator depends on, never a concrete collector."""

    async def collect(self, request: InvestigationRequestedV1) -> list[MetricEvidence]: ...


class LogsCollector(Protocol):
    """Real implementation (Elasticsearch-backed) is future work."""

    async def collect(self, request: InvestigationRequestedV1) -> list[LogEvidence]: ...


class DependencyCollector(Protocol):
    """Real implementation (topology-source-backed) is future work."""

    async def collect(self, request: InvestigationRequestedV1) -> list[DependencyEvidence]: ...
