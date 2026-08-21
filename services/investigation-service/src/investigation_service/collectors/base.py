from typing import Protocol

from investigation_service.contracts.evidence import DependencyEvidence, LogEvidence, MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


class MetricsCollector(Protocol):
    """StubMetricsCollector (deterministic, for isolated tests) and
    PrometheusMetricsCollector (real, production wiring) both implement this -
    the orchestrator depends only on this interface, never a concrete collector."""

    async def collect(self, request: InvestigationRequestedV1) -> list[MetricEvidence]: ...


class LogsCollector(Protocol):
    """Real implementation (Elasticsearch-backed) is future work."""

    async def collect(self, request: InvestigationRequestedV1) -> list[LogEvidence]: ...


class DependencyCollector(Protocol):
    """Real implementation (topology-source-backed) is future work."""

    async def collect(self, request: InvestigationRequestedV1) -> list[DependencyEvidence]: ...
