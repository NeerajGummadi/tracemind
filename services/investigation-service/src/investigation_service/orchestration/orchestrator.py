import asyncio
from datetime import datetime, timezone

from investigation_service.collectors.base import DependencyCollector, LogsCollector, MetricsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.investigation_result import InvestigationResult
from investigation_service.evidence.aggregator import EvidenceAggregator


class InvestigationOrchestrator:
    """The only component that knows the end-to-end sequence. Pure with
    respect to I/O framing - no Kafka, no HTTP - just collectors in, a
    result out. Milestone H will extend this to run AI reasoning after
    aggregation instead of returning a stub result directly."""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        logs_collector: LogsCollector,
        dependency_collector: DependencyCollector,
        aggregator: EvidenceAggregator,
    ):
        self._metrics_collector = metrics_collector
        self._logs_collector = logs_collector
        self._dependency_collector = dependency_collector
        self._aggregator = aggregator

    async def investigate(self, request: InvestigationRequestedV1) -> InvestigationResult:
        # Concurrent evidence collection, per blueprint Section 23.
        metrics, logs, dependencies = await asyncio.gather(
            self._metrics_collector.collect(request),
            self._logs_collector.collect(request),
            self._dependency_collector.collect(request),
        )

        bundle = self._aggregator.aggregate(
            incident_id=request.incident_id,
            metrics=metrics,
            logs=logs,
            dependencies=dependencies,
        )

        return InvestigationResult(
            incident_id=request.incident_id,
            evidence=bundle,
            generated_at=datetime.now(timezone.utc),
        )
