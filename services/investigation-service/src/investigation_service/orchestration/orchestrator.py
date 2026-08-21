import asyncio
import logging
from datetime import datetime, timezone

from investigation_service.ai.ai_investigation_service import AIInvestigationError, AIInvestigationService
from investigation_service.collectors.base import DependencyCollector, LogsCollector, MetricsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.investigation_result import InvestigationResult
from investigation_service.evidence.aggregator import EvidenceAggregator

logger = logging.getLogger(__name__)


class InvestigationOrchestrator:
    """The only component that knows the end-to-end sequence. Pure with
    respect to I/O framing - no Kafka, no HTTP - just collectors and an AI
    service in, a result out. An AI failure never crashes the investigation
    (blueprint Section 31): it becomes a FAILED result, evidence included,
    instead of propagating as an exception."""

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        logs_collector: LogsCollector,
        dependency_collector: DependencyCollector,
        aggregator: EvidenceAggregator,
        ai_investigation_service: AIInvestigationService,
    ):
        self._metrics_collector = metrics_collector
        self._logs_collector = logs_collector
        self._dependency_collector = dependency_collector
        self._aggregator = aggregator
        self._ai_investigation_service = ai_investigation_service

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

        try:
            rca = await self._ai_investigation_service.investigate(bundle)
        except AIInvestigationError as e:
            logger.warning("AI investigation failed for %s: %s (%s)", request.incident_id, e, e.reason)
            return InvestigationResult(
                incident_id=request.incident_id,
                status="FAILED",
                evidence=bundle,
                failure_reason=e.reason,
                generated_at=datetime.now(timezone.utc),
            )

        return InvestigationResult(
            incident_id=request.incident_id,
            status="COMPLETED",
            evidence=bundle,
            root_cause_analysis=rca,
            generated_at=datetime.now(timezone.utc),
        )
