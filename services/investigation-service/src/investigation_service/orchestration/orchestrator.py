import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from investigation_service.ai.ai_investigation_service import AICallMetrics, AIInvestigationError, AIInvestigationService
from investigation_service.collectors.base import DependencyCollector, LogsCollector, MetricsCollector
from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.investigation_metrics import InvestigationMetrics
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.contracts.investigation_result import InvestigationResult, InvestigationStatus
from investigation_service.evidence.aggregator import EvidenceAggregator
from investigation_service.observability.cost_estimator import estimate_cost_usd
from investigation_service.observability.timing import timed

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvidenceCollectionTimings:
    """Log-only detail (see InvestigationMetrics's docstring for why this
    isn't part of the Kafka contract). total_ms is measured independently
    around the whole gather(), not derived from the three individual
    numbers - since collection is concurrent, total_ms reflects the slowest
    collector, not their sum."""

    metrics_ms: float
    logs_ms: float
    dependencies_ms: float
    total_ms: float


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
        total_start = time.monotonic()

        bundle, collection_timings = await self._collect_evidence(request)

        try:
            outcome = await self._ai_investigation_service.investigate(bundle)
        except AIInvestigationError as e:
            logger.warning("AI investigation failed for %s: %s (%s)", request.incident_id, e, e.reason)
            total_ms = (time.monotonic() - total_start) * 1000
            metrics = self._build_metrics(total_ms, collection_timings, e.metrics)
            self._log_completion(request.incident_id, "FAILED", metrics, collection_timings)
            return InvestigationResult(
                incident_id=request.incident_id,
                investigation_run_id=request.investigation_run_id,
                status="FAILED",
                evidence=bundle,
                failure_reason=e.reason,
                metrics=metrics,
                generated_at=datetime.now(timezone.utc),
            )

        total_ms = (time.monotonic() - total_start) * 1000
        metrics = self._build_metrics(total_ms, collection_timings, outcome.metrics)
        self._log_completion(request.incident_id, "COMPLETED", metrics, collection_timings)
        return InvestigationResult(
            incident_id=request.incident_id,
            investigation_run_id=request.investigation_run_id,
            status="COMPLETED",
            evidence=bundle,
            root_cause_analysis=outcome.rca,
            metrics=metrics,
            generated_at=datetime.now(timezone.utc),
        )

    async def _collect_evidence(
        self, request: InvestigationRequestedV1
    ) -> tuple[EvidenceBundle, EvidenceCollectionTimings]:
        # Concurrent evidence collection, per blueprint Section 23. Each
        # collector is independently timed inside the gather(), and the
        # whole gather() is independently timed around it, so total_ms
        # reflects true wall-clock elapsed time, not sum-of-parts.
        collection_start = time.monotonic()
        (metrics, metrics_ms), (logs, logs_ms), (dependencies, deps_ms) = await asyncio.gather(
            timed(self._metrics_collector.collect(request)),
            timed(self._logs_collector.collect(request)),
            timed(self._dependency_collector.collect(request)),
        )
        collection_total_ms = (time.monotonic() - collection_start) * 1000

        bundle = self._aggregator.aggregate(
            incident_id=request.incident_id,
            metrics=metrics,
            logs=logs,
            dependencies=dependencies,
        )
        timings = EvidenceCollectionTimings(
            metrics_ms=metrics_ms, logs_ms=logs_ms, dependencies_ms=deps_ms, total_ms=collection_total_ms
        )
        return bundle, timings

    def _build_metrics(
        self, total_ms: float, collection_timings: EvidenceCollectionTimings, ai_metrics: AICallMetrics
    ) -> InvestigationMetrics:
        model = ai_metrics.model_returned or ai_metrics.model_requested
        return InvestigationMetrics(
            total_duration_ms=total_ms,
            evidence_collection_duration_ms=collection_timings.total_ms,
            open_ai_latency_ms=ai_metrics.ai_latency_ms,
            prompt_tokens=ai_metrics.prompt_tokens,
            completion_tokens=ai_metrics.completion_tokens,
            total_tokens=ai_metrics.total_tokens,
            estimated_api_cost_usd=estimate_cost_usd(model, ai_metrics.prompt_tokens, ai_metrics.completion_tokens),
            model=model,
        )

    def _log_completion(
        self,
        incident_id: str,
        status: InvestigationStatus,
        metrics: InvestigationMetrics,
        collection_timings: EvidenceCollectionTimings,
    ) -> None:
        # Never logs API keys, prompts, or raw evidence content - only
        # aggregate numbers and identifiers. Per-collector breakdown is
        # log-only detail, not part of the Kafka contract.
        logger.info(
            "Investigation completed incidentId=%s status=%s totalDurationMs=%.1f "
            "evidenceCollectionMs=%.1f (metrics=%.1f logs=%.1f dependencies=%.1f) "
            "openAiLatencyMs=%s promptTokens=%s completionTokens=%s totalTokens=%s model=%s",
            incident_id,
            status,
            metrics.total_duration_ms,
            collection_timings.total_ms,
            collection_timings.metrics_ms,
            collection_timings.logs_ms,
            collection_timings.dependencies_ms,
            metrics.open_ai_latency_ms,
            metrics.prompt_tokens,
            metrics.completion_tokens,
            metrics.total_tokens,
            metrics.model,
        )
