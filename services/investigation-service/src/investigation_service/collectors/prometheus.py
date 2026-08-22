import asyncio
import logging
from datetime import datetime, timezone

import httpx

from investigation_service.collectors.label_query_escaping import escape_label_value
from investigation_service.contracts.evidence import MetricEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

logger = logging.getLogger(__name__)

# The three demo metrics for the DB-connection-pool-exhaustion scenario
# (infrastructure/prometheus/demo_metrics_exporter.py serves these). Fact
# templates and units live here, not in the LLM-facing prompt layer - raw
# Prometheus JSON never reaches PromptBuilder, only these clean sentences.
_METRIC_FACT_TEMPLATES: dict[str, str] = {
    "db_connection_pool_active": "Active DB connections: {value}",
    "db_connection_pool_max": "Maximum DB connection pool size: {value}",
    "db_connection_pool_utilization_percent": "DB connection pool utilization reached {value}%",
}
_METRIC_UNITS: dict[str, str] = {
    "db_connection_pool_active": "connections",
    "db_connection_pool_max": "connections",
    "db_connection_pool_utilization_percent": "percent",
}


class PrometheusMetricsCollector:
    """Real Prometheus-backed MetricsCollector. All Prometheus failure modes
    (unavailable, timeout, malformed response, no data) are handled
    internally and never raise - the orchestrator needs no changes to
    tolerate a degraded/empty metrics collection (blueprint Section 31)."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        window_seconds: int,
        max_series: int,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._window_seconds = window_seconds
        self._max_series = max_series

    async def collect(self, request: InvestigationRequestedV1) -> list[MetricEvidence]:
        results = await asyncio.gather(
            *(self._collect_one_metric(metric_name, request) for metric_name in _METRIC_FACT_TEMPLATES)
        )
        return [evidence for metric_evidence in results for evidence in metric_evidence]

    async def _collect_one_metric(
        self, metric_name: str, request: InvestigationRequestedV1
    ) -> list[MetricEvidence]:
        try:
            series_list = await self._query_range(metric_name, request)
        except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(
                "Prometheus query failed for metric=%s incidentId=%s: %s",
                metric_name, request.incident_id, e,
            )
            return []
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(
                "Prometheus returned a malformed response for metric=%s incidentId=%s: %s",
                metric_name, request.incident_id, e,
            )
            return []

        return self._to_evidence(metric_name, series_list, request)

    async def _query_range(self, metric_name: str, request: InvestigationRequestedV1) -> list[dict]:
        query = (
            f'{metric_name}{{service="{escape_label_value(request.primary_service)}", '
            f'environment="{escape_label_value(request.environment)}"}}'
        )
        start = request.first_observed_at.timestamp() - self._window_seconds
        end = request.last_observed_at.timestamp() + self._window_seconds

        response = await self._client.get(
            f"{self._base_url}/api/v1/query_range",
            params={"query": query, "start": start, "end": end, "step": "15s"},
        )
        response.raise_for_status()
        payload = response.json()

        if payload["status"] != "success":
            raise ValueError(f"Prometheus returned status={payload.get('status')!r}")

        return payload["data"]["result"]

    def _to_evidence(
        self, metric_name: str, series_list: list[dict], request: InvestigationRequestedV1
    ) -> list[MetricEvidence]:
        evidence: list[MetricEvidence] = []
        for series in series_list[: self._max_series]:
            try:
                values = series["values"]
                if not values:
                    continue
                last_timestamp, last_value = values[-1]
                value = float(last_value)
            except (KeyError, IndexError, ValueError, TypeError) as e:
                logger.warning(
                    "Skipping malformed Prometheus series for metric=%s incidentId=%s: %s",
                    metric_name, request.incident_id, e,
                )
                continue

            fact = _METRIC_FACT_TEMPLATES[metric_name].format(value=value)
            evidence.append(
                MetricEvidence(
                    evidence_id=f"E-{request.incident_id}-METRIC-{metric_name}",
                    entity=f"{request.primary_service}-db",
                    fact=fact,
                    observed_at=datetime.fromtimestamp(last_timestamp, tz=timezone.utc),
                    value=value,
                    unit=_METRIC_UNITS[metric_name],
                )
            )
        return evidence
