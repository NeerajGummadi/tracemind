import logging
from datetime import datetime, timezone

import httpx

from investigation_service.collectors.label_query_escaping import escape_label_value
from investigation_service.contracts.evidence import LogEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

logger = logging.getLogger(__name__)


class LokiLogsCollector:
    """Real Loki-backed LogsCollector. Same internal-failure-handling
    philosophy as PrometheusMetricsCollector: every Loki failure mode is
    handled here and never raises, so the orchestrator needs no changes
    (blueprint Section 31).

    Raw lines are grouped by exact content into one LogEvidence per distinct
    message, with occurrences = count - matching the LOG_PATTERN evidence
    model already established by StubLogsCollector (blueprint Section 12),
    not one evidence item per raw line."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        window_seconds: int,
        max_entries: int,
    ):
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._window_seconds = window_seconds
        self._max_entries = max_entries

    async def collect(self, request: InvestigationRequestedV1) -> list[LogEvidence]:
        try:
            streams = await self._query_range(request)
        except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning("Loki query failed for incidentId=%s: %s", request.incident_id, e)
            return []
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("Loki returned a malformed response for incidentId=%s: %s", request.incident_id, e)
            return []

        return self._to_evidence(streams, request)

    async def _query_range(self, request: InvestigationRequestedV1) -> list[dict]:
        query = (
            f'{{service="{escape_label_value(request.primary_service)}", '
            f'environment="{escape_label_value(request.environment)}"}}'
        )
        # Loki wants nanosecond Unix timestamps - a real difference from
        # Prometheus's float-seconds API, confirmed empirically before writing this.
        start_ns = int((request.first_observed_at.timestamp() - self._window_seconds) * 1_000_000_000)
        end_ns = int((request.last_observed_at.timestamp() + self._window_seconds) * 1_000_000_000)

        response = await self._client.get(
            f"{self._base_url}/loki/api/v1/query_range",
            params={
                "query": query,
                "start": start_ns,
                "end": end_ns,
                "limit": self._max_entries,
                "direction": "forward",
            },
        )
        response.raise_for_status()
        payload = response.json()

        if payload["status"] != "success":
            raise ValueError(f"Loki returned status={payload.get('status')!r}")

        return payload["data"]["result"]

    def _to_evidence(self, streams: list[dict], request: InvestigationRequestedV1) -> list[LogEvidence]:
        raw_lines: list[tuple[int, str]] = []
        for stream in streams:
            for entry in stream.get("values", []):
                try:
                    timestamp_ns, line = entry
                    raw_lines.append((int(timestamp_ns), str(line)))
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "Skipping malformed Loki log entry for incidentId=%s: %s", request.incident_id, e
                    )

        # Defensive cap in addition to the server-side `limit` query param.
        raw_lines = raw_lines[: self._max_entries]

        groups: dict[str, list[int]] = {}
        for timestamp_ns, line in raw_lines:
            groups.setdefault(line, []).append(timestamp_ns)

        evidence: list[LogEvidence] = []
        for index, line in enumerate(sorted(groups), start=1):
            timestamps = groups[line]
            evidence.append(
                LogEvidence(
                    evidence_id=f"E-{request.incident_id}-LOG-{index}",
                    entity=request.primary_service,
                    fact=f'"{line}" occurred {len(timestamps)} time(s)',
                    observed_at=datetime.fromtimestamp(max(timestamps) / 1_000_000_000, tz=timezone.utc),
                    occurrences=len(timestamps),
                )
            )
        return evidence
