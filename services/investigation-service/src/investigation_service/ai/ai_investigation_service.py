import json
import time
from dataclasses import dataclass

import openai
from openai import AsyncOpenAI
from pydantic import ValidationError

from investigation_service.ai.prompt_builder import PromptBuilder
from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.investigation_result import FailureReason
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis


@dataclass(frozen=True)
class AICallMetrics:
    """Best-effort telemetry for one OpenAI call. ai_latency_ms and
    model_requested are always known (we control both ends of the call);
    everything else is only available once a real response comes back -
    which happens even for MALFORMED_RESPONSE/SCHEMA_VALIDATION_FAILED
    (the request succeeded, only the content failed our checks), but not
    for TIMEOUT/RATE_LIMITED/API_ERROR (no response was ever received)."""

    ai_latency_ms: float
    model_requested: str
    model_returned: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(frozen=True)
class AIInvestigationOutcome:
    rca: RootCauseAnalysis
    metrics: AICallMetrics


class AIInvestigationError(Exception):
    """Every AI failure mode collapses to one exception type with a
    structured reason code, mirroring how the Java side unifies several
    Kafka failure modes under SignalPublishException/OutboxPublishException.
    The orchestrator - not this service - decides what to do about it.
    metrics is required, not optional: every raise site below can always
    compute at least ai_latency_ms and model_requested."""

    def __init__(self, reason: FailureReason, message: str, metrics: AICallMetrics):
        super().__init__(message)
        self.reason = reason
        self.metrics = metrics


class AIInvestigationService:
    """Receives evidence, builds a prompt, calls OpenAI, validates the
    response, returns a RootCauseAnalysis plus call telemetry. No Kafka, no
    orchestration - just this one transformation."""

    def __init__(
        self,
        client: AsyncOpenAI,
        prompt_builder: PromptBuilder,
        model: str,
        temperature: float,
        max_output_tokens: int,
    ):
        self._client = client
        self._prompt_builder = prompt_builder
        self._model = model
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens

    async def investigate(self, evidence: EvidenceBundle) -> AIInvestigationOutcome:
        prompt = self._prompt_builder.build(evidence)

        # Monotonic clock, not datetime subtraction - unaffected by wall-clock
        # adjustments, appropriate for performance timing.
        start = time.monotonic()

        # Order matters: APITimeoutError and RateLimitError are both subclasses
        # of APIError, so the specific cases must be caught first.
        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                response_format={"type": "json_object"},
                temperature=self._temperature,
                max_completion_tokens=self._max_output_tokens,
                messages=[
                    {"role": "system", "content": prompt.system_prompt},
                    {"role": "user", "content": prompt.user_prompt},
                ],
            )
        except openai.APITimeoutError as e:
            raise AIInvestigationError(
                "TIMEOUT", "OpenAI request timed out", self._request_failure_metrics(start)
            ) from e
        except openai.RateLimitError as e:
            raise AIInvestigationError(
                "RATE_LIMITED", "OpenAI rate limit exceeded", self._request_failure_metrics(start)
            ) from e
        except openai.APIError as e:
            raise AIInvestigationError(
                "API_ERROR", f"OpenAI API error: {e}", self._request_failure_metrics(start)
            ) from e

        # We have a real response now - latency, model, and usage are all
        # knowable regardless of whether the content itself is usable.
        latency_ms = (time.monotonic() - start) * 1000
        usage = response.usage
        metrics = AICallMetrics(
            ai_latency_ms=latency_ms,
            model_requested=self._model,
            model_returned=response.model,
            prompt_tokens=usage.prompt_tokens if usage else None,
            completion_tokens=usage.completion_tokens if usage else None,
            total_tokens=usage.total_tokens if usage else None,
        )

        content = response.choices[0].message.content

        try:
            raw = json.loads(content)
        except json.JSONDecodeError as e:
            raise AIInvestigationError("MALFORMED_RESPONSE", "OpenAI response was not valid JSON", metrics) from e

        try:
            rca = RootCauseAnalysis.model_validate(raw)
        except ValidationError as e:
            raise AIInvestigationError(
                "SCHEMA_VALIDATION_FAILED", f"Response failed schema validation: {e}", metrics
            ) from e

        self._verify_grounded_in_evidence(rca, evidence, metrics)
        return AIInvestigationOutcome(rca=rca, metrics=metrics)

    def _request_failure_metrics(self, start: float) -> AICallMetrics:
        """No response was ever received, so only latency and the requested
        model are knowable."""
        return AICallMetrics(ai_latency_ms=(time.monotonic() - start) * 1000, model_requested=self._model)

    def _verify_grounded_in_evidence(
        self, rca: RootCauseAnalysis, evidence: EvidenceBundle, metrics: AICallMetrics
    ) -> None:
        """Schema-valid JSON isn't enough - blueprint Section 37: every
        substantive claim must reference real evidenceIds. A model that
        hallucinates an evidence ID or the wrong incident is exactly as
        untrustworthy as one that returns malformed JSON."""
        if rca.incident_id != evidence.incident_id:
            raise AIInvestigationError(
                "SCHEMA_VALIDATION_FAILED",
                f"Response incidentId {rca.incident_id!r} does not match request {evidence.incident_id!r}",
                metrics,
            )

        known_evidence_ids = {
            item.evidence_id
            for item in (*evidence.metrics, *evidence.logs, *evidence.dependencies)
        }
        cited = set(rca.supporting_evidence_ids)
        unknown = cited - known_evidence_ids
        if unknown:
            raise AIInvestigationError(
                "SCHEMA_VALIDATION_FAILED",
                f"Response cites evidence IDs not present in the EvidenceBundle: {sorted(unknown)}",
                metrics,
            )
