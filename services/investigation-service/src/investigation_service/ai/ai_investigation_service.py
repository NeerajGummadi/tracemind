import json

import openai
from openai import AsyncOpenAI
from pydantic import ValidationError

from investigation_service.ai.prompt_builder import PromptBuilder
from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.investigation_result import FailureReason
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis


class AIInvestigationError(Exception):
    """Every AI failure mode collapses to one exception type with a
    structured reason code, mirroring how the Java side unifies several
    Kafka failure modes under SignalPublishException/OutboxPublishException.
    The orchestrator - not this service - decides what to do about it."""

    def __init__(self, reason: FailureReason, message: str):
        super().__init__(message)
        self.reason = reason


class AIInvestigationService:
    """Receives evidence, builds a prompt, calls OpenAI, validates the
    response, returns RootCauseAnalysis. No Kafka, no orchestration - just
    this one transformation."""

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

    async def investigate(self, evidence: EvidenceBundle) -> RootCauseAnalysis:
        prompt = self._prompt_builder.build(evidence)

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
            raise AIInvestigationError("TIMEOUT", "OpenAI request timed out") from e
        except openai.RateLimitError as e:
            raise AIInvestigationError("RATE_LIMITED", "OpenAI rate limit exceeded") from e
        except openai.APIError as e:
            raise AIInvestigationError("API_ERROR", f"OpenAI API error: {e}") from e

        content = response.choices[0].message.content

        try:
            raw = json.loads(content)
        except json.JSONDecodeError as e:
            raise AIInvestigationError("MALFORMED_RESPONSE", "OpenAI response was not valid JSON") from e

        try:
            rca = RootCauseAnalysis.model_validate(raw)
        except ValidationError as e:
            raise AIInvestigationError("SCHEMA_VALIDATION_FAILED", f"Response failed schema validation: {e}") from e

        self._verify_grounded_in_evidence(rca, evidence)
        return rca

    def _verify_grounded_in_evidence(self, rca: RootCauseAnalysis, evidence: EvidenceBundle) -> None:
        """Schema-valid JSON isn't enough - blueprint Section 37: every
        substantive claim must reference real evidenceIds. A model that
        hallucinates an evidence ID or the wrong incident is exactly as
        untrustworthy as one that returns malformed JSON."""
        if rca.incident_id != evidence.incident_id:
            raise AIInvestigationError(
                "SCHEMA_VALIDATION_FAILED",
                f"Response incidentId {rca.incident_id!r} does not match request {evidence.incident_id!r}",
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
            )
