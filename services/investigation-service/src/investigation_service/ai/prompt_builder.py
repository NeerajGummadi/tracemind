import json
from dataclasses import dataclass

from investigation_service.contracts.evidence import EvidenceBundle
from investigation_service.contracts.root_cause_analysis import RootCauseAnalysis


@dataclass(frozen=True)
class ChatPrompt:
    """Never serialized externally (it's an in-process value, not a wire
    contract), so a plain dataclass rather than a Pydantic model."""

    system_prompt: str
    user_prompt: str


class PromptBuilder:
    """Owns all prompt-text construction, so it's never scattered as ad-hoc
    string concatenation inside AIInvestigationService's call logic. Input
    is EvidenceBundle only, as specified - it doesn't reach into
    InvestigationRequestedV1 for richer context (environment, severity),
    since that would mean expanding EvidenceBundle's shape, which is
    Milestone G's existing architecture and out of scope here."""

    def build(self, evidence: EvidenceBundle) -> ChatPrompt:
        return ChatPrompt(
            system_prompt=self._build_instructions(),
            user_prompt=self._build_facts(evidence),
        )

    def _build_instructions(self) -> str:
        schema = json.dumps(RootCauseAnalysis.model_json_schema(), indent=2)
        return (
            "You are an incident root-cause analysis assistant for TraceMind.\n\n"
            "RULES (must follow exactly):\n"
            "- Use only the evidence supplied below. Never invent telemetry, "
            "relationships, or facts not present in the evidence.\n"
            "- Every claim in probableRootCause must be traceable to at least one "
            "supplied evidence item.\n"
            "- supportingEvidenceIds must only contain evidenceId values that appear "
            "in the evidence below - never invent an evidence ID.\n"
            "- If the evidence is insufficient to determine a root cause, say so "
            "plainly in the summary rather than guessing.\n"
            "- Respond with a single JSON object only - no prose, no markdown, no "
            "code fences. The JSON must conform exactly to this schema:\n\n"
            f"{schema}"
        )

    def _build_facts(self, evidence: EvidenceBundle) -> str:
        sections = [
            self._incident_summary(evidence),
            self._metrics_section(evidence),
            self._logs_section(evidence),
            self._dependencies_section(evidence),
        ]
        return "\n\n".join(sections)

    def _incident_summary(self, evidence: EvidenceBundle) -> str:
        return (
            "INCIDENT SUMMARY\n"
            f"incidentId: {evidence.incident_id}\n"
            f"evidence collected at: {evidence.collected_at.isoformat()}\n"
            f"metrics: {len(evidence.metrics)}, logs: {len(evidence.logs)}, "
            f"dependencies: {len(evidence.dependencies)}"
        )

    def _metrics_section(self, evidence: EvidenceBundle) -> str:
        lines = [
            f"- evidenceId={m.evidence_id} entity={m.entity} fact=\"{m.fact}\" "
            f"value={m.value}{m.unit} observedAt={m.observed_at.isoformat()}"
            for m in evidence.metrics
        ]
        return "METRIC EVIDENCE\n" + ("\n".join(lines) if lines else "(none collected)")

    def _logs_section(self, evidence: EvidenceBundle) -> str:
        lines = [
            f"- evidenceId={log.evidence_id} entity={log.entity} fact=\"{log.fact}\" "
            f"occurrences={log.occurrences} observedAt={log.observed_at.isoformat()}"
            for log in evidence.logs
        ]
        return "LOG EVIDENCE\n" + ("\n".join(lines) if lines else "(none collected)")

    def _dependencies_section(self, evidence: EvidenceBundle) -> str:
        lines = [
            f"- evidenceId={d.evidence_id} entity={d.entity} fact=\"{d.fact}\" "
            f"dependsOn={d.depends_on} observedAt={d.observed_at.isoformat()}"
            for d in evidence.dependencies
        ]
        return "DEPENDENCY EVIDENCE\n" + ("\n".join(lines) if lines else "(none collected)")
