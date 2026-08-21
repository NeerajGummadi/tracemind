from investigation_service.contracts.base import CamelModel


class InvestigationMetrics(CamelModel):
    """Aggregate, defensible operational numbers for one investigation -
    benchmarking/stress-testing material, not per-collector debug detail
    (that stays in structured logs only, see InvestigationOrchestrator).
    Every field is optional: on some failure paths (e.g. a request that
    times out before any response), not everything is measurable."""

    total_duration_ms: float | None = None
    evidence_collection_duration_ms: float | None = None
    open_ai_latency_ms: float | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    estimated_api_cost_usd: float | None = None
    model: str | None = None
