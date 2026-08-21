from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPricing:
    input_cost_per_million_tokens: float
    output_cost_per_million_tokens: float


# The single place model pricing is configured - never hardcode a price
# anywhere else. Deliberately empty: current, verified OpenAI pricing isn't
# available locally, and Milestone I's instructions are explicit not to
# guess it. estimate_cost_usd() returns None for any model not listed here
# until this is populated with a trusted value.
MODEL_PRICING: dict[str, ModelPricing] = {}


def estimate_cost_usd(model: str, prompt_tokens: int | None, completion_tokens: int | None) -> float | None:
    """Cost is derived only from real token counts - never estimated ones.
    Returns None if either the token counts or the model's pricing aren't
    known, rather than fabricating a number."""
    if prompt_tokens is None or completion_tokens is None:
        return None

    pricing = MODEL_PRICING.get(model)
    if pricing is None:
        return None

    return (
        (prompt_tokens / 1_000_000) * pricing.input_cost_per_million_tokens
        + (completion_tokens / 1_000_000) * pricing.output_cost_per_million_tokens
    )
