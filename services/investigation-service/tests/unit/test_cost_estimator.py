from investigation_service.observability import cost_estimator
from investigation_service.observability.cost_estimator import ModelPricing, estimate_cost_usd


def test_unknown_model_returns_none_without_guessing():
    assert estimate_cost_usd("some-model-not-in-the-table", prompt_tokens=100, completion_tokens=50) is None


def test_missing_prompt_tokens_returns_none():
    assert estimate_cost_usd("gpt-4o-mini", prompt_tokens=None, completion_tokens=50) is None


def test_missing_completion_tokens_returns_none():
    assert estimate_cost_usd("gpt-4o-mini", prompt_tokens=100, completion_tokens=None) is None


def test_both_tokens_missing_returns_none():
    assert estimate_cost_usd("gpt-4o-mini", prompt_tokens=None, completion_tokens=None) is None


def test_cost_computed_only_from_actual_token_counts_when_pricing_is_known(monkeypatch):
    # Deliberately fabricated numbers for the test only - not real pricing,
    # and not read from anywhere the real cost calculation would ever use.
    monkeypatch.setitem(
        cost_estimator.MODEL_PRICING,
        "test-model",
        ModelPricing(input_cost_per_million_tokens=1.0, output_cost_per_million_tokens=2.0),
    )

    cost = estimate_cost_usd("test-model", prompt_tokens=1_000_000, completion_tokens=1_000_000)

    assert cost == 3.0  # 1M * $1/1M + 1M * $2/1M


def test_cost_scales_linearly_with_token_count(monkeypatch):
    monkeypatch.setitem(
        cost_estimator.MODEL_PRICING,
        "test-model",
        ModelPricing(input_cost_per_million_tokens=10.0, output_cost_per_million_tokens=10.0),
    )

    cost = estimate_cost_usd("test-model", prompt_tokens=500_000, completion_tokens=0)

    assert cost == 5.0
