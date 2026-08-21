from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Env-var-driven config, mirroring the Java services' application.yml + env override pattern."""

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    kafka_bootstrap_servers: str = "localhost:9092"
    investigation_requested_topic: str = "investigation.requested.v1"
    investigation_results_topic: str = "investigation.results.v1"
    kafka_consumer_group_id: str = "investigation-service"
    http_port: int = 8083

    # No hardcoded default on purpose - create_openai_client() fails fast if
    # this is empty, at actual app startup (main.py's lifespan constructs the
    # client before serving traffic). Left as an empty-string default here
    # rather than a required field so importing this module never fails just
    # because OPENAI_API_KEY isn't set - unit tests that don't touch OpenAI
    # shouldn't need it in their environment.
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_timeout_seconds: float = 30.0
    # 0 = deterministic, reproducible RCA output - appropriate for a
    # structured-JSON, grounded-in-evidence task, not creative generation.
    openai_temperature: float = 0.0
    # Comfortable headroom for the RootCauseAnalysis JSON shape (a handful of
    # short strings + a few array items), explicit rather than relying on
    # the model's full output ceiling.
    openai_max_output_tokens: int = 1000
    # Explicit rather than relying on the SDK's own default (which happens to
    # also be 2, but undocumented-by-us until now).
    openai_max_retries: int = 2

    prometheus_base_url: str = "http://localhost:9090"
    prometheus_timeout_seconds: float = 5.0
    # Padding applied to both sides of [firstObservedAt, lastObservedAt] for
    # the query_range window - not brittle to exact incident/scrape alignment.
    prometheus_query_window_seconds: int = 300
    # Caps how many series-per-metric become evidence - protects against an
    # unexpectedly high-cardinality result, not just our fixed single-series demo.
    prometheus_max_series: int = 10


settings = Settings()
