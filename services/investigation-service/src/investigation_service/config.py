from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Env-var-driven config, mirroring the Java services' application.yml + env override pattern."""

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    kafka_bootstrap_servers: str = "localhost:9092"
    investigation_requested_topic: str = "investigation.requested.v1"
    investigation_results_topic: str = "investigation.results.v1"
    kafka_consumer_group_id: str = "investigation-service"
    http_port: int = 8083


settings = Settings()
