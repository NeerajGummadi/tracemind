from openai import AsyncOpenAI

from investigation_service.config import Settings


def create_openai_client(settings: Settings) -> AsyncOpenAI:
    """The only place that reads the API key. Fails fast at startup (called
    from main.py's lifespan, before the app starts serving traffic) rather
    than letting the app run with a broken key and fail confusingly later."""
    if not settings.openai_api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. No hardcoded default is provided on purpose."
        )
    return AsyncOpenAI(
        api_key=settings.openai_api_key,
        timeout=settings.openai_timeout_seconds,
        max_retries=settings.openai_max_retries,
    )
