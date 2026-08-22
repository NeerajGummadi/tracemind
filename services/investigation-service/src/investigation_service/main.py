import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI

from investigation_service.ai.ai_investigation_service import AIInvestigationService
from investigation_service.ai.openai_client import create_openai_client
from investigation_service.ai.prompt_builder import PromptBuilder
from investigation_service.collectors.loki import LokiLogsCollector
from investigation_service.collectors.prometheus import PrometheusMetricsCollector
from investigation_service.collectors.static_dependency import StaticDependencyCollector, load_topology
from investigation_service.config import settings
from investigation_service.evidence.aggregator import EvidenceAggregator
from investigation_service.kafka.consumer import InvestigationRequestConsumer
from investigation_service.kafka.publisher import InvestigationResultPublisher
from investigation_service.orchestration.orchestrator import InvestigationOrchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = AIOKafkaConsumer(
        settings.investigation_requested_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)

    await consumer.start()
    await producer.start()

    prometheus_http_client = httpx.AsyncClient(timeout=settings.prometheus_timeout_seconds)
    loki_http_client = httpx.AsyncClient(timeout=settings.loki_timeout_seconds)
    # Loaded once, fails fast on a malformed file - never per-investigation I/O.
    dependency_topology = load_topology(settings.dependency_graph_path)

    ai_investigation_service = AIInvestigationService(
        client=create_openai_client(settings),
        prompt_builder=PromptBuilder(),
        model=settings.openai_model,
        temperature=settings.openai_temperature,
        max_output_tokens=settings.openai_max_output_tokens,
    )
    orchestrator = InvestigationOrchestrator(
        metrics_collector=PrometheusMetricsCollector(
            client=prometheus_http_client,
            base_url=settings.prometheus_base_url,
            window_seconds=settings.prometheus_query_window_seconds,
            max_series=settings.prometheus_max_series,
        ),
        logs_collector=LokiLogsCollector(
            client=loki_http_client,
            base_url=settings.loki_base_url,
            window_seconds=settings.loki_query_window_seconds,
            max_entries=settings.loki_max_entries,
        ),
        dependency_collector=StaticDependencyCollector(
            topology=dependency_topology,
            max_depth=settings.dependency_max_depth,
        ),
        aggregator=EvidenceAggregator(),
        ai_investigation_service=ai_investigation_service,
    )
    publisher = InvestigationResultPublisher(producer, settings.investigation_results_topic)
    request_consumer = InvestigationRequestConsumer(consumer, orchestrator, publisher)

    consumer_task = asyncio.create_task(request_consumer.run())
    logger.info("Investigation Service started, consuming %s", settings.investigation_requested_topic)

    try:
        yield
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await producer.stop()
        await prometheus_http_client.aclose()
        await loki_http_client.aclose()
        logger.info("Investigation Service stopped")


app = FastAPI(title="investigation-service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "UP"}
