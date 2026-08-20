import asyncio
import logging
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI

from investigation_service.collectors.stub import StubDependencyCollector, StubLogsCollector, StubMetricsCollector
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

    orchestrator = InvestigationOrchestrator(
        metrics_collector=StubMetricsCollector(),
        logs_collector=StubLogsCollector(),
        dependency_collector=StubDependencyCollector(),
        aggregator=EvidenceAggregator(),
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
        logger.info("Investigation Service stopped")


app = FastAPI(title="investigation-service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "UP"}
