import logging

from aiokafka import AIOKafkaConsumer

from investigation_service.contracts.investigation_requested import InvestigationRequestedV1
from investigation_service.kafka.publisher import InvestigationResultPublisher
from investigation_service.orchestration.orchestrator import InvestigationOrchestrator

logger = logging.getLogger(__name__)


class InvestigationRequestConsumer:
    """Manual commit, offset advanced only after the full pipeline (collect,
    aggregate, publish) succeeds - the same "never acknowledge success you
    can't guarantee" pattern as SignalConsumerListener on the Java side,
    ported to aiokafka's manual-commit idiom.

    Milestone M: also guards against launching a duplicate OpenAI call for a
    redelivered investigation.requested.v1 message (e.g. publish succeeded
    but the commit that follows it failed/timed out, so Kafka redelivers the
    same investigationRunId). The guard is in-memory, scoped to this
    process's lifetime - it does not survive a restart. Durable dedup (never
    overwriting a newer *result*) lives in Incident Service, which owns
    Postgres; giving Investigation Service its own datastore just for this
    would be a bigger architectural change than this milestone authorizes."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        orchestrator: InvestigationOrchestrator,
        publisher: InvestigationResultPublisher,
    ):
        self._consumer = consumer
        self._orchestrator = orchestrator
        self._publisher = publisher
        self._processed_run_ids: set[str] = set()

    async def run(self) -> None:
        async for record in self._consumer:
            try:
                request = InvestigationRequestedV1.model_validate_json(record.value)
                if request.investigation_run_id in self._processed_run_ids:
                    logger.info(
                        "Skipping duplicate investigationRunId=%s at offset %s - already processed",
                        request.investigation_run_id, record.offset)
                    await self._consumer.commit()
                    continue

                result = await self._orchestrator.investigate(request)
                await self._publisher.publish(result)
                self._processed_run_ids.add(request.investigation_run_id)
                await self._consumer.commit()
            except Exception:
                logger.exception(
                    "Failed to process investigation request at offset %s - "
                    "not committing, will be redelivered", record.offset)
