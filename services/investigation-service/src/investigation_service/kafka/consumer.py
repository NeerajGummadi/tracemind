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
    ported to aiokafka's manual-commit idiom."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        orchestrator: InvestigationOrchestrator,
        publisher: InvestigationResultPublisher,
    ):
        self._consumer = consumer
        self._orchestrator = orchestrator
        self._publisher = publisher

    async def run(self) -> None:
        async for record in self._consumer:
            try:
                request = InvestigationRequestedV1.model_validate_json(record.value)
                result = await self._orchestrator.investigate(request)
                await self._publisher.publish(result)
                await self._consumer.commit()
            except Exception:
                logger.exception(
                    "Failed to process investigation request at offset %s - "
                    "not committing, will be redelivered", record.offset)
