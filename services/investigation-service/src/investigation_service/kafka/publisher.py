from aiokafka import AIOKafkaProducer

from investigation_service.contracts.investigation_result import InvestigationResult


class InvestigationResultPublisher:
    """Publishes to investigation.results.v1, keyed by incidentId - same key
    convention as investigation.requested.v1, so both topics stay
    co-partitioned per incident."""

    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic

    async def publish(self, result: InvestigationResult) -> None:
        key = result.incident_id.encode("utf-8")
        value = result.model_dump_json(by_alias=True).encode("utf-8")
        await self._producer.send_and_wait(self._topic, key=key, value=value)
