from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    """Base for every wire contract: snake_case in Python, camelCase on the wire -
    matching every other Kafka topic's JSON convention in this system."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
