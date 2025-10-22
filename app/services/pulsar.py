from __future__ import annotations

from pulsar import Client, InitialPosition
from viaa.configuration import ConfigParser
from viaa.observability import logging

from .. import APP_NAME

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pulsar import Consumer, Message, Producer
    from typing import Callable


CONSUMER_TOPICS = [
    # 1.X
    "public/sipin/sip.loadgraph",
    "public/sipin/bag.transfer",
    "public/sipin/bag.unzip",
    "public/sipin/mh-sip.create",
    "public/sipin/sip.validate.xsd",
    "public/sipin/sip.validate.shacl",
    "public/sipin/bag.validate",
    "public/sipin/mh-sip.transfer",
    # 2.X
    "public/sipin/sip-2.unzip",
    "public/sipin/sip-2.validate",
    "public/sipin/sip-2.transform",
    "public/sipin/sip-2.mh-sip.create",
]


class PulsarClient:
    """
    Abstraction for a Pulsar Client using the message listener
    pattern.
    """

    def __init__(self) -> None:
        """Initialize the PulsarClient with configuration and a client."""
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_config = config_parser.app_cfg["pulsar"]
        self.client = Client(
            f'pulsar://{self.pulsar_config["host"]}:{self.pulsar_config["port"]}'
        )
        self.producers: dict[str, Producer] = {}

    def subscribe(self, handler: Callable[[Consumer[Message], Message], None]) -> None:
        """
        Start consuming topics with the callable `handler'
        responsible for handling the business logic.
        """
        if not callable(handler):
            raise TypeError("subscribe expects a callable (handler)")

        self.consumer = self.client.subscribe(
            topic=CONSUMER_TOPICS,
            subscription_name=APP_NAME,
            message_listener=handler,
            initial_position=InitialPosition.Earliest,
        )
        self.log.info(f"Started consuming topics: {CONSUMER_TOPICS}")

    def close(self) -> None:
        """Close all producers and the consumer."""
        for producer in self.producers.values():
            producer.close()
        self.consumer.close()
