from pulsar import Client
from viaa.configuration import ConfigParser
from viaa.observability import logging

from .. import APP_NAME

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
    Abstraction for a Pulsar Client.
    """

    def __init__(self):
        """Initialize the PulsarClient with configurations and a consumer."""
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_config = config_parser.app_cfg["pulsar"]

        self.client = Client(
            f'pulsar://{self.pulsar_config["host"]}:{self.pulsar_config["port"]}'
        )
        self.consumer = self.client.subscribe(
            CONSUMER_TOPICS,
            APP_NAME,
        )
        self.log.info(f"Started consuming topics: {CONSUMER_TOPICS}")

    def receive(self):
        """Receive a message from the consumer.

        Returns:
            Message: The received message.
        """
        return self.consumer.receive()

    def acknowledge(self, msg):
        """Acknowledge a message on the consumer.

        Args:
            msg: The message to acknowledge.
        """
        self.consumer.acknowledge(msg)

    def negative_acknowledge(self, msg):
        """Send a negative acknowledgment (nack) for a message.

        Args:
            msg: The message to nack.
        """
        self.consumer.negative_acknowledge(msg)

    def close(self):
        """Close all producers and the consumer."""
        for producer in self.producers.values():
            producer.close()
        self.consumer.close()
