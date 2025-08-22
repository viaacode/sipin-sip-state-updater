from cloudevents.events import Event, EventOutcome, PulsarBinding
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.pulsar import PulsarClient
from app.services.db import DbClient


class EventListener:
    """EventListener is responsible for listening to Pulsar events and processing them."""

    def __init__(self):
        """Initializes the EventListener with configuration, logging, and Pulsar client."""
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.db_client = DbClient()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_client = PulsarClient()

    def handle_incoming_sipin_message(self, event: Event):
        """
        Handles an incoming Pulsar pre-MAM event.

        Args:
            event (Event): The incoming event to process.
        """
        self.log.debug(f"Start handling of event with id: {event.id}.")

        # Check if valid
        if not self._is_event_failed(event):
            count = self.db_client.update_sip_ingest_failed(
                event.correlation_id,
                event.type,
                event.time,
                event.get_data().get("message"),
            )
            self.log.info(
                f"Ingest has failed: {event.correlation_id} with type: {event.type}"
            )
            self.log.debug(
                f"Number of rows updated: {count} with correlation ID: {event.correlation_id}"
            )
            return

        #  Valid
        pid = event.get_data().get("pid")
        if pid:
            self.db_client.update_sip_ingest_pid(event.correlation_id, pid)
            self.log.info(f"Update PID for: {event.correlation_id} with PID: {pid}.")

        count = self.db_client.update_sip_ingest(
            event.correlation_id, event.type, event.time
        )
        self.log.debug(
            f"Number of rows updated: {count} with correlation ID: {event.correlation_id}"
        )

    def _is_event_failed(self, event: Event) -> bool:
        """Check if the event is successful."""

        if not event.has_successful_outcome():
            return False

        data_outcome = event.get_data().get("outcome")
        if data_outcome and data_outcome == EventOutcome.FAIL:
            return False

        data_is_valid: bool = event.get_data().get("is_valid")
        if data_is_valid is False:
            return False

        return True

    def start_listening(self):
        """
        Starts listening for incoming messages from the Pulsar topic.
        """
        while True:
            msg = self.pulsar_client.receive()
            try:
                event = PulsarBinding.from_protocol(msg)  # type: ignore
                self.handle_incoming_sipin_message(event)
                self.pulsar_client.acknowledge(msg)
            except Exception as e:
                # Catch and log any errors during message processing
                self.log.error(f"Error: {e}")
                self.pulsar_client.negative_acknowledge(msg)

        self.pulsar_client.close()
