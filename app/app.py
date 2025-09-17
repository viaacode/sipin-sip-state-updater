import signal
import threading
import time

from cloudevents.events import Event, EventOutcome, PulsarBinding
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.db import DbClient
from app.services.mam import MamPoller
from app.services.pulsar import PulsarClient


class UpdaterService:
    """
    UpdaterService is responsible for updating the sipin state, by
    listening to Pulsar events and processing them and by polling
    MediaHaven.
    """

    def __init__(self):
        """
        Initializes the UpdaterService with configuration, logging,
        database client and Pulsar client.
        """
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.db_client = DbClient()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.shutdown = threading.Event()
        self.event_listener = EventListener(self.log, self.db_client)
        self.mam_poller = MamPoller.from_config_parser(
            config_parser,
            log=self.log,
            db_client=self.db_client,
            shutdown=self.shutdown,
        )

    def start(self):
        """
        Starts listening for incoming messages from the Pulsar topic(s)
        and polling MediaHaven.
        """
        self.event_listener.start()
        t = threading.Thread(target=self.mam_poller.poll, daemon=True)
        t.start()

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        try:
            while not self.shutdown.is_set():
                print("Waiting for shutdown...", flush=True)
                self.shutdown.wait(1)
        finally:
            try:
                self.event_listener.stop()
                self.db_client.close()
            except Exception:
                pass

    def stop(self, *_):
        """Stop the service"""
        self.shutdown.set()
        print("Shutting down...", flush=True)


class EventListener:
    """EventListener is responsible for listening to Pulsar events and processing them."""

    def __init__(self, log, db_client):
        self.db_client = db_client
        self.log = log
        self.pulsar_client = PulsarClient()

    def pulsar_handler(self, consumer, message):
        """
        Handle an incoming Pulsar message.
        """
        try:
            event = PulsarBinding.from_protocol(message)  # type: ignore
            self.handle_incoming_sipin_message(event)
            consumer.acknowledge(message)
        except Exception as e:
            # Catch and log any errors during message processing
            self.log.error(f"Error: {e}")
            consumer.negative_acknowledge(message)
        time.sleep(10)

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

    def start(self):
        self.pulsar_client.subscribe(handler=self.pulsar_handler)

    def stop(self):
        self.pulsar_client.close()
