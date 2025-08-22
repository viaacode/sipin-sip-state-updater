# Standard
from datetime import datetime
from enum import StrEnum

# Third-party
from psycopg_pool import ConnectionPool
from viaa.configuration import ConfigParser
from viaa.observability import logging


class SipStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILURE = "failure"


class DbClient:
    def __init__(self):
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.db_config: dict = config_parser.app_cfg["db"]
        self.pool = ConnectionPool(
            f"host={self.db_config['host']} port={self.db_config['port']} dbname={self.db_config['dbname']} user={self.db_config['username']} password={self.db_config['password']}"
        )
        self.table = self.db_config["table"]

    def update_sip_ingest_failed(
        self,
        correlation_id: str,
        event_type: str,
        event_timestamp: datetime,
        failure_message: str,
    ) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE public.{self.table} SET last_event_type=%s, last_event_occurred_at=%s, status=%s, failure_message=%s WHERE correlation_id=%s and last_event_occurred_at<%s;",
                    (
                        event_type,
                        event_timestamp,
                        SipStatus.FAILURE,
                        failure_message,
                        correlation_id,
                        event_timestamp,
                    ),
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def update_sip_ingest(
        self,
        correlation_id: str,
        event_type: str,
        event_timestamp: datetime,
    ) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE public.{self.table} SET last_event_type=%s, last_event_occurred_at=%s WHERE correlation_id=%s and last_event_occurred_at<%s;",
                    (
                        event_type,
                        event_timestamp,
                        correlation_id,
                        event_timestamp,
                    ),
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def update_sip_ingest_pid(
        self,
        correlation_id: str,
        pid: str,
    ) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE public.{self.table} SET pid=%s WHERE correlation_id=%s AND pid IS NULL;",
                    (pid, correlation_id),
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def close(self):
        """Close the connection (pool)"""
        self.pool.close()
