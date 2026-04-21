# Standard
from __future__ import annotations
from datetime import datetime, timedelta
from enum import StrEnum, auto

# Third-party
from psycopg import sql
from psycopg_pool import ConnectionPool
from typing import Optional
from viaa.configuration import ConfigParser
from viaa.observability import logging

# Typing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterator

DEFAULT_SIP_FAILURE_MESSAGE = "SIP ingest failed"
DEFAULT_MAM_FAILURE_MESSAGE = "MediaHaven ingest failed"
POLLER_EVENT_TYPE = "mediahaven.sip.archived"


class SipStatus(StrEnum):
    IN_PROGRESS = auto()
    SUCCESS = auto()
    FAILURE = auto()


class DbClient:
    """
    Query and update the state database.  This client is shared between threads
    (the Pulsar message handler and the MediaHaven poller).

    Note on SQL query construction:
    This class uses psycopg3 to construct queries.  These are dynamic queries with
    some identifiers (table names) taken from config.  These are interpolated by
    calling format() on a SQL object.  The query parameters are passed to the
    execute() method instead.  These are not really interpolated into the query,
    but sent to the server separately.  In psycopg v3.3 / python 3.14, t-strings
    could be used instead.
    """

    def __init__(self) -> None:
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        db_config: dict[str, str] = config_parser.app_cfg["db"]
        self.pool = ConnectionPool(
            " ".join(
                [
                    f"host={db_config['host']}",
                    f"port={db_config['port']}",
                    f"dbname={db_config['dbname']}",
                    f"user={db_config['username']}",
                    f"password={db_config['password']}",
                ]
            ),
            min_size=4,  # default: 4
        )
        self._cutoff = float(db_config["failure_cutoff_weeks"])
        self.schema = "public"
        self.table = db_config["table"]

    def select_sips_in_progress(self) -> Iterator[str]:
        """
        Query the sipin table and select all rows where the PID is
        set and the status is `in progress'.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL("""SELECT pid
                        FROM (
                            SELECT DISTINCT ON (pid) pid, correlation_id, status
                            FROM {}
                            WHERE NULLIF(pid, '') IS NOT NULL
                            ORDER BY pid, created_at DESC
                        ) latest
                        WHERE status = %(in_progress)s;""").format(
                        sql.Identifier(self.schema, self.table)
                    ),
                    params={"in_progress": SipStatus.IN_PROGRESS},
                )
                for row in cur:
                    yield row[0]

    def _get_cutoff_timestamp(self) -> datetime:
        return datetime.now() - timedelta(weeks=self._cutoff)

    def select_recent_failed_sips(self) -> Iterator[str]:
        """
        Query the sipin table and select all rows where the status is
        `failed' and the created_at timestamp is less than 4 weeks old.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cutoff_timestamp = self._get_cutoff_timestamp()
                self.log.info(
                    f"[DB] Looking for failed SIPs created after {cutoff_timestamp}"
                )
                cur.execute(
                    query=sql.SQL("""SELECT pid
                        FROM (
                            SELECT DISTINCT ON (pid) pid, correlation_id, status
                            FROM {}
                            WHERE NULLIF(pid, '') IS NOT NULL
                            AND created_at > %(timestamp)s
                            ORDER BY pid, created_at DESC
                        ) latest
                        WHERE status = %(failure)s;""").format(
                        sql.Identifier(self.schema, self.table)
                    ),
                    params={
                        "failure": SipStatus.FAILURE,
                        "timestamp": cutoff_timestamp,
                    },
                )
                for row in cur:
                    yield row[0]

    def update_sip_ingest_failed(
        self,
        correlation_id: str,
        event_type: str,
        event_timestamp: datetime,
        failure_message: Optional[str],
    ) -> int:
        """Mark a record in the state database as failed during SIP ingest."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL("""UPDATE {}
                        SET last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s,
                            status=%(failure)s,
                            failure_message=%(failure_message)s
                        WHERE correlation_id=%(correlation_id)s
                          AND last_event_occurred_at<%(event_timestamp)s;""").format(
                        sql.Identifier(self.schema, self.table)
                    ),
                    params={
                        "event_type": event_type,
                        "event_timestamp": event_timestamp,
                        "failure": SipStatus.FAILURE,
                        "failure_message": (
                            failure_message
                            if failure_message
                            else DEFAULT_SIP_FAILURE_MESSAGE
                        ),
                        "correlation_id": correlation_id,
                    },
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
        """Record a sipin event in the state database."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL("""UPDATE {}
                        SET last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s
                        WHERE correlation_id=%(correlation_id)s
                          AND last_event_occurred_at<%(event_timestamp)s;""").format(
                        sql.Identifier(self.schema, self.table)
                    ),
                    params={
                        "event_type": event_type,
                        "event_timestamp": event_timestamp,
                        "correlation_id": correlation_id,
                    },
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def update_sip_ingest_pid(
        self,
        correlation_id: str,
        pid: str,
    ) -> int:
        """Update a record in the state database with its PID."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL(
                        """UPDATE {}
                        SET pid=%(pid)s
                        WHERE correlation_id=%(correlation_id)s AND pid IS NULL;"""
                    ).format(sql.Identifier(self.schema, self.table)),
                    params={
                        "pid": pid,
                        "correlation_id": correlation_id,
                    },
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def update_sip_mam_success(
        self,
        pid: str,
        event_timestamp: datetime,
    ) -> int:
        """Mark a SIP as correctly archived in MediaHaven in the state database."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL("""UPDATE {}
                        SET status=%(success)s,
                            last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s
                        WHERE pid=%(pid)s
                          AND last_event_occurred_at<%(event_timestamp)s
                          AND (last_event_type IS DISTINCT FROM %(event_type)s
                            OR status IS DISTINCT FROM %(success)s);""").format(
                        sql.Identifier(self.schema, self.table)
                    ),
                    params={
                        "success": SipStatus.SUCCESS,
                        "in_progress": SipStatus.IN_PROGRESS,
                        "event_type": POLLER_EVENT_TYPE,
                        "event_timestamp": event_timestamp,
                        "pid": pid,
                    },
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def update_sip_mam_failure(
        self,
        pid: str,
        event_timestamp: datetime,
        failure_message: Optional[str],
    ) -> int:
        """Mark a SIP as failed during MediaHaven ingest in the state database."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL(
                        """UPDATE {}
                        SET status=%(failure)s,
                            last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s,
                            failure_message=%(failure_message)s
                        WHERE pid=%(pid)s
                          AND last_event_occurred_at<%(event_timestamp)s
                          AND (last_event_type IS DISTINCT FROM %(event_type)s
                            OR status IS DISTINCT FROM %(failure)s
                            OR failure_message IS DISTINCT FROM %(failure_message)s);"""
                    ).format(sql.Identifier(self.schema, self.table)),
                    params={
                        "failure": SipStatus.FAILURE,
                        "failure_message": (
                            failure_message
                            if failure_message
                            else DEFAULT_MAM_FAILURE_MESSAGE
                        ),
                        "in_progress": SipStatus.IN_PROGRESS,
                        "event_type": POLLER_EVENT_TYPE,
                        "event_timestamp": event_timestamp,
                        "pid": pid,
                    },
                )
                conn.commit()
                row_count = cur.rowcount
        return row_count

    def close(self) -> None:
        """Close the connection (pool)"""
        self.pool.close()
