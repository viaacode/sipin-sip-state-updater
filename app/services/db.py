# Standard
from datetime import datetime
from enum import StrEnum

# Third-party
from psycopg import sql
from psycopg_pool import ConnectionPool
from typing import Optional
from viaa.configuration import ConfigParser
from viaa.observability import logging


DEFAULT_SIP_FAILURE_MESSAGE = "SIP ingest failed"
DEFAULT_MAM_FAILURE_MESSAGE = "MediaHaven ingest failed"
POLLER_EVENT_TYPE = "mediahaven.sip.archived"


class SipStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILURE = "failure"


class DbClient:
    def __init__(self):
        config_parser = ConfigParser()
        self.log = logging.get_logger(__name__, config=config_parser)
        db_config: dict = config_parser.app_cfg["db"]
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
        self.schema = "public"
        self.table = db_config["table"]

    def select_pids_in_progress(
        self,
    ) -> list[str]:
        """
        Query the sipin table and select all rows where the PID is
        set and the status is `in progress'.
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(
                            query=sql.SQL(
                                """SELECT DISTINCT pid FROM {}
                                WHERE status = %(in_progress)s AND pid IS NOT NULL;"""
                            ).format(sql.Identifier(self.schema, self.table)),
                            params={"in_progress": SipStatus.IN_PROGRESS},
                        )
                    except Exception:
                        pass
                    return [x[0] for x in cur.fetchall()]
        except Exception:
            return []

    def update_sip_ingest_failed(
        self,
        correlation_id: str,
        event_type: str,
        event_timestamp: datetime,
        failure_message: Optional[str],
    ) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL(
                        """UPDATE {}
                        SET last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s,
                            status=%(failure)s,
                            failure_message=%(failure_message)s
                        WHERE correlation_id=%(correlation_id)s
                          AND last_event_occurred_at<%(event_timestamp)s;"""
                    ).format(sql.Identifier(self.schema, self.table)),
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
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL(
                        """UPDATE {}
                        SET last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s
                        WHERE correlation_id=%(correlation_id)s
                          AND last_event_occurred_at<%(event_timestamp)s;"""
                    ).format(sql.Identifier(self.schema, self.table)),
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
        event_timestamp,
    ) -> int:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query=sql.SQL(
                        """UPDATE {}
                        SET status=%(success)s,
                            last_event_type=%(event_type)s,
                            last_event_occurred_at=%(event_timestamp)s
                        WHERE pid=%(pid)s
                          AND status=%(in_progress)s;"""
                    ).format(sql.Identifier(self.schema, self.table)),
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
        event_timestamp,
        failure_message: Optional[str],
    ) -> int:
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
                          AND status=%(in_progress)s;"""
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

    def close(self):
        """Close the connection (pool)"""
        self.pool.close()
