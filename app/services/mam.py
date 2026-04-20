# future imports
from __future__ import annotations

# stdlib imports
from datetime import datetime
from enum import StrEnum, auto

# meemoo imports
from mediahaven import MediaHaven
from mediahaven.mediahaven import AcceptFormat
from mediahaven.oauth2 import ROPCGrant
from mediahaven.resources.base_resource import MediaHavenPageObjectJSON
from viaa.configuration import ConfigParser

import time

# local imports
from app import MamRecord
from app.config import MediaHavenConfig

# type imports
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from app import Logger
    from app.services.db import DbClient
    from threading import Event
    from typing import Any, Iterator, Optional, Self, Tuple


class RecordType(StrEnum):
    IE = auto()
    SIP = auto()


class MamPoller:
    """MamPoller is responsible for polling MediaHaven for SIPs in progress."""

    def __init__(
        self,
        config: MediaHavenConfig,
        db_client: DbClient,
        log: Logger,
        shutdown: Event,
        mam_client: MediaHaven,
        polling_interval_hours: float,
    ) -> None:
        self.config = config
        self.mam_client = mam_client
        self.db_client = db_client
        self.log = log
        self.shutdown = shutdown
        self.polling_interval_hours = polling_interval_hours

    @classmethod
    def from_mediahaven_config(
        cls,
        config: MediaHavenConfig,
        **kwargs: Any,
    ) -> Self:
        return cls(
            config=config,
            mam_client=cls.get_mediahaven_client(config),
            **kwargs,
        )

    @classmethod
    def from_config_parser(
        cls,
        config: ConfigParser,
        **kwargs: Any,
    ) -> Self:
        mediahaven_config = MediaHavenConfig.from_config_parser(config)
        return cls.from_mediahaven_config(config=mediahaven_config, **kwargs)

    @classmethod
    def get_mediahaven_client(
        cls,
        mediahaven: MediaHavenConfig,
    ) -> MediaHaven:
        """
        Return a MediaHaven client.

        Parameters:
            mediahaven_config {MediaHavenConfig} -- config dataclass

        Returns:
            MediaHaven -- a MediaHaven client
        """
        grant = ROPCGrant(
            mh_base_url=mediahaven.mh_base_url,
            client_id=mediahaven.client_id,
            client_secret=mediahaven.client_secret,
        )
        grant.request_token(
            username=mediahaven.username,
            password=mediahaven.password,
        )
        client = MediaHaven(mediahaven.mh_base_url, grant)
        return client

    @staticmethod
    def _get_mediahaven_ie_query(
        pid: str,
    ) -> str:
        """Get MediaHaven query by PID."""
        if not pid:
            raise ValueError("No PID to build MediaHaven query with")
        return (
            "+(Administrative.DeleteStatus:*)"
            "+(Internal.IsInIngestSpace:*)"
            "+(Structural.Relations.ContainedBy:*)"
            f"+(Administrative.ExternalId:{pid})"
        )

    @staticmethod
    def _get_mediahaven_sip_query(
        pid: str,
    ) -> str:
        """Get MediaHaven query by PID."""
        if not pid:
            raise ValueError("No PID to build MediaHaven query with")
        return (
            "+(Administrative.DeleteStatus:*)"
            "+(Internal.IsInIngestSpace:*)"
            "+(Administrative.MainRecordType:Sip)"
            f"+(OriginalFilename:{pid}.zip)"
        )

    def _get_record_from_page_object(
        self,
        pid: str,
        target: RecordType,
        query: str,
        page: MediaHavenPageObjectJSON,
    ) -> Optional[MamRecord]:
        records = page.as_generator()
        count = cast(int, page.total_nr_of_results)
        if count == 1:
            record = next(records)
            return cast(MamRecord, record)
        elif count > 1:
            record, *deleted_records = sorted(
                records,
                key=lambda record: record.Administrative.ArchiveDate,
                reverse=True,
            )
            if any(
                record.Administrative.DeleteStatus == "NotDeleted"
                for record in deleted_records
            ):
                message = f"found {count} {target.name} records for PID `{pid}' and fewer than n-1 records were deleted"
                self.log.warning(message, pid=pid, query=query)
            else:
                message = f"found {count} {target.name} records for PID `{pid}' but at least n-1 records were deleted"
                self.log.info(message, pid=pid, query=query)
            return cast(MamRecord, record)
        else:
            return None

    def _get_name(self) -> str:
        return type(self).__name__

    def _query_record_by_pid(self, pid: str) -> Optional[Tuple[MamRecord, RecordType]]:
        if ie_record := self._query_mam(pid, target=RecordType.IE):
            return ie_record, RecordType.IE
        elif sip_record := self._query_mam(pid, target=RecordType.SIP):
            return sip_record, RecordType.SIP
        else:
            return None

    def _query_mam(
        self, pid: str, target: RecordType = RecordType.IE
    ) -> Optional[MamRecord]:
        if not pid:
            return None

        match target:
            case RecordType.IE:
                query = self._get_mediahaven_ie_query(pid)
            case RecordType.SIP:
                query = self._get_mediahaven_sip_query(pid)
            case _:
                raise ValueError(f"Unknown query type `{target}' in MAM query")

        result = self.mam_client.records.search(
            accept_format=AcceptFormat.JSON,
            q=query,
        )
        return self._get_record_from_page_object(pid, target, query, result)

    @staticmethod
    def _is_success(record: MamRecord, record_type: RecordType) -> bool:
        """Define a successfully archived SIP based on MediaHaven record field values."""
        try:
            match record_type:
                case RecordType.IE:
                    return bool(
                        record.Internal.ArchiveStatus == "completed"
                        and (
                            record.Administrative.RecordStatus == "Published"
                            or record.Administrative.RecordStatus == "Draft.Valid"
                        )
                    )
                case RecordType.SIP:
                    return bool(
                        record.Administrative.RecordStatus == "Accepted"
                        or record.Administrative.RecordStatus == "Published"
                    )
                case _:
                    return False
        except Exception:
            return False

    @staticmethod
    def _is_failure(record: MamRecord, record_type: RecordType) -> bool:
        """Define a failed SIP based on MediaHaven record field values."""
        try:
            return bool(
                record.Internal.ArchiveStatus == "failed"
                or record.Administrative.RecordStatus == "Draft.Invalid"
                or record.Administrative.RecordStatus == "Rejected"
            )
        except Exception:
            return False

    def _get_archived_date(self, record: MamRecord) -> datetime:
        """Get the archived date from a MediaHaven record."""
        try:
            date = record.Administrative.ArchiveDate
            return datetime.fromisoformat(date)
        except Exception as e:
            self.log.exception(f"Failed to get archived date: {e}")
            return datetime.now()

    def _get_rejection_date(self, record: MamRecord) -> datetime:
        """Get the rejection date from a MediaHaven record."""
        try:
            date = record.Administrative.RejectionDate
            return datetime.fromisoformat(date)
        except Exception as e:
            self.log.exception(f"Failed to get rejected date: {e}")
            return datetime.now()

    @staticmethod
    def _get_failure_message(record: MamRecord) -> Optional[str]:
        """Get a failure message from a MediaHaven record."""
        try:
            rejections = record.Administrative.RecordRejections.Rejection
            message = "\n".join([r.Motivation for r in rejections])
            return message
        except Exception:
            return None

    def _check_record_status(
        self, record: MamRecord, pid: str, record_type: RecordType
    ) -> None:
        """
        Check the status of a MediaHaven record and store it in the SIP
        deliveries database.
        """
        if self._is_success(record, record_type):
            timestamp = self._get_archived_date(record)
            self.log.debug(
                f"SIP `{pid}' was successfully archived at {timestamp}",
                pid=pid,
                status="success",
            )
            self.db_client.update_sip_mam_success(
                pid=pid,
                event_timestamp=timestamp,
            )
        elif self._is_failure(record, record_type):
            timestamp = self._get_rejection_date(record)
            self.log.info(
                f"SIP `{pid}' failed to archive at {timestamp}",
                pid=pid,
                status="failure",
            )
            self.db_client.update_sip_mam_failure(
                pid=pid,
                event_timestamp=timestamp,
                failure_message=self._get_failure_message(record),
            )
        else:
            self.log.debug(f"SIP `{pid}' neither failed nor succeeded", pid=pid)

    def _pids_to_poll(self) -> Iterator[str]:
        self.log.debug(f"[{self._get_name()}] looking for SIPs in progress")
        return self.db_client.select_sips_in_progress()

    def _get_link_to_monitoring(self, record: MamRecord) -> str:
        base = self.config.mh_base_url
        fragment = record.Internal.FragmentId
        return f"{base}/monitoring/index.php?config=default&service=MediaHaven&view=Files&umid={fragment}"

    def _poll_pid(self, pid: str) -> None:
        try:
            result = self._query_record_by_pid(pid)
            if not result:
                self.log.debug(f"didn't find MAM record for PID `{pid}'", pid=pid)
            else:
                record, record_type = result
                self.log.debug(
                    f"found {record_type.name} for PID `{pid}'",
                    pid=pid,
                    monitoring_url=self._get_link_to_monitoring(record),
                )
                self._check_record_status(record, pid, record_type)
        except Exception as e:
            self.log.exception(f"failed to poll for PID `{pid}': {e}", pid=pid)
        finally:
            time.sleep(0.1)

    def _poll_mam(self) -> None:
        """Get the PIDs to poll for and poll them."""
        for pid in self._pids_to_poll():
            self._poll_pid(pid)
        self.log.debug(
            f"[{self._get_name()}] done polling; checking back in {self.polling_interval_hours}h"
        )

    def _get_polling_interval_seconds(self) -> float:
        """Return the polling interval, in seconds."""
        return self.polling_interval_hours * 60 * 60

    def _is_running(self) -> bool:
        return not self.shutdown.is_set()

    def _wait(self) -> None:
        self.log.debug(
            f"[{self._get_name()}] done polling; checking back in {self.polling_interval_hours}h"
        )
        self.shutdown.wait(self._get_polling_interval_seconds())

    def poll(self) -> None:
        """On a fixed schedule, poll MediaHaven for the status of SIPs."""
        while self._is_running():
            try:
                self._poll_mam()
            except Exception as e:
                self.log.exception(f"[{self._get_name()}] failure during polling: {e}")
            finally:
                self._wait()


class MamFailuresPoller(MamPoller):
    """MamFailuresPoller is responsible for polling MediaHaven for failed SIPs."""

    def _pids_to_poll(self) -> Iterator[str]:
        self.log.debug(f"[{self._get_name()}] looking for recent failed SIPs")
        return self.db_client.select_recent_failed_sips()
