# future imports
from __future__ import annotations

import time

# stdlib imports
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum, auto

# type imports
from typing import TYPE_CHECKING, cast

# meemoo imports
from mediahaven import MediaHaven
from mediahaven.mediahaven import AcceptFormat
from mediahaven.oauth2 import ROPCGrant
from viaa.configuration import ConfigParser

# local imports
from app import MamRecord, SipinRecord, SipStatus
from app.config import MediaHavenConfig

if TYPE_CHECKING:
    from threading import Event
    from typing import Any, Iterator, Self

    from mediahaven.resources.base_resource import (
        MediaHavenPageObjectJSON,
    )

    from app import Logger
    from app.services.db import DbClient


SLEEP_POLL_SECONDS = 1


class RecordType(StrEnum):
    IE = auto()
    SIP = auto()

    def __str__(self):
        return f"{self.name}"


@dataclass
class CheckResult:
    fragment_id: str
    status: SipStatus
    timestamp: datetime
    failure_message: str | None = None


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
        self.log = log.bind(poller=self._get_name())
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
        query: str,
        page: MediaHavenPageObjectJSON,
    ) -> MamRecord | None:
        """
        When querying for a SIP, we want only a single record.  It's possible multiple
        records are returned.  Preferably, we return the newest not-deleted record.
        """
        records = sorted(
            page.as_generator(),
            key=lambda r: r.Administrative.ArchiveDate,
            reverse=True,
        )
        if not records:
            return None

        if len(records) == 1:
            return records[0]

        # multiple records were found
        not_deleted_records = [
            r for r in records if r.Administrative.DeleteStatus == "NotDeleted"
        ]
        if not not_deleted_records:
            message = f"found {len(records)} records; all deleted"
            self.log.warning(message, pid=pid, query=query)
            return records[0]

        if len(not_deleted_records) == 1:
            message = f"found {len(records)} records; only 1 not deleted"
            self.log.debug(message, pid=pid, query=query)
        elif len(not_deleted_records) > 1:
            message = f"found {len(records)} records; {len(not_deleted_records)} of which not deleted"
            self.log.warning(message, pid=pid, query=query)

        return not_deleted_records[0]

    def _get_name(self) -> str:
        return type(self).__name__

    def _query_sip(self, pid: str) -> MamRecord | None:
        if not pid:
            return None

        query = self._get_mediahaven_sip_query(pid)
        search_result = self.mam_client.records.search(
            accept_format=AcceptFormat.JSON,
            q=query,
        )
        return self._get_record_from_page_object(
            pid=pid, query=query, page=cast("MediaHavenPageObjectJSON", search_result)
        )

    @staticmethod
    def _is_success(record: MamRecord, record_type: RecordType) -> bool:
        """Define a successfully archived SIP based on MediaHaven record field values."""
        try:
            match record_type:
                case RecordType.IE:
                    return bool(
                        record.Administrative.RecordStatus == "Draft.Valid"
                        or record.Administrative.RecordStatus == "Published"
                    )
                case RecordType.SIP:
                    return bool(
                        record.Administrative.RecordStatus == "Accepted"
                        or record.Administrative.RecordStatus == "Published"
                    )
        except Exception:
            return False

    @staticmethod
    def _is_failure(record: MamRecord, record_type: RecordType) -> bool:
        """Define a failed SIP based on MediaHaven record field values."""
        try:
            return bool(
                record.Administrative.RecordStatus == "Draft.Invalid"
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
    def _get_failure_message(record: MamRecord) -> str | None:
        """Get a failure message from a MediaHaven record."""
        try:
            rejections = record.Administrative.RecordRejections.Rejection
            message = "\n".join([r.Motivation for r in rejections])
            return message
        except Exception:
            return None

    def _check_record(self, record: MamRecord, record_type: RecordType) -> CheckResult:
        """Check the status of a MediaHaven record."""
        if self._is_success(record, record_type):
            timestamp = self._get_archived_date(record)
            return CheckResult(
                fragment_id=record.Internal.FragmentId,
                status=SipStatus.SUCCESS,
                timestamp=timestamp,
            )
        elif self._is_failure(record, record_type):
            timestamp = self._get_rejection_date(record)
            message = self._get_failure_message(record)
            return CheckResult(
                fragment_id=record.Internal.FragmentId,
                status=SipStatus.FAILURE,
                timestamp=timestamp,
                failure_message=message,
            )
        else:
            return CheckResult(
                fragment_id=record.Internal.FragmentId,
                status=SipStatus.IN_PROGRESS,
                timestamp=datetime.now(),
            )

    def _sipin_records_to_poll(self) -> Iterator[SipinRecord]:
        self.log.debug("looking for SIPs in progress")
        return self.db_client.select_sips_in_progress()

    def _get_ie_from_sip(self, sip: MamRecord) -> MamRecord | None:
        try:
            umid = sip.Structural.Relations.Contains[0]
            return self.mam_client.records.get(umid)
        except:
            return None

    def _poll_pid(self, pid: str) -> CheckResult | None:
        try:
            sip = self._query_sip(pid)
            if not sip:
                self.log.debug(f"No SIP found for PID {pid}", pid=pid)
                return None

            result = self._check_record(sip, RecordType.SIP)
            if result.status in (SipStatus.IN_PROGRESS, SipStatus.FAILURE):
                return result

            # SIP status is SUCCESS, so it should contain an IE
            ie = self._get_ie_from_sip(sip)
            if ie:
                return self._check_record(ie, RecordType.IE)
            else:
                return CheckResult(
                    fragment_id=sip.Internal.FragmentId,
                    status=SipStatus.FAILURE,
                    timestamp=datetime.now(),
                    failure_message="Accepted SIP without IE found",
                )

        except Exception as e:
            self.log.exception(f"failed to poll for PID {pid}: {e}", pid=pid)
            return None

    def _persist_status(self, record: SipinRecord, result: CheckResult) -> None:
        """If status is success or failure, write to the db."""
        match result.status:
            case SipStatus.IN_PROGRESS:
                return
            case SipStatus.SUCCESS:
                self.db_client.update_sip_mam_success(
                    correlation_id=record.correlation_id,
                    event_timestamp=result.timestamp,
                )
            case SipStatus.FAILURE:
                self.db_client.update_sip_mam_failure(
                    correlation_id=record.correlation_id,
                    event_timestamp=result.timestamp,
                    failure_message=result.failure_message,
                )

    def _poll_mam(self) -> None:
        """Get the PIDs to poll for and poll them."""
        for record in self._sipin_records_to_poll():
            result = self._poll_pid(record.pid)
            if result:
                self.log.debug(
                    f"MAM status for PID {record.pid}: {result.status}",
                    correlation_id=record.correlation_id,
                    pid=record.pid,
                    fragment_id=result.fragment_id,
                    result=result.status,
                )
                self._persist_status(record, result)
            time.sleep(SLEEP_POLL_SECONDS)

    def _is_running(self) -> bool:
        return not self.shutdown.is_set()

    def _get_polling_interval_seconds(self) -> float:
        """Return the polling interval, in seconds."""
        return self.polling_interval_hours * 60 * 60

    def _wait(self) -> None:
        self.log.debug(f"done polling; checking back in {self.polling_interval_hours}h")
        self.shutdown.wait(self._get_polling_interval_seconds())

    def poll(self) -> None:
        """On a fixed schedule, poll MediaHaven for the status of SIPs."""
        while self._is_running():
            try:
                self._poll_mam()
            except Exception as e:
                self.log.exception(f"failure during polling: {e}")
            finally:
                self._wait()


class MamFailuresPoller(MamPoller):
    """MamFailuresPoller is responsible for polling MediaHaven for failed SIPs."""

    def _sipin_records_to_poll(self) -> Iterator[SipinRecord]:
        self.log.debug("looking for recent failed SIPs")
        return self.db_client.select_recent_failed_sips()
