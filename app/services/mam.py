# future imports
from __future__ import annotations

# stdlib imports
from datetime import datetime

# meemoo imports
from mediahaven import MediaHaven
from mediahaven.mediahaven import AcceptFormat
from mediahaven.oauth2 import ROPCGrant
from mediahaven.resources.base_resource import MediaHavenPageObjectJSON
from viaa.configuration import ConfigParser

# local imports
from app.config import MediaHavenConfig

# type imports
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from app import MamRecord
    from typing import Self


class MamPoller:
    """MamPoller is responsible for polling MediaHaven."""

    def __init__(
        self,
        db_client,
        log,
        shutdown,
        mam_client: MediaHaven,
    ) -> None:
        self.mam_client = mam_client
        self.db_client = db_client
        self.log = log
        self.shutdown = shutdown

    @classmethod
    def from_mediahaven_config(
        cls,
        config: MediaHavenConfig,
        **kwargs,
    ) -> Self:
        return cls(
            mam_client=cls.get_mediahaven_client(config),
            **kwargs,
        )

    @classmethod
    def from_config_parser(cls, config: ConfigParser, **kwargs) -> Self:
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
    def _get_mediahaven_pids_query(
        pids: list[str],
    ) -> str:
        """Get MediaHaven query by PIDs."""
        return MediaHavenQuery().pids(pids).build()

    @staticmethod
    def _get_records_from_page_object(
        page: MediaHavenPageObjectJSON,
    ) -> list[MamRecord]:
        n: int = cast(int, page.total_nr_of_results)
        records: list[MamRecord]
        if n > 0:
            records = [r for r in page.as_generator()]
        else:
            records = []
        return records

    def query_records_by_pids(
        self,
        pids: list[str],
    ) -> list[MamRecord]:
        """
        Looks for MediaHaven records by PID.

        Parameters:
            pids {list[str} -- a list of PIDs

        Returns:
            records {list[MamRecord]}
        """
        query = self._get_mediahaven_pids_query(pids)
        result = self.mam_client.records.search(
            accept_format=AcceptFormat.JSON,
            q=query,
        )
        return self._get_records_from_page_object(result)

    @staticmethod
    def _is_sip_archived(record: MamRecord):
        try:
            return (
                record.Internal.ArchiveStatus == "completed"
                and record.Administrative.RecordStatus == "Published"
            )
        except Exception:
            return False

    @staticmethod
    def _is_sip_failed(record: MamRecord):
        try:
            return (
                record.Internal.ArchiveStatus == "failed"
                or record.Administrative.RecordStatus == "Rejected"
            )
        except Exception:
            return False

    @staticmethod
    def _sip_record_to_pid(record: MamRecord) -> str:
        filename = record.Descriptive.OriginalFilename
        return filename.removesuffix(".zip")

    def _get_archived_date(self, record: MamRecord) -> datetime:
        try:
            date = record.Administrative.ArchivedDate
            return datetime.fromisoformat(date)
        except Exception:
            return datetime.now()

    def _get_rejection_date(self, record: MamRecord) -> datetime:
        try:
            date = record.Administrative.RejectionDate
            return datetime.fromisoformat(date)
        except Exception as e:
            self.log.error(f"_get_rejection_date error: {e}")
            return datetime.now()

    @staticmethod
    def _get_failure_message(record: MamRecord) -> Optional[str]:
        try:
            rejections = record.Administrative.RecordRejections.Rejection
            message = "\n".join([r.Motivation for r in rejections])
            return message
        except Exception:
            return None

    def _check_record_status(self, record: MamRecord) -> None:
        if self._is_sip_archived(record):
            pid = self._sip_record_to_pid(record)
            timestamp = self._get_archived_date(record)
            self.db_client.update_sip_mam_success(
                pid=pid,
                event_timestamp=timestamp,
            )
        elif self._is_sip_failed(record):
            pid = self._sip_record_to_pid(record)
            timestamp = self._get_rejection_date(record)
            self.db_client.update_sip_mam_failure(
                pid=pid,
                event_timestamp=timestamp,
                failure_message=self._get_failure_message(record),
            )
        else:
            pass

    def update_mam_state(self):
        pids_in_progress = self.db_client.select_pids_in_progress()
        if len(pids_in_progress) > 0:
            records = self.query_records_by_pids(pids_in_progress)
        else:
            return

        # MediaHaven status are in flux.  ArchiveStatus seems to be a
        # legacy field.  What we call "archived" seems to loosely
        # correspond to MediaHaven's "published", with the caveat that
        # MediaHaven items are auto-published, which seems fragile to
        # depend upon.  MediaHaven has an archived state, which is a
        # final black hole state (removed from view; admin-only
        # access) without a meemoo analogue.
        #
        # v25.1.152
        # ---------
        # ArchiveStatus
        #   This is an enum (on_ingest_tape, in_progress, failed,
        #   on_disk, on_tape, completed).
        #
        #   success: completed
        #   failure: failed
        #
        # RecordStatus:
        #   Records go through 3 phases (Concept, Published, Archive);
        #   their initial status is New/Draft.Valid
        #
        #   success: Published
        #   failure: Draft.Invalid, Rejected, RejectedForCorrection,
        #            ApprovedForDestruction, Destructed, Archived
        for r in records:
            try:
                self._check_record_status(r)
            except Exception as e:
                self.log.error(f"failed to check status of record: {e}")

    def poll(self):
        try:
            while not self.shutdown.is_set():
                self.update_mam_state()
                self.shutdown.wait(60)  # TODO: set to 60 minutes, in seconds
        except Exception as e:
            pass


class MediaHavenQuery:
    """
    Simple MediaHaven query builder

    Only supports generating a single combined query, combining all search
    terms, and looking for results containing either one of them.
    """

    def __init__(self) -> None:
        self._clauses: list[str] = []

    def pids(self, pids: list[str]) -> Self:
        if not pids:
            raise ValueError("no pids to build MediaHaven query with")
        for p in pids:
            self._clauses.append(f'OriginalFilename:"{p}.zip"')
        return self

    def get_clauses(self) -> list[str]:
        return self._clauses

    def build(self) -> str:
        if not (clauses := self.get_clauses()):
            ValueError("No clauses to build MediaHaven query with")
        return (
            "+(Administrative.DeleteStatus:*)"
            + "+(Internal.IsInIngestSpace:*)"
            + "+("
            + " ".join(clauses)
            + ")"
        )
