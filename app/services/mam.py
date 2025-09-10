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
        return False  # TODO

    @staticmethod
    def _sip_record_to_pid(record: MamRecord):
        filename = record.Descriptive.OriginalFilename
        return filename.removesuffix(".zip")

    @staticmethod
    def _get_archived_date(record: MamRecord) -> datetime:
        try:
            date = record.Administrative.ArchivedDate
            return datetime.fromisoformat(date)
        except Exception:
            return datetime.now()

    def update_mam_state(self):
        pids_in_progress = self.db_client.select_pids_in_progress()
        if len(pids_in_progress) > 0:
            records = self.query_records_by_pids(pids_in_progress)
        else:
            return

        for r in records:
            if self._is_sip_archived(r):
                pid = self._sip_record_to_pid(r)
                timestamp = self._get_archived_date(r)
                self.db_client.update_sip_mam_success(
                    pid=pid,
                    event_timestamp=timestamp,
                )
            elif self._is_sip_failed(r):
                pid = self._sip_record_to_pid(r)
                self.db_client.update_sip_mam_failure(pid)
            else:
                pass

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

    def build(self) -> str:
        return "+(Administrative.DeleteStatus:*)" + "+(" + " ".join(self._clauses) + ")"
