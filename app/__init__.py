from enum import StrEnum, auto
from dataclasses import dataclass
from structlog.stdlib import BoundLogger
from types import SimpleNamespace

APP_NAME = "sipin-sip-state-updater"
CONFIG_FILE = "config.yml"


class ConfigError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class SipStatus(StrEnum):
    IN_PROGRESS = auto()
    SUCCESS = auto()
    FAILURE = auto()


@dataclass
class SipinRecord:
    pid: str
    correlation_id: str


Logger = BoundLogger
MamRecord = SimpleNamespace
