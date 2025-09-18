from structlog.stdlib import BoundLogger
from types import SimpleNamespace

APP_NAME = "sipin-sip-state-updater"
CONFIG_FILE = "config.yml"


class ConfigError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


Logger = BoundLogger
MamRecord = SimpleNamespace
