# future imports
from __future__ import annotations

# stdlib imports
from dataclasses import dataclass

# meemoo imports
from viaa.configuration import ConfigParser

# local imports
from app import ConfigError, CONFIG_FILE

# type imports
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self


__all__ = [
    "MediaHavenConfig",
]


@dataclass(frozen=True)
class MediaHavenConfig:
    """Validation of mediahaven section of app config."""

    client_id: str
    client_secret: str
    username: str
    password: str
    mh_base_url: str

    @classmethod
    def from_config_parser(cls, config: ConfigParser) -> Self:
        try:
            try:
                mediahaven = config.app_cfg["mediahaven"]
            except ValueError:
                raise ConfigError("no mediahaven section in app config ({CONFIG_FILE})")
            return cls(
                client_id=mediahaven["id"],
                client_secret=mediahaven["secret"],
                username=mediahaven["username"],
                password=mediahaven["password"],
                mh_base_url=mediahaven["url"],
            )
        except ValueError as e:
            raise ConfigError(
                f"missing key `{e}' in mediahaven section of app config ({CONFIG_FILE})"
            )
