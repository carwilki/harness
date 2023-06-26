from os import environ as env
from typing import Optional

from harness.config.EnvConfig import EnvConfig


class HarnessJobManagerEnvironment(object):
    _config: Optional[EnvConfig] = None

    @classmethod
    def bindenv(cls, config: EnvConfig):
        cls._config = config

    @classmethod
    def workspace_url(cls) -> Optional[str]:
        return cls._config.workspace_url

    @classmethod
    def workspace_token(cls) -> Optional[str]:
        return cls._config.workspace_token

    @classmethod
    def catalog(cls) -> Optional[str]:
        return cls._config.catalog

    @classmethod
    def metadata_schema(cls) -> Optional[str]:
        return cls._config.metadata_schema

    @classmethod
    def metadata_table(self) -> Optional[str]:
        return self._config.metadata_table

    @classmethod
    def snapshot_schema(cls) -> Optional[str]:
        return cls._config.snapshot_schema

    @classmethod
    def getConfig(cls) -> dict:
        return dict(cls._config)
