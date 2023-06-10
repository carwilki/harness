from os import environ as env
from typing import Optional

from harness.config.EnvConfig import EnvConfig


class HarnessJobManagerEnvironment:
    @classmethod
    def bindenv(cls, config: EnvConfig):
        env["__WORKSPACE_URL"] = config.workspace_url
        env["__WORKSPACE_TOKEN"] = config.workspace_token
        env["__CATALOG"] = config.catalog
        env["__METADATA_SCHEMA"] = config.metadata_schema
        env["__METADATA_TABLE"] = config.metadata_table
        env["__JDBC_URL"] = config.jdbc_url
        env["__JDBC_USER"] = config.jdbc_user
        env["__JDBC_PASSWORD"] = config.jdbc_password
        env["__JDBC_NUM_PART"] = config.jdbc_num_part

    @property
    def workspace_url(cls) -> Optional[str]:
        try:
            return env["__WORKSPACE_URL"]
        except KeyError:
            return None

    @property
    def workspace_token(cls) -> Optional[str]:
        try:
            return env["__WORKSPACE_TOKEN"]
        except KeyError:
            return None

    @property
    def catalog(cls) -> Optional[str]:
        try:
            return env["__CATALOG"]
        except KeyError:
            return None

    @property
    def metadata_schema(cls) -> Optional[str]:
        try:
            return env["__METADATA_SCHEMA"]
        except KeyError:
            return None

    @property
    def metadata_table(self) -> Optional[str]:
        try:
            return env["__METADATA_TABLE"]
        except KeyError:
            return None

    @property
    def jdbc_url(cls) -> Optional[str]:
        try:
            return env["__JDBC_URL"]
        except KeyError:
            return None

    @property
    def jdbc_user(cls) -> Optional[str]:
        try:
            return env["__JDBC_USER"]
        except KeyError:
            return None

    @property
    def jdbc_password(cls) -> Optional[str]:
        try:
            return env["__JDBC_PASSWORD"]
        except KeyError:
            return None

    @property
    def jdbc_num_part(cls) -> Optional[int]:
        try:
            return env["__JDBC_NUM_PART"]
        except KeyError:
            return None