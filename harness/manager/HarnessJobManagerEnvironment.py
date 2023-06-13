from os import environ as env
from typing import Optional

from harness.config.EnvConfig import EnvConfig


class HarnessJobManagerEnvironment:
    @classmethod
    def bindenv(cls, config: EnvConfig):
        env["__WORKSPACE_URL"] = config.workspace_url
        env["__WORKSPACE_TOKEN"] = config.workspace_token
        if config.catalog is not None:
            env["__CATALOG"] = config.catalog
        else:
            env["__CATALOG"] = ""
        env["__HARNESS_METADATA_SCHEMA"] = config.metadata_schema
        env["__HARNESS_METADATA_TABLE"] = config.metadata_table
        env["__HARNESS_SNAPSHOT_SCHEMA"] = config.snapshot_schema
        env["__HARNESS_SNAPSHOT_TABLE_POSTFIX"] = config.snapshot_table_post_fix
        env["__JDBC_URL"] = config.jdbc_url
        env["__JDBC_USER"] = config.jdbc_user
        env["__JDBC_PASSWORD"] = config.jdbc_password
        env["__JDBC_NUM_PART"] = str(config.jdbc_num_part)

    @classmethod
    def workspace_url(cls) -> Optional[str]:
        try:
            if env["__WORKSPACE_URL"] != "":
                return env["__WORKSPACE_URL"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def workspace_token(cls) -> Optional[str]:
        try:
            if env["__WORKSPACE_TOKEN"] != "":
                return env["__WORKSPACE_TOKEN"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def catalog(cls) -> Optional[str]:
        try:
            if env["__CATALOG"] != "":
                return env["__CATALOG"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def metadata_schema(cls) -> Optional[str]:
        try:
            if env["__HARNESS_METADATA_SCHEMA"] != "":
                return env["__HARNESS_METADATA_SCHEMA"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def metadata_table(self) -> Optional[str]:
        try:
            if env["__HARNESS_METADATA_TABLE"] != "":
                return env["__HARNESS_METADATA_TABLE"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def jdbc_url(cls) -> Optional[str]:
        try:
            if env["__JDBC_URL"] != "":
                return env["__JDBC_URL"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def jdbc_user(cls) -> Optional[str]:
        try:
            if env["__JDBC_USER"] != "":
                return env["__JDBC_USER"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def jdbc_password(cls) -> Optional[str]:
        try:
            if env["__JDBC_PASSWORD"] != "":
                return env["__JDBC_PASSWORD"]
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def jdbc_num_part(cls) -> Optional[int]:
        try:
            if env["__JDBC_NUM_PART"] != "":
                return int(env["__JDBC_NUM_PART"])
            else:
                return None
        except KeyError:
            return None

    @classmethod
    def snapshot_schema(cls) -> Optional[str]:
        try:
            if env["__HARNESS_SNAPSHOT_SCHEMA"] != "":
                return env["__HARNESS_SNAPSHOT_SCHEMA"]
            else:
                return None
        except KeyError:
            return None

    def snapshot_table_postfix(cls) -> Optional[str]:
        try:
            if env["__HARNESS_SNAPSHOT_TABLE_POSTFIX"] != "":
                return env["__HARNESS_SNAPSHOT_TABLE_POSTFIX"]
            else:
                return None
        except KeyError:
            return None
