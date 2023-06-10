from os import environ as env
from typing import Optional
from harness.config.config import HarnessJobConfig, SnapshotConfig
from harness.config.env import EnvConfig
from harness.manager.manager_data import ManagerMetaData
from harness.snaphotter.snapshotter import AbstractSnapshotter, SnapshotterFactory
from pyspark.sql import SparkSession
from uuid import uuid4
from logging import Logger as logger


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


class HarnessJobManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(
        self, config: HarnessJobConfig, envconfig: EnvConfig, session: SparkSession
    ):
        self.config: HarnessJobConfig = config
        self._snapshoters: dict[str, AbstractSnapshotter] = {}
        self._metadataManager = ManagerMetaData(session)
        self._env = self._bindenv(envconfig)
        self._configureMetaData()
        self._configureSnapshoters()

    def _bindenv(self, envconfig: EnvConfig) -> HarnessJobManagerEnvironment:
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return HarnessJobManagerEnvironment

    def _configureMetaData(self):
        self._metadataManager.create_metadata_table(self.config)
        self._metadataManager.create(self.config)

    def _configureSnapshoters(self):
        source: SnapshotConfig
        for source in self.config.sources.values():
            snapshotter = SnapshotterFactory.create(source)
            if source.name is not None:
                self._snapshoters[source.name] = snapshotter
            else:
                self._snapshoters[str(uuid4())] = snapshotter

    def snapshot(self):
        for snapshotter in self._snapshoters.values():
            if snapshotter.config.version <= 1:
                snapshotter.take_snapshot()
                self._metadataManager.update(self.config)
            else:
                logger.info("Snapshot already completed, skipping...")
