from os import environ as env
from harness.config.config import HarnessJobConfig, SnapshotConfig
from harness.config.env import EnvConfig
from harness.manager.manager_data import ManagerMetaData
from harness.snaphotter.snapshotter import AbstractSnapshotter, SnapshotterFactory
from pyspark.sql import SparkSession
from uuid import uuid4


class HarnessJobManagerEnvironment:
    def __init__(self, config: EnvConfig) -> None:
        env["__WORKSPACE_URL"] = config.workspace_url
        env["__WORKSPACE_TOKEN"] = config.workspace_token
        env["__CATALOG"] = config.catalog
        env["__METADATA_SCHEMA"] = config.metadata_schema
        env["__METADATA_TABLE"] = config.metadata_table

    @property
    def workspace_url(self):
        return env["__WORKSPACE_URL"]

    @property
    def workspace_token(self):
        return env["__WORKSPACE_TOKEN"]

    @property
    def catalog(self):
        return env["__CATALOG"]

    @property
    def metadata_schema(self):
        return env["__METADATA_SCHEMA"]

    @property
    def metadata_table(self):
        return env["__METADATA_TABLE"]


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
        return HarnessJobManagerEnvironment(envconfig)

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