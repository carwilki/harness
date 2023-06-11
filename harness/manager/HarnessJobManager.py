from logging import Logger as logger
from uuid import uuid4

from pyspark.sql import SparkSession

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.snaphotter.SnapshotterFactory import SnapshotterFactory


class HarnessJobManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(
        self, config: HarnessJobConfig, envconfig: EnvConfig, session: SparkSession
    ):
        self.session: SparkSession = session
        self.config: HarnessJobConfig = config
        self._snapshoters: dict[str, AbstractSnapshotter] = {}
        self._metadataManager = HarnessJobManagerMetaData(session)
        self._env = self._bindenv(envconfig)
        self._configureMetaData()
        self._configureSnapshoters()

    def _bindenv(self, envconfig: EnvConfig) -> HarnessJobManagerEnvironment:
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return HarnessJobManagerEnvironment

    def _configureMetaData(self):
        self._metadataManager.create_metadata_table(
            HarnessJobManagerEnvironment.metadata_schema(),
            HarnessJobManagerEnvironment.metadata_table(),
        )
        self._metadataManager.create(self.config)

    def _configureSnapshoters(self):
        source: SnapshotConfig
        for source in self.config.sources.values():
            snapshotter = SnapshotterFactory.create(
                snapshot_config=source, session=self.session
            )
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
