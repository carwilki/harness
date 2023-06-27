from uuid import uuid4

from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.manager.TestDataManger import TestDataManager
from harness.snaphotter.Snapshotter import Snapshotter
from harness.snaphotter.SnapshotterFactory import SnapshotterFactory
from harness.utils.logger import getLogger


class HarnessJobManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(self, config: HarnessJobConfig, session: SparkSession):
        """
        Configures a new HarnessJobManager for use.
        Args:
            config (HarnessJobConfig): The config that is used to setup the manager
            envconfig (EnvConfig): Exteranl Environment Config
            session (SparkSession): an active spark session
        """
        self._logger = getLogger()
        self.session: SparkSession = session
        self.config: HarnessJobConfig = config
        self._metadataManager: HarnessJobManagerMetaData = HarnessJobManagerMetaData(
            session
        )
        self.__loadExistingMetaDataIfExists()
        self._source_snapshoters: dict[str, Snapshotter] = {}
        self._input_snapshoters: dict[str, Snapshotter] = {}
        # this will overwrite any existing inputs if there is an existing
        # job config
        self.__configureSourceSnaphotters()

    def __loadExistingMetaDataIfExists(self):
        """
        loads the existing metadata if it exists
        """
        existing_config: HarnessJobConfig = self._metadataManager.get(
            self.config.job_id
        )
        if existing_config is not None:
            # if we found an existing config then swap it out
            self.config = existing_config
            self._logger.info(
                f"Found existing metadata for job {self.config.job_id}, loading..."
            )
        else:
            # else we create a new one based on the current config
            self._logger.info(
                f"Could not find existing metadata for job {self.config.job_id}, creating new..."
            )
            self._metadataManager.create(self.config)

    def __configureSourceSnaphotters(self):
        """
        configures the source snapshotters
        """
        for source in self.config.sources.values():
            snapshotter = SnapshotterFactory.create(
                snapshot_config=source, session=self.session
            )
            if source.name is not None:
                self._source_snapshoters[source.name] = snapshotter
            else:
                self._source_snapshoters[str(uuid4())] = snapshotter

    def resetDataForTestRun(self):
        for snapshot in self._source_snapshoters.values():
            TestDataManager.configureJDBCSourceForTest(
                snapshot.source, snapshot.target, self.session, isBase=True
            )

    def snapshot(self):
        """
        Takes a snapshot of the data sources and inputs.
        """
        if self.config.version < 1:
            self._logger.info("Taking snapshot V1...")
            self.__snapshot(self._source_snapshoters)
            self._logger.info("V1 snapshot completed.")

            self.config.version = 1
            for input in self.config.inputs.values():
                input.version = 1
            self._metadataManager.update(self.config)
        elif self.config.version == 1:
            self._logger.info("Taking snapshot V2...")
            self.__snapshot(self._source_snapshoters)
            self._logger.info("V2 Source Snapshot completed.")
            self._logger.info("Snapshotting Inputs...")
            self.__snapshot(self._input_snapshoters)
            self._logger.info("V2 Input Snapshot completed.")
            self._logger.info("V2 Snapshot completed.")
            self._metadataManager.update(self.config)
        else:
            self._logger.info("Snapshot already completed, skipping...")

    def __snapshot(self, snapshotters: dict[str, Snapshotter]):
        for snapshotter in snapshotters.values():
            snapshotter.snapshot()
            self._metadataManager.update(snapshotter.config)
            self._logger.info(f"Snapshotted {snapshotter.config.name}")
