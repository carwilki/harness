from logging import getLogger
from uuid import uuid4

from pyspark.sql import SparkSession

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.Snapshotter import Snapshotter
from harness.snaphotter.SnapshotterFactory import SnapshotterFactory


class HarnessJobManager:
    """
    Harness Manager is responsible for orchestrating the snapshotting process.
    """

    def __init__(
        self, config: HarnessJobConfig, envconfig: EnvConfig, session: SparkSession
    ):
        """
        Configures a new HarnessJobManager for use.
        Args:
            config (HarnessJobConfig): The config that is used to setup the manager
            envconfig (EnvConfig): Exteranl Environment Config
            session (SparkSession): an active spark session
        """
        self.logger = getLogger()
        self.session: SparkSession = session
        self.config: HarnessJobConfig = config
        self._source_snapshoters: dict[str, Snapshotter] = {}
        self._input_snapshoters: dict[str, Snapshotter] = {}
        self._metadataManager: HarnessJobManagerMetaData = HarnessJobManagerMetaData(
            session
        )
        self._env: HarnessJobManagerEnvironment = self._bindenv(envconfig)
        self._configureMetaData()
        self._configureSourceSnaphotters()
        self._configureInputSnapshotters()

    def _bindenv(self, envconfig: EnvConfig) -> HarnessJobManagerEnvironment:
        """
        Binds the environment config to the os environment.
        Args:
            envconfig (EnvConfig): The environment config to bind

        Returns:
            HarnessJobManagerEnvironment: The environment that was bound
        """
        HarnessJobManagerEnvironment.bindenv(envconfig)
        return HarnessJobManagerEnvironment

    def _configureMetaData(self):
        """
        configures the metadata manager repository
        """
        self._metadataManager.create_metadata_table(
            HarnessJobManagerEnvironment.metadata_schema(),
            HarnessJobManagerEnvironment.metadata_table(),
        )
        self._metadataManager.create(self.config)

    def _configureSourceSnaphotters(self):
        """
        configures the source snapshotters
        """
        source: SnapshotConfig
        for source in self.config.sources.values():
            snapshotter = SnapshotterFactory.create(
                snapshot_config=source, session=self.session
            )
            if source.name is not None:
                self._source_snapshoters[source.name] = snapshotter
            else:
                self._source_snapshoters[str(uuid4())] = snapshotter

    def _configureInputSnapshotters(self):
        """
        configures the input snapshotters
        """
        source: SnapshotConfig
        for source in self.config.inputs.values():
            snapshotter = SnapshotterFactory.create(
                snapshot_config=source, session=self.session
            )
            if source.name is not None:
                self._input_snapshoters[source.name] = snapshotter
            else:
                self._source_snapshoters[str(uuid4())] = snapshotter

    def run(self):
        self.snapshot()

    def snapshot(self):
        """
        Takes a snapshot of the data sources and inputs.
        """
        if self.config.version < 1:
            self.logger.info("Taking snapshot V1...")
            self._snapshot(self._source_snapshoters)
            self.logger.info("V1 snapshot completed.")

            self.config.version = 1
            for input in self.config.inputs.values():
                input.version = 1
            self._metadataManager.update(self.config.job_id, self.config)
        elif self.config.version == 1:
            self.logger.info("Taking snapshot V2...")
            self._snapshot(self._source_snapshoters)
            self.logger.info("V2 Source Snapshot completed.")
            self.logger.info("Snapshotting Inputs...")
            self._snapshot(self._input_snapshoters)
            self.logger.info("V2 Input Snapshot completed.")
            self.logger.info("V2 Snapshot completed.")
            self._metadataManager.update(self.config.job_id, self.config)
        else:
            self.logger.info("Snapshot already completed, skipping...")

    def _snapshot(self, snapshotters: dict[str, Snapshotter]):
        for snapshotter in snapshotters.values():
            snapshotter.snapshot()
            self.logger.info(f"Snapshotted {snapshotter.config.name}")
