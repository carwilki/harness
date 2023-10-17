from uuid import uuid4

from pyspark.sql import SparkSession
from harness.config.EnvironmentEnum import EnvironmentEnum

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.Snapshotter import Snapshotter
from harness.snaphotter.SnapshotterFactory import SnapshotterFactory
from harness.target.TableTargetConfig import TableTargetConfig
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
        self.snapshoters: dict[str, Snapshotter] = {}
        self._loadExistingMetaDataIfExists()
        # this will overwrite any existing inputs if there is an existing
        # job config
        self._configureSourceSnaphotters()

    def _loadExistingMetaDataIfExists(self):
        """
        loads the existing metadata if it exists
        """
        existing_config: HarnessJobConfig = self._metadataManager.get(
            self.config.job_id
        )
        if existing_config is not None:
            # if we found an existing config then swap it out
            self.config = existing_config
            self._logger.debug(
                f"Found existing metadata for job {self.config.job_id}, loading..."
            )
        else:
            # else we create a new one based on the current config
            self._logger.debug(
                f"Could not find existing metadata for job {self.config.job_id}, creating new..."
            )
            self._metadataManager.create(self.config)

    def _configureSourceSnaphotters(self):
        """
        configures the source snapshotters
        """
        for source in self.config.snapshots.values():
            snapshotter = SnapshotterFactory.create(
                harness_config=self.config, snapshot_config=source, session=self.session
            )
            if source.name is not None:
                self.snapshoters[source.name] = snapshotter
            else:
                self.snapshoters[str(uuid4())] = snapshotter

    def setupTestData(self, env: EnvironmentEnum | None = None):
        """
        Sets up the test data for the sources
        loops throught the snapshots and call the
        setup test data method for each snapshot. this moves v1 to the refine table
        if its not an input and v2 if it is an input.
        """
        if env is None:
            env = EnvironmentEnum.QA
        for snapshot in self.snapshoters.values():
            snapshot.setupTestDataForEnv(env)

    def snapshot(self):
        """
        Takes a snapshot of the data sources and inputs.
        """
        if self.config.version <= 0:
            self.config.version = 0
            self._logger.debug("Taking snapshot V1...")
            self._snapshot(self.snapshoters)
            self._logger.debug("V1 snapshot completed.")
            self.config.version = 1
            self._metadataManager.update(self.config)
        elif self.config.version == 1:
            self._logger.debug("Taking snapshot V2...")
            self._snapshot(self.snapshoters)
            self._logger.debug("V2 Snapshot completed.")
            self.config.version = 2
            self._metadataManager.update(self.config)
        else:
            self._logger.debug("Snapshot already completed, skipping...")

    def _snapshot(self, snapshotters: dict[str, Snapshotter]):
        """
            internal method to take a snapshot of the data sources and inputs.
        Args:
            snapshotters (dict[str, Snapshotter]): Dictionary of Snapshotter objects
        """
        for snapshotter in snapshotters.values():
            if snapshotter.config.version == self.config.version:
                snapshotter.snapshot()
                self._metadataManager.update(self.config)
                self._logger.debug(f"Snapshotted {snapshotter.config.name}")
            else:
                self._logger.info(
                    f"skipping snapshot:{snapshotter.config.name} already taken"
                )

    def updateTestSchema(self, snapshotName: str, schema: str):
        """
        Updates the test schema for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            schema (str): The schema of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateTestTargetSchema(schema)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateTestTable(self, snapshotName: str, table: str):
        """
        Updates the test table for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            table (str): The table of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateTestTargetTable(table)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateDevSchema(self, snapshotName: str, schema: str):
        """
        Updates the dev schema for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            schema (str): The schema of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateDevTargetSchema(schema)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateDevTable(self, snapshotName: str, table: str):
        """
        Updates the dev table for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            table (str): The table of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateDevTargetTable(table)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateSnapshotSchema(self, snapshotName: str, schema: str):
        """
        Updates the snapshot schema for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            schema (str): The schema of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateSnapshotSchema(schema)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateSnapshotTable(self, snapshotName: str, table: str):
        """
        Updates the snapshot table for a given snapshot.
        Args:
            snapshotName (str): The name of the snapshot
            table (str): The table of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateSnapshotTable(table)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def disableSnapshot(self, snapshotName: str):
        """
        Disables a snapshot
        Args:
            snapshotName (str): The name of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.disable()
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def enableSnapshot(self, snapshotName: str):
        """
        Enables a snapshot
        Args:
            snapshotName (str): The name of the snapshot
        """
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.enable()
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def getTargetConfigForSnapshot(self, snapshotName: str) -> TableTargetConfig:
        """
        Returns the target config for a given snapshot
        Args:
            snapshotName (str): The name of the snapshot
        Returns:
            TableTargetConfig: The target config for the snapshot
        """
        ss = self.config.snapshots.get(snapshotName)
        if ss is not None:
            return ss.target
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def getSnapshotTable(self, snapshotName: str, version: int) -> str:
        """
        Returns the table name for a given snapshot
        Args:
            snapshotName (str): The name of the snapshot
            version (int): The version of the snapshot
        Returns:
            str: The table name for the snapshot
        """
        ss = self.snapshoters.get(snapshotName)
        if ss is not None:
            return ss.target.getSnapshotTableName(version)

    def update(self):
        """
        Updates the metadata for the job
        """
        self._logger.debug("Updating metadata...")
        self._metadataManager.update(self.config)
        self._logger.debug("Metadata updated.")

    def destroy(self):
        """
        Destroys the job manager
        """
        self._logger.debug("Destroying metadata...")
        for snapshotter in self.snapshoters.values():
            snapshotter.destroy()

        self._metadataManager.delete(self.config.job_id)
