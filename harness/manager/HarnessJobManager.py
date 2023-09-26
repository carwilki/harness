from uuid import uuid4

from pyspark.sql import SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.snaphotter.Snapshotter import Snapshotter
from harness.snaphotter.SnapshotterFactory import SnapshotterFactory
from harness.target.TableTargetConfig import TableTargetConfig
from harness.utils.logger import getLogger
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


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

    def setupTestData(self):
        """
        Sets up the test data for the sources
        loops throught the snapshots and call the
        setup test data method for each snapshot. this moves v1 to the refine table
        if its not an input and v2 if it is an input.
        """
        for snapshot in self.snapshoters.values():
            snapshot.setupTestData()

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
        for snapshotter in snapshotters.values():
            if snapshotter.config.version == self.config.version:
                snapshotter.snapshot()
                self._metadataManager.update(self.config)
                self._logger.debug(f"Snapshotted {snapshotter.config.name}")
            else:
                self._logger.info(f"skipping snapshot:{snapshotter.config.name} already taken")

    def validateResults(self) -> dict[str, DataFrameValidatorReport]:
        if self.config.validation_reports is None:
            self.config.validation_reports = {}

        for snapshotter in self.snapshoters.values():
            validation_report = snapshotter.validateResults()
            self.config.validation_reports[snapshotter.config.name] = validation_report

        self._metadataManager.update(self.config)

        return self.config.validation_reports

    def updateAllTargetSchema(self, schema: str):
        for snapshotter in self.snapshoters.values():
            snapshotter.updateTargetSchema(schema)

        self._metadataManager.update(self.config)

    def updateAllTargetTable(self, table: str):
        for snapshotter in self.snapshoters.values():
            snapshotter.updateTargetTable(table)

        self._metadataManager.update(self.config)

    def updateTargetSchema(self, snapshotName: str, schema: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateTargetSchema(schema)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateTargetTable(self, snapshotName: str, table: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateTargetTable(table)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateSnapshotSchema(self, snapshotName: str, schema: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateSnapshotSchema(schema)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def updateSnapshotTable(self, snapshotName: str, table: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.updateSnapshotTable(table)
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def disableSnapshot(self, snapshotName: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.disable()
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def enableSnapshot(self, snapshotName: str):
        snapshotter = self.snapshoters[snapshotName]

        if snapshotter is not None:
            snapshotter.enable()
            self._metadataManager.update(self.config)
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def cleanupValidationReports(self) -> str:
        clean_up = self._metadataManager.cleanupValidationReports(self.config.job_name)
        self._logger.debug(f"Cleaned up validation reports: {clean_up}")
        return clean_up

    def markInputsSnapshots(self, names: list[str]):
        for name in names:
            snapshotter = self.snapshoters.get(name)
            if snapshotter is not None:
                snapshotter.markAsInput()

        self._metadataManager.update(self.config)

    def updateValidaitonFilter(self, snapshotName: str, filter: str):
        ss = self.snapshoters.get(snapshotName)
        ss.updateValidaitonFilter(filter)
        self._metadataManager.update(self.config)

    def updateAllValidationFilters(self, filter: str):
        for ss in self.snapshoters.values():
            ss.updateValidaitonFilter(filter)
        self._metadataManager.update(self.config)

    def runSingleValidation(self, snapshotName: str) -> DataFrameValidatorReport:
        ss = self.snapshoters.get(snapshotName)
        if ss is not None:
            report = ss.validateResults()
            self.config.validation_reports[snapshotName] = report
            return report
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def getReport(self, snapshotName) -> DataFrameValidatorReport:
        ss = self.config.validation_reports[snapshotName]
        if ss is not None:
            return ss
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def getTargetConfigForSnapshot(self, snapshotName: str) -> TableTargetConfig:
        ss = self.config.snapshots.get(snapshotName)
        if ss is not None:
            return ss.target
        else:
            raise ValueError(f"Snapshot {snapshotName} does not exist")

    def getSnapshotTable(self, snapshotName: str, version: int) -> str:
        ss = self.snapshoters.get(snapshotName)
        if ss is not None:
            return ss.target.getSnapshotTableName(version)

    def update(self):
        self._metadataManager.update(self.config)
        
    def destroy(self):
        for snapshotter in self.snapshoters.values():
            snapshotter.destroy()
            
        self._metadataManager.delete(self.config.job_id)
