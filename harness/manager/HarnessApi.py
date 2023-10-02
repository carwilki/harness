import csv
from typing import Optional

from pyspark.sql import SparkSession

from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from harness.sources.SourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


class HarnessApi:
    def __init__(
        self,
        harness_job_manager_environment: EnvConfig,
        spark: Optional[SparkSession] = None,
    ):
        """

        Args:
            harness_job_manager_environment (EnvConfig): _description_
            spark (Optional[SparkSession], optional): _description_. Defaults to None.
        """
        self._harness_job_manager_environment = harness_job_manager_environment
        HarnessJobManagerEnvironment.bindenv(harness_job_manager_environment)
        self._metadataManager = HarnessJobManagerMetaData(spark)
        if spark is None:
            self._spark = SparkSession.getActiveSession()
        else:
            self._spark = spark
        self._configureMetaData()

    def _configureMetaData(self):
        """
        configures the metadata manager repository
        """
        self._metadataManager.create_metadata_table()

    def resetEverything(self, dry_run: bool = True) -> str:
        """
            resets the entire metadata manager repository.
        Args:
            dry_run (bool, optional): if True, no changes will be made to the database. Defaults to True.

        Returns:
            str: a message describing the result of the reset.
        """
        return self._metadataManager.resetEverything(dry_run)

    def getHarnessJobById(self, id: str) -> Optional[HarnessJobManager]:
        """
            returns the HarnessJobManager object for the given id.
        Args:
            id (str): the id of the HarnessJobManager object to return.

        Returns:
            Optional[HarnessJobManager]: could be None if the given id does not exist.
        """
        meta = HarnessJobManagerMetaData.getJobById(id, self._spark)
        if meta is None:
            return None
        else:
            return HarnessJobManager(meta, self._spark)

    def createHarnessJob(self, config: HarnessJobConfig) -> Optional[HarnessJobManager]:
        """
            creates a new HarnessJobManager object from the given HarnessJobConfig object.
        Args:
            config (HarnessJobConfig): a config that fully describes the HarnessJobManager object to create.

        Returns:
            Optional[HarnessJobManager]: may return None if the given config is invalid.
        """
        return HarnessJobManager(
            config=config,
            session=self._spark,
        )

    def createHarnessJobFromJSON(self, json: str):
        """
            creates a new HarnessJobManager object from the given JSON string.
        Args:
            json (str): a JSON string that describes the HarnessJobManager object to create.

        Returns:
            Optional[HarnessJobManager]: may return None if the given JSON string is invalid.
        """
        config = HarnessJobConfig.from_json(json)
        return self.createNewHarnessJob(config, self._spark)

    def createHarnessJobFromCSV(
        self, id: str, name: str, path: str, sourceType: SourceTypeEnum
    ) -> Optional[HarnessJobManager]:
        # TODO: need to refactor this to a helper class.
        with open(path, "r") as file:
            tableList = csv.DictReader(file)
            sources = dict()
            for table in tableList:
                sc = JDBCSourceConfig(
                    source_table=table["source_table"],
                    source_schema=table["source_schema"],
                    source_filter=table["source_filter"],
                    source_type=sourceType,
                )
                tc = TableTargetConfig(
                    primary_key=table["primary_key"].split("|"),
                    snapshot_target_table=table["snapshot_target_table"],
                    snapshot_target_schema=table["snapshot_target_schema"],
                    test_target_schema=table["test_target_schema"],
                    test_target_table=table["test_target_table"],
                    dev_target_schema=table["dev_target_schema"],
                    dev_target_table=table["dev_target_table"],
                )
                snc = SnapshotConfig(
                    job_id=id,
                    target=tc,
                    source=sc,
                    name=table["name"],
                    isInput=table["is_input"],
                )
                sources[table["name"]] = snc
        hjc = HarnessJobConfig(job_id=id, job_name=name, snapshots=sources)

        return self.createHarnessJob(hjc)

    def deleteHarnessJob(self, id: str) -> str:
        """
            deletes the HarnessJobManager object with the given id.
        Args:
            id (str): the id of the HarnessJobManager object to delete.

        Returns:
            str: a message describing the result of the delete.
        """
        hjm = self.getHarnessJobById(id)
        hjm.destroy()
