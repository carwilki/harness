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
from harness.sources.JDBCSourceConfig import JDBCSourceConfig
from harness.target.TableTargetConfig import TableTargetConfig


class HarnessApi:
    def __init__(
        self,
        harness_job_manager_environment: EnvConfig,
        spark: Optional[SparkSession] = None,
    ):
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
        return self._metadataManager.resetEverything(dry_run)

    def getHarnessJobById(self, id: str) -> Optional[HarnessJobManager]:
        meta = HarnessJobManagerMetaData.getJobById(id, self._spark)
        if meta is None:
            return None
        else:
            return HarnessJobManager(meta, self._spark)

    def createHarnessJob(self, config: HarnessJobConfig) -> Optional[HarnessJobManager]:
        return HarnessJobManager(
            config=config,
            session=self._spark,
        )

    def createHarnessJobFromJSON(self, json: str):
        config = HarnessJobConfig.from_json(json)
        return self.createNewHarnessJob(config, self._spark)

    def createHarnessJobFromCSV(
        self, id: str, path: str, sourceType: SourceTypeEnum
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
                    snapshot_target_table=table["snapshot_target_table"],
                    snapshot_target_schema=table["snapshot_target_schema"],
                    test_target_schema=table["test_target_schema"],
                    test_target_table=table["test_target_table"],
                )
                snc = SnapshotConfig(
                    job_id=id, target=tc, source=sc, name=table["name"]
                )
                sources[table["name"]] = snc
        hjc = HarnessJobConfig(
            job_id=id, job_name="databricks_jdbc_test", sources=sources
        )

        return self.createHarnessJob(hjc)
