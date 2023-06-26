import csv
from typing import Optional
from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.manager.HarnessJobManager import HarnessJobManager
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.manager.HarnessJobManagerMetaData import HarnessJobManagerMetaData
from pyspark.sql import SparkSession

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
        with open(path, "r") as file:
            tableList = csv.DictReader(file)
            sources = dict()
            for table in tableList:
                sc = JDBCSourceConfig(
                    source_table=table["tables"],
                    source_schema=table["source_schema"],
                    source_filter=table["source_filter"],
                    source_type=sourceType,
                )
                tc = TableTargetConfig(
                    target_table=table["target_table"],
                    target_schema=HarnessJobManagerEnvironment.metadata_schema(),
                )
                snc = SnapshotConfig(
                    job_id=id, target=tc, source=sc, name=table["tables"]
                )
                sources[table["tables"]] = snc
                hjc = HarnessJobConfig(job_id=id, sources=sources, inputs={})

        return self.createHarnessJob(hjc)
