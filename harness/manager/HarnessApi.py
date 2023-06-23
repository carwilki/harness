import csv
from typing import Optional
from harness.config.EnvConfig import EnvConfig
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
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

        if spark is None:
            self._spark = SparkSession.getActiveSession()
        else:
            self._spark = spark

    def getHarnessJobById(self, id: str) -> Optional[HarnessJobManager]:
        meta = HarnessJobManagerMetaData.getJobById(id, self._spark)
        return HarnessJobManager(
            meta, self._harness_job_manager_environment, self._spark
        )

    def createNewHarnessJob(
        self, config: HarnessJobConfig
    ) -> Optional[HarnessJobManager]:
        return HarnessJobManager(
            config=config,
            envconfig=self._harness_job_manager_environment,
            session=self._spark,
        )

    def createHarnessJobFromJSON(self, json: str):
        config = HarnessJobConfig.from_json(json)
        return self.createNewHarnessJob(config, self._spark)

    def createHarnessJobFromCSV(
        self, id: str, path: str
    ) -> Optional[HarnessJobManager]:
        with open(path, "r") as file:
            tableList = csv.DictReader(file)
            sources = dict()
            for table in tableList:
                sc = JDBCSourceConfig(
                    source_table=table["tables"],
                    source_schema=table["source_schema"],
                    source_filter=table["source_filter"],
                )
                tc = TableTargetConfig(
                    target_table=table["target_table"],
                    target_schema="hive_metastore.nzmigration",
                )
                snc = SnapshotConfig(
                    job_id=id, target=tc, source=sc, name=table["tables"]
                )
                sources[table["tables"]] = snc
                hjc = HarnessJobConfig(job_id=id, sources=sources, inputs={})

        return self.createNewHarnessJob(hjc, self._spark)
