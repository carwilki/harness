from typing import Optional
from uuid import uuid4
from pyspark.sql import DataFrame, SparkSession
from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig

from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig


class TableTarget(AbstractTarget):
    def __init__(
        self,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        config: TableTargetConfig,
        session: SparkSession,
    ):
        super().__init__(
            harness_job_config=harness_job_config,
            snapshot_config=snapshot_config,
            session=session,
        )
        self.config = config

    def write(self, df: DataFrame):
        if self.config.snapshot_target_schema is None:
            raise Exception("Schema name is required")

        if self.config.snapshot_target_table is None:
            raise Exception("Table name is required")

        temptable = f"{str(uuid4()).replace('-','')}_data"
        df.createOrReplaceTempView(temptable)
        table = f"{self.config.snapshot_target_schema}.{self.harness_job_config.job_name}_{self.config.snapshot_target_table}_V{self.snapshot_config.version}"
        if not self.session.catalog.tableExists(
            f"{self.config.snapshot_target_schema}.{self.config.snapshot_target_table}"
        ):
            self.session.sql(
                f"""create table {table} 
                as select * from {temptable}"""
            )
        else:
            self.session.sql(f"truncate table {table}")
            self.session.sql(f"insert into {table} select * from {temptable}")
