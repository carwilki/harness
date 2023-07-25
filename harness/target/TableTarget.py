from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidator import DataFrameValidator
from harness.utils.logger import getLogger


class TableTarget(AbstractTarget):
    def __init__(
        self,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        table_config: TableTargetConfig,
        session: SparkSession,
    ):
        super().__init__(
            harness_job_config=harness_job_config,
            snapshot_config=snapshot_config,
            session=session,
        )
        self.logger = getLogger()
        self.table_config = table_config
        if self.table_config.snapshot_target_schema is None:
            raise Exception("Schema name is required")

        if self.table_config.snapshot_target_table is None:
            raise Exception("Table name is required")

    def write(self, df: DataFrame):
        temptable = f"{str(uuid4()).replace('-','')}_data"
        df.createOrReplaceTempView(temptable)
        table = f"{self.table_config.snapshot_target_schema}.{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}_V{self.snapshot_config.version + 1}"  # noqa: E501
        if not self.session.catalog.tableExists(
            f"{self.table_config.snapshot_target_schema}.{self.table_config.snapshot_target_table}"
        ):
            self.session.sql(
                f"""create table {table}
                as select * from {temptable}"""
            )
        else:
            self.session.sql(f"truncate table {table}")
            self.session.sql(f"insert into {table} select * from {temptable}")

    def setup_test_target(self):
        catalog = self.session.catalog
        ts = self.table_config.test_target_schema
        tt = self.table_config.test_target_table
        ss = self.table_config.snapshot_target_schema
        st = f"{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}"
        if catalog.tableExists(
            f"{self.table_config.test_target_schema}.{self.table_config.test_target_table}"
        ):
            self.session.sql(
                f"truncate table {self.table_config.test_target_schema}.{self.table_config.test_target_table}"
            )
            if self.snapshot_config.isInput:
                self.session.sql(f"insert into {ts}.{tt} select * from {ss}.{st}_V2")
            else:
                self.session.sql(f"insert into {ts}.{tt} select * from {ss}.{st}_V1")
        else:
            if self.snapshot_config.isInput:
                self.session.sql(
                    f"create table {ts}.{tt} as select * from {ss}.{st}_V2"
                )
            else:
                self.session.sql(
                    f"create table {ts}.{tt} as select * from {ss}.{st}_V1"
                )

    def validate_results(self):
        ts = self.table_config.test_target_schema
        tt = self.table_config.test_target_table
        ss = self.table_config.snapshot_target_schema
        st = (
            f"{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}_V2"
        )
        
        filter = ""
        if self.table_config.validation_filter is not None and self.table_config.validation_filter != "":
            filter = f" where {self.table_config.validation_filter}"
            
        validator = DataFrameValidator()
        results = self.session.sql(f"select * from {ts}.{tt}{filter}")
        base = self.session.sql(f"select * from {ss}.{st}{filter}")
        
        self.logger.info(f"Validating results in {ts}.{tt} againsts {ss}.{st}")
        
        return validator.validateDF(
            f"{self.harness_job_config.job_name}_{tt}",
            results,
            base,
            self.table_config.primary_key,
            self.session,
        )
