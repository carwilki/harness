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

    def getSnapshotTableName(self, version: int) -> str:
        return f"{self.table_config.snapshot_target_schema}.{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}_V{version}"

    def write(self, df: DataFrame):
        temptable = f"{str(uuid4()).replace('-','')}_data"
        df.createOrReplaceTempView(temptable)
        table = self.getSnapshotTableName(self.snapshot_config.version + 1)
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
        st = self.table_config.snapshot_target_table
        v1 = self.session.sql(f"select * from {self.getSnapshotTableName(1)}") 
        v2 = self.session.sql(f"select * from {self.getSnapshotTableName(2)}") 

        validator = DataFrameValidator()
        refine_q = f"select * from {ts}.{tt}"
        self.logger.debug(f"refine query: {refine_q}")

        results = self.session.sql(refine_q)
        base = v1.unionAll(v2).distinct()
        (compare, base) = self._only_what_is_shared(results, base)
        self.logger.info(f"Validating results in {ts}.{tt} againsts {ss}.{st}")

        return validator.validateDF(
            f"{self.harness_job_config.job_name}_{tt}",
            compare,
            base,
            self.table_config.primary_key,
            self.session,
        )

    def _only_what_is_shared(
        self, compare: DataFrame, base: DataFrame
    ) -> (DataFrame, DataFrame):
        keys = self.table_config.primary_key
        compare_keys = compare.select(keys)
        base_keys = base.select(keys)
        intersect = compare_keys.intersect(base_keys)
        compare_final = compare.join(intersect, keys, "inner")
        base_final = base.join(intersect, keys, "inner")
        return compare_final, base_final
