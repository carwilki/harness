from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig
from harness.utils.logger import getLogger


class DeltaTableTarget(AbstractTarget):
    """
    Represents a Delta table target. Takes data from a datafram and write it to its target table.
    """

    def __init__(
        self,
        harness_job_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        table_config: TableTargetConfig,
        session: SparkSession,
    ):
        """
            Builds Delta table target.
        Args:
            harness_job_config (HarnessJobConfig): the job configuration
            snapshot_config (SnapshotConfig): snapshot configuration
            table_config (TableTargetConfig): Target table configuration
            session (SparkSession): spark session to use to perfrom spark operations

        Raises:
            Exception: If no schema or table name is provided.
        """
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
        """Gets the name of the snapshot table."""

        return f"""{self.table_config.snapshot_target_schema}.{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}_V{version}"""

    def write(self, df: DataFrame):
        """ "
        writes the data from the dataframe into the target table
        """
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

    def setup_dev_target(self):
        """
        Sets up the target table for development.
        """
        catalog = self.session.catalog
        ts = self.table_config.dev_target_schema
        tt = self.table_config.dev_target_table
        ss = self.table_config.snapshot_target_schema
        st = f"{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}"
        self.setup_data(catalog, ts, tt, ss, st)

    def setup_test_target(self):
        """
        Sets up the target table for testing.
        """
        catalog = self.session.catalog
        ts = self.table_config.test_target_schema
        tt = self.table_config.test_target_table
        ss = self.table_config.snapshot_target_schema
        st = f"{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}"
        self.setup_data(catalog, ts, tt, ss, st)

    def setup_data(self, catalog, ts: str, tt: str, ss: str, st: str):
        """
            performs the setup opertations to configure a table with new data for test or development.
        Args:
            catalog (_type_): _description_
            ts : the schema to put the data into
            tt : the table to put the data into
            ss : the source schema from the testing data database where the source data is stored
            st : the source table from the testing data database where the source data is stored
        """
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

    def destroy(self):
        """destroys all of the source data. does not delete the target table(s)"""
        for i in range(1, self.snapshot_config.version + 1):
            self.session.sql(f"drop table if exists {self.getSnapshotTableName(i)};")
