from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Catalog
from harness.config.EnvConfig import EnvConfig
from harness.config.EnvironmentEnum import EnvironmentEnum

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig
from harness.utils.logger import getLogger
from harness.utils.schema import stripPrefix


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
        self.env_config: EnvConfig = HarnessJobManagerEnvironment.get_config()
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

    def getTargetSchemaForEnv(self, env: EnvironmentEnum) -> str:
        """
        Gets the target schema for the given environment.
        """
        # TODO: this is PetsMart specific implementation. Should be refactored to be generalized.
        schema = stripPrefix(self.table_config.test_target_schema)
        if env == EnvironmentEnum.DEV:
            return f"dev_{schema}"
        elif env == EnvironmentEnum.QA:
            return f"qa_{schema}"
        elif env == EnvironmentEnum.PROD:
            return schema
        else:
            raise Exception("Invalid environment")

    def setupDataForEnv(self, env: EnvironmentEnum):
        """
        Sets up the target table for testing.
        """
        catalog = self.session.catalog
        ts = self.getTargetSchemaForEnv(env)
        tt = self.table_config.test_target_table
        ss = self.table_config.snapshot_target_schema
        st = f"{self.harness_job_config.job_name}_{self.table_config.snapshot_target_table}"
        self._setupData(catalog, ts, tt, ss, st)

    def _setupData(self, catalog: Catalog, ts: str, tt: str, ss: str, st: str):
        """
            performs the setup opertations to configure a table with new data for test or development.
        Args:
            catalog (Catalog): catalog to use to look up if the schema or table exists
            ts : the schema to put the data into
            tt : the table to put the data into
            ss : the source schema from the testing data database where the source data is stored
            st : the source table from the testing data database where the source data is stored
        """
        if catalog.tableExists(f"{ts}.{tt}"):
            query = f"truncate table {ts}.{tt}"
            self.logger.debug("executing truncate table")
            self.logger.debug(query)
            self.session.sql(query)

            if self.snapshot_config.isInput:
                query = f"insert into {ts}.{tt} select * from {ss}.{st}_V2"
            else:
                query = f"insert into {ts}.{tt} select * from {ss}.{st}_V1"

            self.logger.debug("executing insert into table")
            self.logger.debug(query)
            self.session.sql(query)
        else:
            if self.snapshot_config.isInput:
                query = f"create table {ts}.{tt} as select * from {ss}.{st}_V2"
            else:
                query = f"create table {ts}.{tt} as select * from {ss}.{st}_V1"
            self.logger.debug("executing create table")
            self.logger.debug(query)
            self.session.sql(query)

    def destroy(self):
        """destroys all of the source data. does not delete the target table(s)"""
        for i in range(1, self.snapshot_config.version + 1):
            self.session.sql(f"drop table if exists {self.getSnapshotTableName(i)};")
