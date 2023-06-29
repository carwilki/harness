from pyspark.sql import DataFrame, SparkSession

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class DatabricksJDBCSource(AbstractSource):
    def __init__(
        self,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        config: JDBCSourceConfig,
        session: SparkSession,
    ):
        super().__init__(
            harness_config=harness_config,
            snapshot_config=snapshot_config,
            session=session,
        )
        self.config: JDBCSourceConfig = config

    def read(self) -> DataFrame:
        config = HarnessJobManagerEnvironment.getConfig()
        SQL = (
            f"""Select * from {self.config.source_schema}.{self.config.source_table}"""
        )

        if self.config.source_filter is not None:
            SQL = SQL + f""" WHERE {self.config.source_filter}"""

        SQL = f"""({SQL}) as data"""

        reader_options = {
            "host": config.get("databricks_jdbc_host"),
            "httpPath": config.get("databricks_jdbc_http_path"),
            "personalAccessToken": config.get("databricks_jdbc_pat"),
            "dbtable": f"{SQL}",
        }

        df = self.session.read.format("databricks").options(**reader_options).load()

        return df.repartition(50)


class NetezzaJDBCSource(AbstractSource):
    def __init__(
        self,
        harness_config: HarnessJobConfig,
        snapshot_config: SnapshotConfig,
        config: JDBCSourceConfig,
        session: SparkSession,
    ):
        super().__init__(
            harness_config=harness_config,
            snapshot_config=snapshot_config,
            session=session,
        )
        self.config: JDBCSourceConfig = config

    def read(self) -> DataFrame:
        config = HarnessJobManagerEnvironment.getConfig()
        SQL = (
            f"""Select * from {self.config.source_schema}.{self.config.source_table}"""
        )

        if self.config.source_filter is not None:
            SQL = SQL + f""" WHERE {self.config.source_filter}"""

        SQL = f"""({SQL}) as data"""

        reader_options = {
            "driver": config.get("netezza_jdbc_driver"),
            "url": f"""{config.get("netezza_jdbc_url")}{self.config.source_schema};""",
            "dbtable": f"{SQL}",
            "fetchsize": 10000,
            "user": config.get("netezza_jdbc_user"),
            "password": config.get("netezza_jdbc_password"),
            "numPartitions": config.get("netezza_jdbc_num_part"),
        }

        df = self.session.read.format("jdbc").options(**reader_options).load()

        return df.repartition(50)
