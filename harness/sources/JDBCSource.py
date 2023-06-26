from pyspark.sql import DataFrame, SparkSession

from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class DatabricksJDBCSource(AbstractSource):
    def __init__(self, config: JDBCSourceConfig, session: SparkSession):
        super().__init__(session)
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
    def __init__(self, config: JDBCSourceConfig, session: SparkSession):
        super().__init__(session)
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
            "url": config.get("netezza_jdbc_url"),
            "dbtable": f"{SQL}",
            "fetchsize": config.get("netezza_jdbc_url"),
            "user": config.get("netezza_jdbc_user"),
            "password": config.get("netezza_jdbc_password"),
            "numPartitions": config.get("netezza_jdbc_num_part"),
        }

        df = self.session.read.format("jdbc").options(**reader_options).load()

        return df.repartition(50)
