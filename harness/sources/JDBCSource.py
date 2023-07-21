from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType, ByteType, ShortType, IntegerType, LongType
from pyspark.sql.functions import col
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

        for feild in df.schema.fields:
            if isinstance(feild.dataType, DecimalType):
                if feild.dataType.scale == 0:
                    if 0 < feild.dataType.precision < 5:
                        df = df.withColumn(feild.name, col(feild.name).cast(ShortType()))
                    elif 5 < feild.dataType.precision < 12:
                        df = df.withColumn(feild.name, col(feild.name).cast(IntegerType()))
                    elif 12 < feild.dataType.precision < 22:
                        df = df.withColumn(feild.name, col(feild.name).cast(LongType()))

        return df.repartition(50)
