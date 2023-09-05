from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import ByteType, DecimalType, IntegerType, LongType, ShortType

from harness.config.HarnessJobConfig import HarnessJobConfig
from harness.config.SnapshotConfig import SnapshotConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.AbstractSource import AbstractSource
from harness.sources.SourceConfig import JDBCSourceConfig


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

        df = self._convert_decimal_to_int_types(df)

        return df.repartition(50)

    def _convert_decimal_to_int_types(self, df):
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                if field.dataType.scale == 0:
                    if 0 < field.dataType.precision <= 2:
                        df = df.withColumn(field.name, col(field.name).cast(ByteType()))
                    elif 2 < field.dataType.precision <= 5:
                        df = df.withColumn(
                            field.name, col(field.name).cast(ShortType())
                        )
                    elif 5 < field.dataType.precision <= 9:
                        df = df.withColumn(
                            field.name, col(field.name).cast(IntegerType())
                        )
                    elif 10 <= field.dataType.precision <= 18:
                        df = df.withColumn(field.name, col(field.name).cast(LongType()))
        return df


class SimplifiedNetezzaJDBCSource:
    def __init__(
        self,
        source_schema: str,
        source_table: str,
        harness_config: HarnessJobConfig,
        session: SparkSession,
    ):
        self.harness_config: HarnessJobConfig = harness_config
        self.session: SparkSession = session
        self.source_schema: str = source_schema
        self.source_table: str = source_table
        
    def read(self) -> DataFrame:
        config = HarnessJobManagerEnvironment.getConfig()
        SQL = f"""(select * from {self.source_schema}..{self.source_table}) as data"""

        reader_options = {
            "driver": config.get("netezza_jdbc_driver"),
            "url": f"""{config.get("netezza_jdbc_url")}{self.source_schema};""",
            "dbtable": f"{SQL}",
            "fetchsize": 10000,
            "user": config.get("netezza_jdbc_user"),
            "password": config.get("netezza_jdbc_password"),
            "numPartitions": config.get("netezza_jdbc_num_part"),
        }

        df = self.session.read.format("jdbc").options(**reader_options).load()

        df = self._convert_decimal_to_int_types(df)

        return df.repartition(50)

    def _convert_decimal_to_int_types(self, df):
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                if field.dataType.scale == 0:
                    if 0 < field.dataType.precision <= 2:
                        df = df.withColumn(field.name, col(field.name).cast(ByteType()))
                    elif 2 < field.dataType.precision <= 5:
                        df = df.withColumn(
                            field.name, col(field.name).cast(ShortType())
                        )
                    elif 5 < field.dataType.precision <= 9:
                        df = df.withColumn(
                            field.name, col(field.name).cast(IntegerType())
                        )
                    elif 10 <= field.dataType.precision <= 18:
                        df = df.withColumn(field.name, col(field.name).cast(LongType()))
        return df
