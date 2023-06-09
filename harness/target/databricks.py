from harness.config.config import TargetConfig
from harness.target.target import AbstractTarget
from pyspark.sql import SparkSession, DataFrame


class TableTargetConfig(TargetConfig):
    target_schema: str
    target_table: str


class TableTarget(AbstractTarget):
    def __init__(self, config: TableTargetConfig, session: SparkSession):
        super().__init__(config, session)
        self._targetSchema = config.target_schema
        self._targetTable = config.target_table
        self._session = session

    def write(self, df: DataFrame):
        df.writeTo("""{self.target_schema}.{self.target_table}""")
