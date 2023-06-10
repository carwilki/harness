from pyspark.sql import DataFrame, SparkSession

from harness.target import TableTargetConfig
from harness.target.AbstractTarget import AbstractTarget


class TableTarget(AbstractTarget):
    def __init__(self, config: TableTargetConfig, session: SparkSession):
        super().__init__(config, session)
        self._targetSchema = config.target_schema
        self._targetTable = config.target_table
        self._session = session

    def create_write(self, df: DataFrame):
        df.writeTo("""{self.target_schema}.{self.target_table}""").overwrite()

    def append_write(self, df: DataFrame):
        # This function will append to an already existing table
        df.writeTo("""{self.target_schema}.{self.target_table}""").append()