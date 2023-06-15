from pyspark.sql import DataFrame, SparkSession

from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig


class TableTarget(AbstractTarget):
    def __init__(self, config: TableTargetConfig, session: SparkSession):
        super().__init__(session=session)
        self.config = config

    def write(self, df: DataFrame) -> bool:
        self.session.sql(f"""truncate table {self.config.target_schema}.{self.config.target_table}""")
        df.writeTo(f"""{self.target_schema}.{self.target_table}""")
