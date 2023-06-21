from typing import Optional
from uuid import uuid4
from pyspark.sql import DataFrame, SparkSession

from harness.target.AbstractTarget import AbstractTarget
from harness.target.TableTargetConfig import TableTargetConfig


class TableTarget(AbstractTarget):
    def __init__(self, config: TableTargetConfig, session: SparkSession):
        super().__init__(session=session)
        self.config = config

    def write(self, df: DataFrame, prefix: Optional[str] = None):
        if self.config.target_schema is None:
            raise Exception("Schema name is required")

        if self.config.target_table is None:
            raise Exception("Table name is required")

        temptable = f"{str(uuid4()).replace('-','')}_data"
        df.createOrReplaceTempView(temptable)

        if not self.session.catalog.tableExists(
            f"{self.config.target_schema}.{self.config.target_table}"
        ):
            if prefix is not None:  
                self.session.sql(
                    f"""create table {self.config.target_schema}.{self.config.target_table} 
                as select * from {temptable}"""
                )
            else:
                self.session.sql(
                    f"""create table {self.config.target_schema}.{self.config.target_table} 
                as select * from {temptable}"""
                )
        else:
            self.session.sql(
                f"truncate table {self.config.target_schema}.{self.config.target_table}"
            )
            self.session.sql(
                f"insert into {self.config.target_schema}.{self.config.target_table} select * from {temptable}"
            )
