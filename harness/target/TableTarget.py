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
        
        if prefix is not None:
            SQL = f"""create table {self.config.target_schema}.{self.config.target_table} 
            as select * from {temptable}"""
        else:
            SQL = f"""create table {self.config.target_schema}.{self.config.target_table} 
            as select * from {temptable}"""

        self.session.sql(SQL)
