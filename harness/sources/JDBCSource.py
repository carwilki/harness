from pyspark.sql import DataFrame, SparkSession

from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class JDBCSource(AbstractSource):
    def __init__(self, config: JDBCSourceConfig, session: SparkSession):
        super().__init__(session)
        self.config: JDBCSourceConfig = config

    def read(self) -> DataFrame:
        SQL = f"""Select * from {self.config.source_schema}.{self.config.source_table}"""
        
        if self.config.source_filter is not None:
            SQL = SQL + f""" WHERE {self.config.source_filter}"""
        
        SQL = f"""({SQL}) as data"""

        reader_options = {
            "url": f"""{HarnessJobManagerEnvironment.jdbc_url()}/{self.config.source_schema}""",
            "dbtable": f"{SQL}",
            "fetchsize": 10000,
            "user": HarnessJobManagerEnvironment.jdbc_user(),
            "password": HarnessJobManagerEnvironment.jdbc_password(),
            "numPartitions": str(HarnessJobManagerEnvironment.jdbc_num_part()),
        }

        df = (
            self.session.read.format("jdbc")
            .option("driver", "org.netezza.Driver")
            .options(**reader_options)
            .load()
        )

        return df.repartition(100)
