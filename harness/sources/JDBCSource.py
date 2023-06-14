from pyspark.sql import DataFrame, SparkSession

from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class JDBCSource(AbstractSource):
    def __init__(self, config: JDBCSourceConfig, session: SparkSession):
        super().__init__(session)
        self.config = config

    def read(self) -> DataFrame:
        SQL = f"(select * from {self.config.source_table}"
        if self.config.source_filter is not None:
            SQL = SQL + f" where {self.config.source_filter})"

        reader_options = {
            "url": self.config.jdbc_url,
            "dbtable": f"{SQL}",
            "user": HarnessJobManagerEnvironment.jdbc_user,
            "password": HarnessJobManagerEnvironment.jdbc_password,
            "numPartitions": HarnessJobManagerEnvironment.jdbc_num_part,
        }

        nz_Source_DF = (
            self.session.read.format("jdbc")
            .option("driver", "org.netezza.Driver")
            .options(**reader_options)
            .load()
        )

        return nz_Source_DF
