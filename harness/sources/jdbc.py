from pyspark.sql import SparkSession, DataFrame
from harness.config.config import SourceConfig
from harness.manager.manager import HarnessJobManagerEnvironment
from harness.sources.source import AbstractSource


class JDBCSourceConfig(SourceConfig):
    filter: str
    table: str
    schema: str


class JDBCSource(AbstractSource):
    def __init__(self, config: JDBCSourceConfig, session: SparkSession):
        self.config = config
        self.session = session

    def read(self) -> DataFrame:
        SQL = f"(select * from {self.config.table} where {self.config.filter}) as src"

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
