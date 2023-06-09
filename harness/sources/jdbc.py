from pyspark.sql import SparkSession
from harness.config.config import SourceConfig
from harness.sources.source import AbstractSource


class JDBCSource(AbstractSource):
    def __init__(self, config: SourceConfig, session: SparkSession):
        super().__init__(config, session)

    def read(self):
        pass
