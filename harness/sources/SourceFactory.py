from pyspark.sql import SparkSession

from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSource import JDBCSource
from harness.sources.JDBCSourceConfig import JDBCSourceConfig


class SourceFactory:
    @classmethod
    def create(
        cls, source_config: SourceConfig, session: SparkSession
    ) -> AbstractSource:
        match source_config.source_type:
            case SourceTypeEnum.jdbc:
                assert isinstance(source_config, JDBCSourceConfig)
                return JDBCSource(config=source_config, session=session)
            case _:
                raise Exception(f"Unknown source type {source_config.source_type}")
