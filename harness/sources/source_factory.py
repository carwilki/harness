from harness.config.config import SourceConfig, SourceTypeEnum
from harness.sources.jdbc import JDBCSource
from harness.sources.source import AbstractSource


class SourceFactory:
    @classmethod
    def create(cls, source_config: SourceConfig) -> AbstractSource:
        match source_config.source_type:
            case SourceTypeEnum.netezza:
                return JDBCSource(config=source_config)
            case _:
                raise Exception(f"Unknown source type {source_config.source_type}")
