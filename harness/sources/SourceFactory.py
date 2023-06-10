from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.sources.AbstractSource import AbstractSource
from harness.sources.JDBCSource import JDBCSource


class SourceFactory:
    @classmethod
    def create(cls, source_config: SourceConfig) -> AbstractSource:
        match source_config.source_type:
            case SourceTypeEnum.netezza:
                return JDBCSource(config=source_config)
            case _:
                raise Exception(f"Unknown source type {source_config.source_type}")
