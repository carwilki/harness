from harness.config.SourceConfig import SourceConfig


class JDBCSourceConfig(SourceConfig):
    source_filter: str
    source_table: str
    source_schema: str