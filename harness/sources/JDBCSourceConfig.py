from typing import Optional

from harness.config.SourceConfig import SourceConfig


class JDBCSourceConfig(SourceConfig):
    source_filter: Optional[str]
    source_table: str
    source_schema: str
